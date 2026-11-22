//! Scan CSV structure across whole record-aligned spans. Each vector load
//! supplies the separator, quote and newline masks for all rows it touches.

pub(super) fn enabled<const N: usize>() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    (N == 4 || N == 7)
        && *ENABLED.get_or_init(|| {
            #[cfg(target_arch = "x86_64")]
            {
                std::is_x86_feature_detected!("avx2")
                    && std::env::var_os("PERF_SCANNER").is_some_and(|v| v == "span")
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                false
            }
        })
}

pub(super) fn window_enabled<const N: usize>() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        (N == 4 || N == 7) && *ON.get_or_init(|| {
            let arch = std::env::var("PERF_ARCH").unwrap_or_default();
            let requested = std::env::var("PERF_SCANNER")
                .map_or(matches!(arch.as_str(), "A" | "B"), |mode| mode == "window");
            requested && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512f")
        })
    }
    #[cfg(not(target_arch = "x86_64"))]
    false
}

// The span reader owns complete records. Keep the masks of its current
// 64-byte block while consuming rows: a separator, quote or newline byte is
// classified once even when the next row starts halfway through this block.
#[cfg(target_arch = "x86_64")]
pub(super) fn window<'a, const N: usize>(span: &'a [u8], emit: &mut impl FnMut([&'a [u8]; N])) {
    #[inline(always)]
    unsafe fn load(ptr: *const u8) -> [u64; 3] {
        let (comma, quote, newline);
        unsafe {
            std::arch::asm!(
                "vmovdqu64 zmm0, [{p}]",
                "vpbroadcastb zmm1, {c:e}", "vpcmpeqb k1, zmm0, zmm1",
                "vpbroadcastb zmm1, {q:e}", "vpcmpeqb k2, zmm0, zmm1",
                "vpbroadcastb zmm1, {n:e}", "vpcmpeqb k3, zmm0, zmm1",
                "kmovq {cm}, k1", "kmovq {qm}, k2", "kmovq {nm}, k3",
                p=in(reg) ptr, c=in(reg) 44u32, q=in(reg) 34u32, n=in(reg) 10u32,
                cm=lateout(reg) comma, qm=lateout(reg) quote, nm=lateout(reg) newline,
                out("xmm0") _, out("xmm1") _, out("k1") _, out("k2") _, out("k3") _,
                options(pure, readonly, nostack, preserves_flags)
            );
        }
        [comma, quote, newline]
    }
    let mut start = 0;
    let mut base = 0;
    let mut saved_commas = 0u128;
    let mut saved_quotes = 0u128;
    while base + 64 <= span.len() {
        // Called only after window_enabled checked AVX-512F and BW. The
        // complete 64-byte block is inside the span, including near EOF.
        let [commas, quotes, mut ends] = unsafe { load(span.as_ptr().add(base)) };
        let mut position = 0;
        while ends != 0 {
            let end = ends.trailing_zeros() as usize;
            let line = &span[start..base + end];
            let split = if line.len() <= 128 {
                let prefix = (1u64 << end) - 1;
                let carried = (base + position) - start;
                // At an aligned newline just after a 128-byte row the
                // current block contributes no bits.
                let shift = carried & 127;
                let c = saved_commas | (u128::from((commas & prefix) >> position) << shift);
                let q = saved_quotes | (u128::from((quotes & prefix) >> position) << shift);
                parsed::<N>(line, c, q)
            } else {
                super::fields::<N>(line)
            };
            if let Some(fields) = split {
                emit(fields);
            }
            ends &= ends - 1;
            start = base + end + 1;
            position = end + 1;
            saved_commas = 0;
            saved_quotes = 0;
        }
        let carried = base + position - start;
        if position < 64 && base + 64 - start <= 128 {
            saved_commas |= u128::from(commas >> position) << carried;
            saved_quotes |= u128::from(quotes >> position) << carried;
        }
        base += 64;
    }
    // An arbitrarily long record and a short final block share the same
    // exact parser. The pending prefix was never published as a row.
    super::each_line(&span[start..], &mut |line| {
        if let Some(fields) = super::fields::<N>(line) {
            emit(fields);
        }
    });
}

#[inline]
fn parsed<'a, const N: usize>(line: &'a [u8], commas: u128, quotes: u128) -> Option<[&'a [u8]; N]> {
    if line.len() > 128 {
        return super::fields::<N>(line);
    }
    if commas.count_ones() as usize + 1 != N {
        return None;
    }
    let openings = if quotes == 0 {
        0
    } else {
        let openings = quotes & ((commas << 1) | 1);
        let ends = (commas >> 1) | (1u128 << (line.len() - 1));
        // Subtraction borrows from the closest field end. Empty or unpaired
        // quotes, a quote inside a bare field, or a quoted comma fail equality.
        let closings = ends & !ends.wrapping_sub(openings);
        if quotes != openings ^ closings {
            return None;
        }
        openings
    };
    #[cfg(all(feature = "large-input", target_arch = "x86_64"))]
    if N == 7 {
        let low = commas as u64;
        let high = (commas >> 64) as u64;
        let nlow = low.count_ones();
        if (3..=5).contains(&nlow) {
            let cuts = super::event_cuts(low, high, nlow);
            if openings == 0 {
                return Some(super::event_slices::<N, false>(line, cuts, 0));
            }
            #[cfg(target_feature = "bmi2")]
            {
                use std::arch::x86_64::_pext_u64;
                let after = openings >> 1;
                let quoted = unsafe {
                    _pext_u64(after as u64, low) | (_pext_u64((after >> 64) as u64, high) << nlow)
                };
                let trims = ((quoted << 1) | (openings as u64 & 1)) as usize;
                return Some(super::event_slices::<N, true>(line, cuts, trims));
            }
        }
    }
    #[inline(always)]
    fn split<const N: usize, const QUOTED: bool>(
        line: &[u8],
        mut commas: u128,
        openings: u128,
    ) -> [&[u8]; N] {
        let mut start = 0;
        std::array::from_fn(|i| {
            let end = if i + 1 == N {
                line.len()
            } else {
                let end = commas.trailing_zeros() as usize;
                commas &= commas - 1;
                end
            };
            // The only possible start at 128 belongs to an empty last field.
            let trim =
                usize::from(QUOTED && openings.checked_shr(start as u32).unwrap_or(0) & 1 != 0);
            // Popcount and the field loop fix the number of distinct increasing
            // commas. A trim flag certifies distinct outer quotes in this field.
            let field = unsafe { line.get_unchecked(start + trim..end - trim) };
            start = end + 1;
            field
        })
    }
    Some(if openings == 0 {
        split::<N, false>(line, commas, 0)
    } else {
        split::<N, true>(line, commas, openings)
    })
}

#[derive(Default)]
struct Row {
    start: usize,
    commas: u128,
    quotes: u128,
}

impl Row {
    #[inline(always)]
    fn append(&mut self, base: usize, from: usize, to: usize, commas: u64, quotes: u64) {
        if from == to || base + to - self.start > 128 {
            return;
        }
        let width = to - from;
        let used = base + from - self.start;
        let below = u64::MAX >> (64 - width);
        self.commas |= u128::from((commas >> from) & below) << used;
        self.quotes |= u128::from((quotes >> from) & below) << used;
    }

    #[inline(always)]
    fn finish<'a, const N: usize>(
        &mut self,
        span: &'a [u8],
        end: usize,
        emit: &mut impl FnMut([&'a [u8]; N]),
    ) {
        if let Some(values) = parsed::<N>(&span[self.start..end], self.commas, self.quotes) {
            emit(values);
        }
        *self = Self {
            start: end + 1,
            ..Default::default()
        };
    }
}

pub(super) fn scan<'a, const N: usize>(span: &'a [u8], emit: &mut impl FnMut([&'a [u8]; N])) {
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx2") {
        return unsafe { scan_avx2::<N>(span, emit) };
    }
    super::each_line(span, &mut |line| {
        if let Some(values) = super::fields_portable::<N>(line) {
            emit(values);
        }
    });
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_avx2<'a, const N: usize>(span: &'a [u8], emit: &mut impl FnMut([&'a [u8]; N])) {
    use std::arch::x86_64::*;
    #[inline(always)]
    unsafe fn masks(ptr: *const u8) -> [u32; 3] {
        unsafe {
            let block = _mm256_loadu_si256(ptr.cast());
            let lanes =
                |c| _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, _mm256_set1_epi8(c))) as u32;
            [lanes(b'\n' as i8), lanes(b',' as i8), lanes(b'"' as i8)]
        }
    }
    let mut row = Row::default();
    let mut consume = |base: usize, len: usize, [mut lines, commas, quotes]: [u64; 3]| {
        let mut previous = 0;
        while lines != 0 {
            let end = lines.trailing_zeros() as usize;
            row.append(base, previous, end, commas, quotes);
            row.finish::<N>(span, base + end, emit);
            previous = end + 1;
            lines &= lines - 1;
        }
        row.append(base, previous, len, commas, quotes);
    };
    let mut groups = span.chunks_exact(64);
    for (i, bytes) in groups.by_ref().enumerate() {
        let lo = unsafe { masks(bytes.as_ptr()) };
        let hi = unsafe { masks(bytes.as_ptr().add(32)) };
        consume(
            64 * i,
            64,
            std::array::from_fn(|j| (u64::from(hi[j]) << 32) | u64::from(lo[j])),
        );
    }
    let tail = groups.remainder();
    if !tail.is_empty() {
        // Zero padding produces no structural characters.
        let mut bytes = [0; 64];
        bytes[..tail.len()].copy_from_slice(tail);
        let lo = unsafe { masks(bytes.as_ptr()) };
        let hi = unsafe { masks(bytes.as_ptr().add(32)) };
        consume(
            span.len() - tail.len(),
            tail.len(),
            std::array::from_fn(|j| (u64::from(hi[j]) << 32) | u64::from(lo[j])),
        );
    }
    if row.start < span.len() {
        row.finish::<N>(span, span.len(), emit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check<const N: usize>(bytes: &[u8]) {
        let mut expected = Vec::new();
        super::super::each_line(bytes, &mut |s| {
            if let Some(parts) = super::super::fields_portable::<N>(s) {
                expected.push(parts.map(<[u8]>::to_vec));
            }
        });
        let mut actual = Vec::new();
        scan::<N>(bytes, &mut |parts| actual.push(parts.map(<[u8]>::to_vec)));
        assert_eq!(actual, expected);
        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("avx512bw") && std::is_x86_feature_detected!("avx512f") {
            actual.clear();
            window::<N>(bytes, &mut |parts| actual.push(parts.map(<[u8]>::to_vec)));
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn boundaries_quotes_and_eof_agree_with_portable() {
        let mut bytes = Vec::new();
        for k in 0..=192 {
            for suffix in [
                &b",b,\"c\",,d,\"\",\r\n"[..],
                b",b,c,d,e,f,\n",
                b",b,c\",d,e,f,g\n",
                b",b,\"c\",d\n",
                b",\"b\",,\r\n",
            ] {
                bytes.extend(std::iter::repeat_n(b'p', k));
                bytes.extend_from_slice(suffix);
            }
        }
        bytes.extend_from_slice(b"\n\"\",,,");
        check::<4>(&bytes);
        check::<7>(&bytes);
    }

    #[test]
    fn random_fields_and_unaligned_subspans_match_reference() {
        let alphabet = b"ab,,,\r012345\xff\n\"";
        let mut state = 19u32;
        let data: Vec<_> = (0..32768)
            .map(|_| {
                state = state.wrapping_mul(1664525).wrapping_add(1013904223);
                alphabet[((state >> 16) as usize) % alphabet.len()]
            })
            .collect();
        for shift in 0..64 {
            check::<4>(&data[shift..7919 + shift]);
            check::<7>(&data[shift..7919 + shift]);
        }
    }

    #[test]
    fn partial_reader_blocks_and_mapping_keep_each_row_once() -> anyhow::Result<()> {
        use super::super::{Chunk, Source};
        use std::sync::Arc;
        let path =
            std::env::temp_dir().join(format!("shopping-structural-{}.csv", std::process::id()));
        let mut bytes = b"k,user,c,t\n".to_vec();
        let start = bytes.len();
        bytes.extend_from_slice(b"k1,u,c,t\n");
        bytes.extend(std::iter::repeat_n(b'X', 140_000));
        bytes.extend_from_slice(b",u,\"c\",t\n\nmalformed\nf,b,c,t\nf,\"b\",,t");
        std::fs::write(&path, &bytes)?;
        let mut reference = Vec::new();
        super::super::each_line(&bytes[start..], &mut |line| {
            if let Some(f) = super::super::fields_portable::<4>(line) {
                reference.push(f.map(<[u8]>::to_vec));
            }
        });
        for mapped in [false, true] {
            let source = Arc::new(Source::new(path.clone(), bytes.len() as u64, mapped));
            let mut rows = Vec::new();
            for offset in (0..bytes.len()).step_by(65536) {
                Chunk {
                    source: source.clone(),
                    start: offset as u64,
                    end: (offset + 65536).min(bytes.len()) as u64,
                }
                .spans(|span| scan::<4>(span, &mut |f| rows.push(f.map(<[u8]>::to_vec))))?;
            }
            assert_eq!(rows, reference);
        }
        std::fs::remove_file(path)?;
        Ok(())
    }
}
