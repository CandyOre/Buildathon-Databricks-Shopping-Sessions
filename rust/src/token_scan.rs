//! Classify 64-byte blocks once. Each retained separator occupies one byte:
//! its low seven bits give its position modulo 128 and its top bit identifies
//! a quoted field. The classifier passes compact endpoints to the decoder.
use std::arch::x86_64::{_pdep_u64, _pext_u64};

const BLOCKS: usize = 32;
const TILE: usize = 64 * BLOCKS;
const INDEX: [u8; 128] = {
    let mut a = [0; 128];
    let mut i = 0;
    while i < a.len() {
        a[i] = i as u8;
        i += 1;
    }
    a
};

pub(super) fn available() -> bool {
    // CPUID.7.0:ECX[6] is byte/word compression. BW/F detection also checks
    // OS support for the complete AVX-512 register state.
    std::is_x86_feature_detected!("avx512f")
        && std::is_x86_feature_detected!("avx512bw")
        && std::is_x86_feature_detected!("bmi2")
        && std::is_x86_feature_detected!("popcnt")
        && (unsafe { std::arch::x86_64::__cpuid_count(7, 0) }.ecx & (1 << 6)) != 0
}

pub(super) fn enabled<const N: usize>() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    (N == 4 || N == 7)
        && *ON.get_or_init(|| {
            std::env::var("PERF_SCANNER").map_or_else(
                |_| matches!(std::env::var("PERF_ARCH").as_deref(), Ok("A" | "B")),
                |v| v == "tokens",
            ) && available()
        })
}

#[derive(Clone, Copy, Default)]
struct Block {
    separators: u64,
    lines: u64,
    bad: u64,
    first: usize,
}

/// ptr and indices address complete 64-byte blocks, and out has room for
/// at least 64 more tokens. The caller has checked VBMI2, BW and F.
#[inline(always)]
unsafe fn classify(
    ptr: *const u8,
    indices: *const u8,
    out: *mut u8,
    prev_quote: u64,
) -> (u64, u64, u64) {
    let (separators, lines, quotes);
    unsafe {
        std::arch::asm!(
            "vmovdqu64 zmm0, [{ptr}]",
            "vpbroadcastb zmm1, {comma:e}", "vpcmpeqb k1, zmm0, zmm1",
            "vpbroadcastb zmm1, {newline:e}", "vpcmpeqb k2, zmm0, zmm1",
            "vpbroadcastb zmm1, {quote:e}", "vpcmpeqb k3, zmm0, zmm1",
            "korq k1, k1, k2",
            "kshiftlq k4, k3, 1", "kmovq k5, {prev}", "korq k4, k4, k5",
            "vmovdqu64 zmm2, [{index}]",
            "vpbroadcastb zmm1, {trim:e}", "vpaddb zmm2{{k4}}, zmm2, zmm1",
            "vpcompressb zmmword ptr [{out}]{{k1}}, zmm2",
            "kmovq {separators}, k1", "kmovq {lines}, k2", "kmovq {quotes}, k3",
            ptr=in(reg) ptr, index=in(reg) indices, out=in(reg) out,
            prev=in(reg) prev_quote, comma=in(reg) 44u32, newline=in(reg) 10u32,
            quote=in(reg) 34u32, trim=in(reg) 128u32,
            separators=lateout(reg) separators, lines=lateout(reg) lines, quotes=lateout(reg) quotes,
            out("xmm0") _, out("xmm1") _, out("xmm2") _,
            out("k1") _, out("k2") _, out("k3") _, out("k4") _, out("k5") _,
            options(nostack, preserves_flags)
        );
    }
    (separators, lines, quotes)
}

#[inline(always)]
fn separator(c: u8) -> bool {
    c == b',' || c == b'\n'
}

/// In the common case every field end is in the next 128 bytes, so endpoint
/// addresses are independent. Wider rows can still use the tokens when every
/// individual field is shorter than 128; the real newline checks unwrapping.
#[inline(always)]
fn slices<'a, const N: usize>(
    span: &'a [u8],
    start: usize,
    end: usize,
    tokens: &[u8],
) -> Option<[&'a [u8]; N]> {
    if end - start >= 128 {
        return wide_slices::<N>(span, start, end, tokens);
    }
    let mut field = start;
    Some(std::array::from_fn(|i| {
        let token = tokens[i];
        let cut = start + (usize::from(token).wrapping_sub(start) & 127);
        let trim = usize::from(token >> 7);
        // Classification establishes N increasing cuts in this row and that
        // a trim bit implies two distinct, validated outer quotes.
        let s = unsafe { span.get_unchecked(field + trim..cut - trim) };
        field = cut + 1;
        s
    }))
}

#[inline]
fn wide_slices<'a, const N: usize>(
    span: &'a [u8],
    start: usize,
    end: usize,
    tokens: &[u8],
) -> Option<[&'a [u8]; N]> {
    let mut field = start;
    let mut result = [&[][..]; N];
    for (part, &token) in result.iter_mut().zip(tokens) {
        let cut = field + (usize::from(token).wrapping_sub(field) & 127);
        let trim = usize::from(token >> 7);
        // If a field has wrapped by 128 or more, the compact endpoint can
        // precede its opening quote. Never slice until this check succeeds.
        if cut < field + 2 * trim {
            return super::fields_portable::<N>(&span[start..end]);
        }
        *part = &span[field + trim..cut - trim];
        field = cut + 1;
    }
    if field == end + 1 {
        Some(result)
    } else {
        super::fields_portable::<N>(&span[start..end])
    }
}

// This entire pass is under one CPU check per chunk.
#[target_feature(enable = "bmi2,popcnt")]
pub(super) unsafe fn scan<'a, const N: usize>(
    span: &'a [u8],
    emit: &mut impl FnMut([&'a [u8]; N]),
) {
    let nblocks = span.len() / 64;
    let mut buffer = [0u8; TILE];
    let mut blocks = [Block::default(); BLOCKS];
    let mut field_quoted = u64::from(span.first() == Some(&b'"'));
    let mut start = 0;
    let mut prior_bad = false;
    for tile in (0..nblocks).step_by(BLOCKS) {
        let count = (nblocks - tile).min(BLOCKS);
        let mut total = 0;
        for (i, b) in blocks[..count].iter_mut().enumerate() {
            let base = (tile + i) * 64;
            let prev = if base == 0 { b'\n' } else { span[base - 1] };
            let next = span.get(base + 64).copied().unwrap_or(b'\n');
            let prev_quote = u64::from(prev == b'"');
            let (s, lines, q) = unsafe {
                classify(
                    span.as_ptr().add(base),
                    INDEX.as_ptr().add(base & 127),
                    buffer.as_mut_ptr().add(total),
                    prev_quote,
                )
            };
            let n = s.count_ones() as usize;
            let mut bad = 0;
            if q | prev_quote | u64::from(next == b'"') | field_quoted != 0 {
                let at_start = (s << 1) | u64::from(separator(prev));
                let at_end = (s >> 1) | (u64::from(separator(next)) << 63);
                // An interior or singleton quote belongs to neither or both
                // sets. Each remaining opening must have a matching close
                // before the very next separator, possibly in another block.
                bad = q & !(at_start ^ at_end);
                if n != 0 {
                    // scan() checks BMI2 once for the whole span.
                    let after = unsafe { _pext_u64((q >> 1) | (u64::from(next == b'"') << 63), s) };
                    let before = unsafe { _pext_u64((q << 1) | prev_quote, s) };
                    bad |= unsafe { _pdep_u64(((after << 1) | field_quoted) ^ before, s) };
                    field_quoted = (after >> (n - 1)) & 1;
                }
            }
            *b = Block {
                separators: s,
                lines,
                bad,
                first: total,
            };
            total += n;
        }
        let mut token_start = 0;
        for (i, b) in blocks[..count].iter().enumerate() {
            let base = (tile + i) * 64;
            let mut lines = b.lines;
            let mut bad = b.bad;
            while lines != 0 {
                let bit = lines.trailing_zeros() as usize;
                let prefix = u64::MAX >> (63 - bit);
                let end = base + bit;
                let token_end = b.first + (b.separators & prefix).count_ones() as usize;
                if !prior_bad && bad & prefix == 0 {
                    let parts = if start < tile * 64 {
                        // Only the first crossing row of a 2-KiB tile uses
                        // this branch. Its previous tile's tokens were reused.
                        super::fields_portable::<N>(&span[start..end])
                    } else if token_end - token_start == N {
                        slices::<N>(span, start, end, &buffer[token_start..token_end])
                    } else {
                        None
                    };
                    if let Some(parts) = parts {
                        emit(parts);
                    }
                }
                prior_bad = false;
                bad &= !prefix;
                start = end + 1;
                token_start = token_end;
                lines &= lines - 1;
            }
            prior_bad |= bad != 0;
        }
    }
    super::each_line(&span[start..], &mut |line| {
        if let Some(parts) = super::fields_portable::<N>(line) {
            emit(parts);
        }
    });
}
