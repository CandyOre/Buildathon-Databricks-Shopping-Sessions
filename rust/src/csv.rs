use std::cell::RefCell;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use memchr::{memchr, memchr_iter, memrchr};
use crate::input::{Calendar, DAY, Dimensions, LOOKBACK, Window, unsigned};
use crate::records::Purchase;

const CHUNK_BYTES: u64 = 4 * 1024 * 1024;

pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(target_arch = "x86_64")]
    if (32..=128).contains(&line.len())
        && std::is_x86_feature_detected!("avx2")
        && std::is_x86_feature_detected!("bmi1")
        && std::is_x86_feature_detected!("popcnt")
    {
        // SAFETY: the CPU supports AVX2 and each input contains 1 to 4 vectors.
        return unsafe { fields_avx2(line) };
    }
    fields_portable(line)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,popcnt")]
unsafe fn fields_avx2<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use std::arch::x86_64::*;

    #[inline(always)]
    unsafe fn masks(ptr: *const u8) -> (u64, u64) {
        let b = unsafe { _mm256_loadu_si256(ptr.cast()) };
        unsafe {
            (
                (_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, _mm256_set1_epi8(b',' as i8))) as u32)
                    as u64,
                (_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, _mm256_set1_epi8(b'"' as i8))) as u32)
                    as u64,
            )
        }
    }
    let len = line.len();
    let masks32 = || {
        let (a, qa) = unsafe { masks(line.as_ptr()) };
        let (b, qb) = if len > 32 && len <= 64 {
            let (m, q) = unsafe { masks(line.as_ptr().add(len - 32)) };
            (m >> (64 - len), q >> (64 - len))
        } else if len > 64 {
            unsafe { masks(line.as_ptr().add(32)) }
        } else {
            (0, 0)
        };
        let (c, qc) = if len > 64 && len <= 96 {
            let (m, q) = unsafe { masks(line.as_ptr().add(len - 32)) };
            (m >> (96 - len), q >> (96 - len))
        } else if len > 96 {
            unsafe { masks(line.as_ptr().add(64)) }
        } else {
            (0, 0)
        };
        let (d, qd) = if len > 96 {
            let (m, q) = unsafe { masks(line.as_ptr().add(len - 32)) };
            (m >> (128 - len), q >> (128 - len))
        } else {
            (0, 0)
        };
        (a | b << 32, c | d << 32, qa | qb << 32, qc | qd << 32)
    };
    #[cfg(all(feature = "large-input", target_feature = "avx512bw"))]
    let (mut low, mut high, qlow, qhigh) = if N == 7 && len > 64 {
        unsafe { masks64(line) }
    } else {
        masks32()
    };
    #[cfg(not(all(feature = "large-input", target_feature = "avx512bw")))]
    let (mut low, mut high, qlow, qhigh) = masks32();
    let nlow = low.count_ones();
    if (nlow + high.count_ones()) as usize + 1 != N {
        return None;
    }
    #[cfg(feature = "large-input")]
    if N == 7 && (3..=5).contains(&nlow) {
        let cuts = event_cuts(low, high, nlow);
        if qlow | qhigh == 0 {
            return Some(event_slices::<N, false>(line, cuts, 0));
        }
        #[cfg(target_feature = "bmi2")]
        {
            let commas = u128::from(low) | (u128::from(high) << 64);
            let quotes = u128::from(qlow) | (u128::from(qhigh) << 64);
            let openings = quotes & ((commas << 1) | 1);
            let ends = (commas >> 1) | (1u128 << (len - 1));
            // Each opening borrows up to its own field's end and stops there.
            // A one-byte quote would have both ends at the same position; XOR
            // cancels them and rejects the row. Extra interior quotes also fail.
            let closings = ends & !ends.wrapping_sub(openings);
            if quotes != (openings ^ closings) {
                return None;
            }
            let following = unsafe {
                _pext_u64((quotes >> 1) as u64, low) | (_pext_u64(qhigh >> 1, high) << nlow)
            };
            let trims = ((following << 1) | (qlow & 1)) as usize;
            return Some(event_slices::<N, true>(line, cuts, trims));
        }
    }
    let quote_count = qlow.count_ones() + qhigh.count_ones();
    let mut result = [&[][..]; N];
    let mut matched_quotes = 0;
    let mut start = 0;
    for field in &mut result {
        let end = if low != 0 {
            let end = low.trailing_zeros() as usize;
            low &= low - 1;
            end
        } else if high != 0 {
            let end = 64 + high.trailing_zeros() as usize;
            high &= high - 1;
            end
        } else {
            line.len()
        };
        let left = line[start.min(line.len() - 1)];
        let right = line[end.saturating_sub(1)];
        let trim = usize::from((end - start >= 2) & (left == b'"') & (right == b'"'));
        matched_quotes += 2 * trim as u32;
        *field = &line[start + trim..end - trim];
        start = end + 1;
    }
    // Every quote must be one of the two outer bytes of its own field.
    (matched_quotes == quote_count).then_some(result)
}

// Event rows of 65--128 bytes fit in two legal overlapping 64-byte loads.
// Discard the overlap in the trailing masks so that bit zero of the second
// pair describes byte 64 in the input.
#[cfg(feature = "large-input")]
#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
#[inline(always)]
unsafe fn masks64(line: &[u8]) -> (u64, u64, u64, u64) {
    let (low, tail, qlow, qtail): (u64, u64, u64, u64);
    unsafe {
        std::arch::asm!(
            "vpbroadcastb zmm0, {comma:e}",
            "vpbroadcastb zmm1, {quote:e}",
            "vmovdqu64 zmm2, [{head}]",
            "vmovdqu64 zmm3, [{last}]",
            "vpcmpeqb k1, zmm2, zmm0",
            "vpcmpeqb k2, zmm3, zmm0",
            "vpcmpeqb k3, zmm2, zmm1",
            "vpcmpeqb k4, zmm3, zmm1",
            "kmovq {low}, k1",
            "kmovq {tail}, k2",
            "kmovq {qlow}, k3",
            "kmovq {qtail}, k4",
            comma = in(reg) u32::from(b','),
            quote = in(reg) u32::from(b'"'),
            head = in(reg) line.as_ptr(),
            last = in(reg) line.as_ptr().add(line.len() - 64),
            low = lateout(reg) low, tail = lateout(reg) tail,
            qlow = lateout(reg) qlow, qtail = lateout(reg) qtail,
            out("xmm0") _, out("xmm1") _, out("xmm2") _, out("xmm3") _,
            out("k1") _, out("k2") _, out("k3") _, out("k4") _,
            options(pure, readonly, nostack, preserves_flags),
        );
    }
    let skip = 128 - line.len();
    (low, tail >> skip, qlow, qtail >> skip)
}

// The first three delimiters and the last delimiter can be extracted on
// independent chains. Only the middle two depend on which half owns them.
#[cfg(feature = "large-input")]
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn event_cuts(low: u64, high: u64, nlow: u32) -> [usize; 6] {
    let low1 = low & (low - 1);
    let low2 = low1 & (low1 - 1);
    debug_assert!((3..=5).contains(&nlow));
    let low3 = low2 & (low2 - 1);
    let high_last = 63 - high.leading_zeros();
    let high_rest = high ^ (1u64 << high_last);
    let d = if low3 != 0 {
        low3.trailing_zeros()
    } else {
        64 + high.trailing_zeros()
    };
    let e = if high_rest != 0 {
        127 - high_rest.leading_zeros()
    } else {
        63 - low.leading_zeros()
    };
    [
        low.trailing_zeros(),
        low1.trailing_zeros(),
        low2.trailing_zeros(),
        d,
        e,
        64 + high_last,
    ]
    .map(|p| p as usize)
}

#[cfg(feature = "large-input")]
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn event_slices<const N: usize, const QUOTED: bool>(
    line: &[u8],
    cuts: [usize; 6],
    trims: usize,
) -> [&[u8]; N] {
    let [a, b, c, d, e, f] = cuts;
    let field = |index: usize, start: usize, end: usize| {
        let trim = if QUOTED { (trims >> index) & 1 } else { 0 };
        // The six delimiters are in increasing order within the row. A set
        // trim bit certifies two distinct outer quotes for its field.
        unsafe { line.get_unchecked(start + trim..end - trim) }
    };
    let mut result = [&[][..]; N];
    result[0] = field(0, 0, a);
    result[1] = field(1, a + 1, b);
    result[2] = field(2, b + 1, c);
    result[3] = field(3, c + 1, d);
    result[4] = field(4, d + 1, e);
    result[5] = field(5, e + 1, f);
    result[6] = field(6, f + 1, line.len());
    result
}

fn fields_portable<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut commas = memchr_iter(b',', line);
    let mut quotes = memchr_iter(b'"', line);
    let mut next_quote = quotes.next();
    let mut start = 0;
    let mut fields = [&[][..]; N];
    for (index, field) in fields.iter_mut().enumerate() {
        let end = match commas.next() {
            Some(at) if index + 1 < N => at,
            None if index + 1 == N => line.len(),
            _ => return None,
        };
        *field = if next_quote.is_some_and(|position| position < end) {
            if next_quote != Some(start) || quotes.next() != Some(end - 1) {
                return None;
            }
            next_quote = quotes.next();
            &line[start + 1..end - 1]
        } else {
            &line[start..end]
        };
        start = end + 1;
    }
    Some(fields)
}

#[cfg(test)]
mod field_tests {
    use super::*;

    #[test]
    fn vector_boundaries_agree_with_portable_csv() {
        // Move every delimiter and quote across the 32/64/96-byte boundaries,
        // including malformed rows, empty fields, CRs and a final empty field.
        for padding in 0..140 {
            for suffix in [
                &b",b,\"c\",,d,\"\",\r"[..],
                &b",\"b\",c,d,e,f,"[..],
                &b",b,c,d,e,\"f\"\"\",g"[..],
                &b",b,c,d,e,f,g\""[..],
                &b",b,c,d,e,f"[..],
                &b",b,c,d,e,f,g,h"[..],
            ] {
                let mut line = vec![b'a'; padding];
                line.extend_from_slice(suffix);
                assert_eq!(fields::<7>(&line), fields_portable::<7>(&line));
            }
        }
    }

    #[cfg(feature = "large-input")]
    #[test]
    fn seven_slices_match_every_quoting_pattern_and_mutated_boundaries() {
        for timestamp_width in [13, 20, 25] {
            let timestamp = vec![b'9'; timestamp_width];
            for empty_product in [false, true] {
                let product: &[u8] = if empty_product { b"" } else { b"p12345" };
                let values: [&[u8]; 7] = [
                    b"e12345678",
                    b"u12345678",
                    &timestamp,
                    b"2028-02-29T00:00:00Z",
                    b"view",
                    product,
                    b"",
                ];
                for quoted in 0..128 {
                    let mut line = Vec::new();
                    for (index, value) in values.iter().enumerate() {
                        if index != 0 {
                            line.push(b',');
                        }
                        let wrapped = quoted & (1 << index) != 0;
                        if wrapped {
                            line.push(b'"');
                        }
                        line.extend_from_slice(value);
                        if wrapped {
                            line.push(b'"');
                        }
                    }
                    assert_eq!(fields::<7>(&line), Some(values));
                    for i in 0..line.len() {
                        let keep = line[i];
                        for byte in [b'"', b',', b'\r'] {
                            line[i] = byte;
                            assert_eq!(fields::<7>(&line), fields_portable::<7>(&line));
                        }
                        line[i] = keep;
                    }
                }
            }
        }
    }
}

pub fn parts(path: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "csv") && path.is_file() {
            paths.push(path);
        }
    }
    Ok(paths)
}

pub struct Chunk {
    source: Arc<Source>,
    start: u64,
    end: u64,
}

struct Source {
    path: PathBuf,
    #[cfg(target_os = "linux")]
    mapping: Option<Mapping>,
}

impl Source {
    fn new(path: PathBuf, len: u64, mapped: bool) -> Self {
        #[cfg(target_os = "linux")]
        let mapping = if mapped && len != 0 {
            Mapping::open(&path, len).ok()
        } else {
            None
        };
        #[cfg(not(target_os = "linux"))]
        let _ = (len, mapped);
        Self { path, #[cfg(target_os = "linux")] mapping }
    }
}

#[cfg(target_os = "linux")]
struct Mapping {
    data: std::ptr::NonNull<u8>,
    len: usize,
    release_page_size: usize,
}

// The scorer's input volume is read-only throughout a run. Every access to
// the mapping is read-only; the last Arc<Source> owns the unmap.
#[cfg(target_os = "linux")]
unsafe impl Send for Mapping {}
#[cfg(target_os = "linux")]
unsafe impl Sync for Mapping {}

#[cfg(target_os = "linux")]
impl Mapping {
    fn open(path: &Path, len: u64) -> Result<Self> {
        use std::os::fd::AsRawFd;
        let len = usize::try_from(len)?;
        anyhow::ensure!(len <= isize::MAX as usize, "mapping exceeds addressable slice");
        let file = File::open(path)?;
        let ptr = unsafe {
            libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, file.as_raw_fd(), 0)
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error().into());
        }
        let page = if perf_on("PERF_PAGE_RELEASE") {
            usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(0)
        } else { 0 };
        if let Some(data) = std::ptr::NonNull::new(ptr.cast()) {
            Ok(Self { data, len, release_page_size: page })
        } else {
            unsafe { libc::munmap(ptr, len); }
            anyhow::bail!("null mapping address")
        }
    }

    fn span(&self, start: u64, end: u64, visit: &mut impl FnMut(&[u8])) {
        let bytes = unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len) };
        let start = (start as usize).min(bytes.len());
        let end = usize::try_from(end).unwrap_or(usize::MAX).min(bytes.len());
        let before = start.saturating_sub(1);
        if before < end {
            if let Some(newline) = memchr(b'\n', &bytes[before..end]) {
                let first = before + newline + 1;
                if first < end {
                    let last = if end == bytes.len() || bytes[end - 1] == b'\n' {
                        end
                    } else {
                        memchr(b'\n', &bytes[end..]).map_or(bytes.len(), |offset| end + offset + 1)
                    };
                    visit(&bytes[first..last]);
                }
            }
        }
        // Advise only complete pages inside this task's nominal byte range.
        // Linux DONTNEED on a clean file mapping releases PTEs, not file data.
        let page = self.release_page_size;
        if page != 0 {
            let first = start.div_ceil(page) * page;
            let last = end / page * page;
            if first < last {
                unsafe { libc::madvise(self.data.as_ptr().add(first).cast(), last - first, libc::MADV_DONTNEED); }
            }
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for Mapping {
    fn drop(&mut self) { unsafe { libc::munmap(self.data.as_ptr().cast(), self.len); } }
}

pub(crate) fn perf_on(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|v| v == "1")
}

fn use_mmap() -> bool {
    std::env::var_os("PERF_IO").is_some_and(|v| v == "mmap")
}

fn each_line<'a>(bytes: &'a [u8], row: &mut impl FnMut(&'a [u8])) {
    let mut start = 0;
    for end in memchr_iter(b'\n', bytes) {
        row(&bytes[start..end]);
        start = end + 1;
    }
    if start < bytes.len() { row(&bytes[start..]); }
}

struct ScanBuffer {
    bytes: Vec<u8>,
    partial_line: Vec<u8>,
}

thread_local! {
    static SCAN_BUFFER: RefCell<ScanBuffer> = RefCell::new(ScanBuffer {
        bytes: vec![0; 128 * 1024],
        partial_line: Vec::new(),
    });
}

impl Chunk {
    pub fn byte_len(&self) -> usize {
        (self.end - self.start).min(CHUNK_BYTES) as usize
    }

    pub fn scan(&self, mut row: impl FnMut(&[u8])) -> Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(map) = &self.source.mapping {
            map.span(self.start, self.end, &mut |span| each_line(span, &mut row));
            return Ok(());
        }
        let mut file = File::open(&self.source.path)?;
        let mut offset = self.start.saturating_sub(1);
        file.seek(SeekFrom::Start(offset))?;
        SCAN_BUFFER.with_borrow_mut(|buffer| {
            let ScanBuffer {
                bytes,
                partial_line,
            } = buffer;
            partial_line.clear();
            // Skip the header, or the preceding chunk's last record. Looking back
            // one byte makes a record that starts exactly on the boundary ours.
            let mut skip_first = true;
            loop {
                let len = match file.read(bytes) {
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    result => result?,
                };
                if len == 0 {
                    if !partial_line.is_empty() {
                        row(partial_line);
                    }
                    return Ok(());
                }
                let block = &bytes[..len];
                let mut start = 0;
                for end in memchr_iter(b'\n', block) {
                    if skip_first {
                        skip_first = false;
                    } else if partial_line.is_empty() {
                        row(&block[start..end]);
                    } else {
                        partial_line.extend_from_slice(&block[start..end]);
                        row(partial_line);
                        partial_line.clear();
                    }
                    start = end + 1;
                    if offset + start as u64 >= self.end {
                        return Ok(());
                    }
                }
                offset += len as u64;
                if !skip_first {
                    partial_line.extend_from_slice(&block[start..]);
                } else if offset >= self.end {
                    return Ok(());
                }
            }
        })
    }
}

impl Chunk {
    // Each span starts on a record boundary and ends after a complete record.
    // The last record at EOF may have no LF. This lets a numeric decoder hold
    // borrowed fields from several adjacent rows while their buffer is live.
    fn spans(&self, mut visit: impl FnMut(&[u8])) -> Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(map) = &self.source.mapping {
            map.span(self.start, self.end, &mut visit);
            return Ok(());
        }
        let mut file = File::open(&self.source.path)?;
        let mut offset = self.start.saturating_sub(1);
        file.seek(SeekFrom::Start(offset))?;
        SCAN_BUFFER.with_borrow_mut(|buf| {
            let ScanBuffer { bytes, partial_line } = buf;
            partial_line.clear();
            let mut skip = true;
            loop {
                let n = match file.read(bytes) {
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    result => result?,
                };
                if n == 0 {
                    if !partial_line.is_empty() { visit(partial_line); }
                    return Ok(());
                }
                let block = &bytes[..n];
                let mut pos = 0;
                if skip {
                    let limit = self.end.saturating_sub(offset).min(n as u64) as usize;
                    if let Some(i) = memchr(b'\n', &block[..limit]) {
                        pos = i + 1;
                        skip = false;
                    } else {
                        offset += n as u64;
                        if offset >= self.end { return Ok(()); }
                        continue;
                    }
                }
                if !partial_line.is_empty() {
                    if let Some(i) = memchr(b'\n', &block[pos..]) {
                        partial_line.extend_from_slice(&block[pos..=pos+i]);
                        visit(partial_line);
                        partial_line.clear();
                        pos += i + 1;
                    } else {
                        partial_line.extend_from_slice(&block[pos..]);
                        offset += n as u64;
                        continue;
                    }
                }
                if offset + pos as u64 >= self.end { return Ok(()); }
                let remaining = self.end - offset;
                if remaining <= n as u64 {
                    let boundary = remaining as usize - 1;
                    if let Some(i) = memchr(b'\n', &block[boundary..]) {
                        visit(&block[pos..=boundary+i]);
                        return Ok(());
                    }
                }
                if let Some(i) = memrchr(b'\n', &block[pos..]) {
                    visit(&block[pos..=pos+i]);
                    pos += i + 1;
                }
                partial_line.extend_from_slice(&block[pos..]);
                offset += n as u64;
            }
        })
    }

    pub fn scan_parsed<const N: usize>(&self, mut row: impl FnMut([&[u8]; N])) -> Result<()> {
        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("avx2")
            && std::is_x86_feature_detected!("bmi1")
            && std::is_x86_feature_detected!("popcnt")
        {
            // SAFETY: CPU features are checked once for this entire chunk.
            return unsafe { self.scan_parsed_avx2(row) };
        }
        self.scan(|line| {
            if let Some(values) = fields_portable(line) {
                row(values);
            }
        })
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2,bmi1,popcnt")]
    unsafe fn scan_parsed_avx2<const N: usize>(
        &self,
        mut row: impl FnMut([&[u8]; N]),
    ) -> Result<()> {
        self.scan(|line| {
            let values = if (32..=128).contains(&line.len()) {
                // SAFETY: target features are active and the vector loads fit.
                unsafe { fields_avx2(line) }
            } else {
                fields_portable(line)
            };
            if let Some(values) = values {
                row(values);
            }
        })
    }
}

pub fn chunks(directory: &Path) -> Result<Vec<Chunk>> {
    let mut paths = parts(directory)?
        .into_iter()
        .map(|path| {
            let length = path.metadata()?.len();
            Ok((length, path))
        })
        .collect::<Result<Vec<_>>>()?;
    paths.sort_unstable_by_key(|(len, _)| std::cmp::Reverse(*len));
    let total = paths.iter().fold(0u64, |s, (n, _)| s.saturating_add(*n));
    let chunk_bytes = match std::env::var("PERF_CHUNK").as_deref() {
        Ok("adaptive") => {
            let target = total / (rayon::current_num_threads().max(1) as u64 * 256);
            target.clamp(64 << 10, 2 << 20).next_power_of_two()
        }
        Ok(n) => n.parse::<u64>().unwrap_or(CHUNK_BYTES).clamp(4096, CHUNK_BYTES),
        Err(_) => CHUNK_BYTES,
    };
    let mapped = use_mmap();
    let jobs = paths.into_iter().flat_map(|(len, path)| {
        let source = Arc::new(Source::new(path, len, mapped));
        (0..len)
            .step_by(chunk_bytes as usize)
            .map(move |start| Chunk {
                source: source.clone(),
                start,
                end: start.saturating_add(chunk_bytes).min(len),
            })
    });
    Ok(jobs.collect())
}

#[cfg(test)]
mod scan_tests {
    use super::*;

    #[test]
    fn long_rows_are_owned_by_the_chunk_containing_their_first_byte() -> Result<()> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shopping-slab-long-row-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&directory)?;
        let part = directory.join("part.csv");
        fs::write(&part, b"")?;
        assert!(chunks(&directory)?.is_empty());
        let block = CHUNK_BYTES as usize;
        let mut data = b"header\n".to_vec();
        data.resize(block - 1, b'a');
        data.push(b'\n');
        data.resize(3 * block + 17, b'b');
        data.extend_from_slice(b"\nz");
        fs::write(&part, data)?;
        let mut actual = Vec::new();
        for chunk in chunks(&directory)? {
            let mut lengths = Vec::new();
            chunk.scan(|line| lengths.push(line.len()))?;
            actual.push(lengths);
        }
        fs::remove_file(part)?;
        fs::remove_dir(directory)?;
        assert_eq!(
            actual,
            vec![vec![block - 8], vec![2 * block + 17], vec![], vec![1]]
        );
        Ok(())
    }

    #[test]
    fn chunks_preserve_lines_across_read_buffers_and_job_boundaries() -> Result<()> {
        let path =
            std::env::temp_dir().join(format!("shopping-csv-boundaries-{}", std::process::id()));
        let block = 128 * 1024;
        let mut data = b"header\n".to_vec();
        let expected = [
            vec![b'a'; block - data.len() - 1],
            vec![b'b'; 3 * block + 17],
            Vec::new(),
            b"last\r".to_vec(),
        ];
        for (position, record) in expected.iter().enumerate() {
            data.extend_from_slice(record);
            if position + 1 < expected.len() {
                data.push(b'\n');
            }
        }
        fs::write(&path, &data)?;
        for mapped in [false, true] {
            let source = Arc::new(Source::new(path.clone(), data.len() as u64, mapped));
            #[cfg(target_os = "linux")]
            if mapped { assert!(source.mapping.is_some()); }
            let mut actual = Vec::new();
            for start in (0..data.len()).step_by(block) {
                let chunk = Chunk {
                    source: source.clone(),
                    start: start as u64,
                    end: (start + block).min(data.len()) as u64,
                };
                chunk.spans(|span| each_line(span, &mut |row| actual.push(row.to_vec())))?;
            }
            assert_eq!(actual.as_slice(), expected);
        }
        let mut actual = Vec::new();
        for start in (0..data.len()).step_by(block) {
            Chunk {
                source: Arc::new(Source::new(path.clone(), data.len() as u64, false)),
                start: start as u64,
                end: (start + block).min(data.len()) as u64,
            }
            .scan(|line| actual.push(line.to_vec()))?;
        }
        fs::remove_file(&path)?;
        assert_eq!(actual.as_slice(), expected);
        Ok(())
    }
}

pub fn scan_file(path: &Path, row: impl FnMut(&[u8])) -> Result<()> {
    Chunk {
        source: Arc::new(Source::new(path.to_owned(), path.metadata()?.len(), use_mmap())),
        start: 0,
        end: u64::MAX,
    }
    .scan(row)
    .with_context(|| format!("reading {}", path.display()))
}

const MAX_BATCH: usize = 64;

pub fn batch_size() -> usize {
    static SIZE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *SIZE.get_or_init(|| {
        if !std::env::var_os("PERF_DECODER").is_some_and(|v| v == "batch") { return 0; }
        std::env::var("PERF_BATCH").ok().and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(16).clamp(1, MAX_BATCH)
    })
}

fn parsed_groups<'a, const N: usize>(
    span: &'a [u8], size: usize, visit: &mut impl FnMut(&[[&'a [u8]; N]]),
) {
    let mut rows = [[&[][..]; N]; MAX_BATCH];
    let mut len = 0;
    each_line(span, &mut |line| {
        if let Some(parts) = fields::<N>(line) {
            // A structurally valid event or click with empty ID is discarded
            // before any validity checks or deduplication.
            if !parts[0].is_empty() {
                rows[len] = parts;
                len += 1;
                if len == size {
                    visit(&rows[..len]);
                    len = 0;
                }
            }
        }
    });
    if len != 0 { visit(&rows[..len]); }
}

#[inline]
fn event_payload(
    fields: &[&[u8]; 7], time: Option<i64>, ingested: i64, window: Window, dim: &Dimensions,
) -> Option<(i64, Option<Purchase>)> {
    let time = time?;
    if fields[1].is_empty() || fields[5].is_empty() || time < window.start
        || time >= window.end || time < ingested - DAY { return None; }
    let purchase = match fields[4] {
        b"view" | b"cart" => None,
        b"purchase" => {
            let cents = unsigned(fields[6])?;
            if !(1..=10_000_000).contains(&cents) { return None; }
            let category = dim.products.lookup(fields[5]).unwrap_or(dim.unknown_category);
            Some(Purchase { category, cents: cents as u32 })
        }
        _ => return None,
    };
    Some((time, purchase))
}

impl Chunk {
    pub fn scan_events(&self, calendar: &Calendar, window: Window, dim: &Dimensions,
        mut emit: impl FnMut(&[u8], &[u8], i64, Option<(i64, Option<Purchase>)>),
    ) -> Result<()> {
        let size = batch_size();
        if size == 0 {
            return self.scan_parsed(|f: [&[u8]; 7]| {
                if f[0].is_empty() { return; }
                let ingest = calendar.ingest(f[3]);
                let payload = event_payload(&f, calendar.parse(f[2]), ingest, window, dim);
                emit(f[0], f[1], ingest, payload)
            });
        }
        self.spans(|span| parsed_groups::<7>(span, size, &mut |rows| {
            let mut times = [None; MAX_BATCH];
            calendar.times(rows, 2, &mut times[..rows.len()]);
            for (f, time) in rows.iter().zip(times) {
                let ingest = calendar.ingest(f[3]);
                let payload = event_payload(f, time, ingest, window, dim);
                emit(f[0], f[1], ingest, payload);
            }
        }))
    }

    pub fn scan_clicks(&self, calendar: &Calendar, window: Window, dim: &Dimensions,
        mut emit: impl FnMut(&[u8], &[u8], i64, u32),
    ) -> Result<()> {
        let mut valid = |f: &[&[u8];4], time: Option<i64>| {
            if let Some(time) = time {
                if !f[0].is_empty() && !f[1].is_empty() && time >= window.start - LOOKBACK && time < window.end {
                    if let Some(campaign) = dim.campaign_ids.lookup(f[2]) { emit(f[0], f[1], time, campaign); }
                }
            }
        };
        let size = batch_size();
        if size == 0 { return self.scan_parsed(|f| valid(&f, calendar.parse(f[3]))); }
        self.spans(|span| parsed_groups::<4>(span, size, &mut |rows| {
            let mut times = [None; MAX_BATCH];
            calendar.times(rows, 3, &mut times[..rows.len()]);
            for (f, time) in rows.iter().zip(times) { valid(f, time); }
        }))
    }
}
