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

    fn span(&self, start: u64, end: u64, visit: &mut impl FnMut(&[u8])) -> (usize, usize) {
        let bytes = unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len) };
        let start = (start as usize).min(bytes.len());
        let end = usize::try_from(end).unwrap_or(usize::MAX).min(bytes.len());
        let before = start.saturating_sub(1);
        let mut scanned = end - before;
        let mut overrun = 0;
        if before < end {
            if let Some(newline) = memchr(b'\n', &bytes[before..end]) {
                scanned = newline + 1;
                let first = before + newline + 1;
                if first < end {
                    let last = if end == bytes.len() || bytes[end - 1] == b'\n' {
                        end
                    } else {
                        memchr(b'\n', &bytes[end..]).map_or(bytes.len(), |offset| end + offset + 1)
                    };
                    overrun = last - end;
                    scanned += overrun + last - first;
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
        (scanned, overrun)
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

pub(crate) fn work_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| perf_on("PROFILE_WORK"))
}

#[derive(Default)]
struct IoWork {
    reads: u64,
    read_bytes: u64,
    mapped: u64,
    mapped_scan_bytes: u64,
    overrun: u64,
}

impl IoWork {
    fn read(&mut self, offset: u64, len: usize, end: u64) {
        self.reads += 1;
        self.read_bytes += len as u64;
        self.overrun += (offset + len as u64).saturating_sub(end) - offset.saturating_sub(end);
    }
}

const WORK_NAMES: [&str; 19] = ["tasks", "nominal_bytes", "read_calls", "read_bytes", "mapped_tasks",
    "mapped_scan_bytes", "overrun_bytes", "rows", "row_bytes", "quoted_rows", "malformed_rows",
    "canonical_id_rows", "other_id_rows", "ts_f1_shape", "ts_f2_shape", "ts_epoch_digits",
    "ts_other", "over_128b_rows", "over_128k_rows"];
const GROUPS: [&str; 4] = ["events", "clicks", "links", "dimensions"];
type Counts = [u64; WORK_NAMES.len()];
static TOTALS: std::sync::Mutex<[Counts; 4]> = std::sync::Mutex::new([[0; WORK_NAMES.len()]; 4]);

fn profile_group(path: &Path) -> usize {
    let folder = path.parent().and_then(Path::file_name).and_then(|s| s.to_str());
    GROUPS[..3].iter().position(|&s| Some(s) == folder).unwrap_or(3)
}

fn observe_line(counts: &mut Counts, group: usize, line: &[u8]) {
    counts[7] += 1;
    counts[8] += line.len() as u64;
    counts[9] += u64::from(memchr(b'"', line).is_some());
    counts[17] += u64::from(line.len() > 128);
    counts[18] += u64::from(line.len() > 128 * 1024);
    let pair = match group {
        0 => fields::<7>(line).map(|f| (f[0],f[2],b'e')),
        1 => fields::<4>(line).map(|f| (f[0],f[3],b'k')),
        _ => {
            counts[10] += u64::from(fields::<2>(line).is_none());
            return;
        }
    };
    let Some((id, ts, prefix)) = pair else { counts[10] += 1; return; };
    let canonical = id.first() == Some(&prefix) &&
        (matches!(id.len(), 2..=9) && id[1..].iter().all(|v| v.is_ascii_digit() || v.is_ascii_lowercase())
         || id.len() == 33 && id[1..].iter().all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(v)));
    counts[if canonical {11} else {12}] += 1;
    let format = if ts.len()==20 && ts[10]==b'T' && ts[19]==b'Z' { 13 }
        else if ts.len()==25 && ts[10]==b'T' && matches!(ts[19],b'+'|b'-') {14}
        else if !ts.is_empty() && ts.iter().all(u8::is_ascii_digit) {15} else {16};
    counts[format] += 1;
}

fn submit_work(group: usize, nominal: u64, io: IoWork, mut counts: Counts) {
    counts[..7].copy_from_slice(&[1, nominal, io.reads, io.read_bytes, io.mapped, io.mapped_scan_bytes, io.overrun]);
    for (d,s) in TOTALS.lock().unwrap()[group].iter_mut().zip(counts) { *d += s; }
}

pub fn print_work() {
    if !work_enabled() { return; }
    let totals = TOTALS.lock().unwrap();
    for (name, counts) in GROUPS.into_iter().zip(totals.iter()) {
        let mut record = serde_json::Map::new();
        record.insert("stage".into(), format!("work_csv_{name}").into());
        for (&key, &value) in WORK_NAMES.iter().zip(counts) { record.insert(key.into(), value.into()); }
        eprintln!("profile {}", serde_json::Value::Object(record));
    }
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

    /// Diagnostic read prefix. Touch the same offsets on both backends,
    /// including every input page, without searching for record delimiters.
    pub(crate) fn probe_read(&self) -> Result<u64> {
        let sample = |bytes: &[u8]| bytes.iter().step_by(4096).map(|&v| u64::from(v)).sum();
        #[cfg(target_os = "linux")]
        if let Some(map) = &self.source.mapping {
            let bytes = unsafe { std::slice::from_raw_parts(map.data.as_ptr(), map.len) };
            return Ok(sample(&bytes[self.start as usize..self.end as usize]));
        }
        let mut file = File::open(&self.source.path)?;
        file.seek(SeekFrom::Start(self.start))?;
        SCAN_BUFFER.with(|cell| {
            let mut buffer = cell.borrow_mut();
            let mut remaining = self.byte_len();
            let mut checksum = 0;
            while remaining > 0 {
                let n = remaining.min(buffer.bytes.len());
                file.read_exact(&mut buffer.bytes[..n])?;
                checksum += sample(&buffer.bytes[..n]);
                remaining -= n;
            }
            Ok(checksum)
        })
    }

    pub fn scan(&self, mut row: impl FnMut(&[u8])) -> Result<()> {
        let mut io = IoWork::default();
        if !work_enabled() { return self.scan_inner::<false>(row, &mut io); }
        let group = profile_group(&self.source.path);
        let mut counts = Counts::default();
        let result = self.scan_inner::<true>(|line| { observe_line(&mut counts, group, line); row(line); }, &mut io);
        submit_work(group, self.end-self.start, io, counts);
        result
    }

    fn scan_inner<const WORK: bool>(&self, mut row: impl FnMut(&[u8]), io: &mut IoWork) -> Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(map) = &self.source.mapping {
            let (n, extra) = map.span(self.start, self.end, &mut |span| each_line(span, &mut row));
            if WORK { io.mapped = 1; io.mapped_scan_bytes=n as u64; io.overrun=extra as u64; }
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
                if WORK { io.read(offset, len, self.end); }
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
        let mut io = IoWork::default();
        if !work_enabled() { return self.spans_inner::<false>(visit,&mut io); }
        let group = profile_group(&self.source.path);
        let mut counts = Counts::default();
        let result = self.spans_inner::<true>(|span| {
            each_line(span, &mut |line| observe_line(&mut counts,group,line));
            visit(span);
        }, &mut io);
        submit_work(group,self.end-self.start,io,counts);
        result
    }

    fn spans_inner<const WORK: bool>(&self, mut visit: impl FnMut(&[u8]), io: &mut IoWork) -> Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(map) = &self.source.mapping {
            let (n, extra) = map.span(self.start, self.end, &mut visit);
            if WORK { io.mapped=1; io.mapped_scan_bytes=n as u64; io.overrun=extra as u64; }
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
                if WORK { io.read(offset,n,self.end); }
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

// Tiny chunks and header-only files must not each retain a complete matrix
// of per-bucket Vec headers/slab padding for the duration of event ingestion.
// Grouping changes neither file reads nor ownership of crossing records.
pub(crate) fn work_groups(chunks: &[Chunk], requested: usize, metadata_per_task: usize) -> Vec<&[Chunk]> {
    const METADATA_TARGET: usize = 128 << 20;
    group_slices(chunks, requested, metadata_per_task, METADATA_TARGET)
}

fn group_slices(chunks: &[Chunk], requested: usize, metadata: usize, budget: usize) -> Vec<&[Chunk]> {
    let requested = requested.max(1);
    let max_groups = (budget / metadata.max(1)).max(1);
    let count = chunks.len().div_ceil(max_groups).max(requested);
    if count == requested {
        return chunks.chunks(requested).collect();
    }
    let mut groups = Vec::new();
    let mut first = 0;
    let mut bytes = 0;
    // A scorer using one original chunk per job can encode each candidate
    // position in 22 bits. Keep the same limit, including groups across files.
    let max_bytes = requested.saturating_mul(CHUNK_BYTES as usize);
    for (i, chunk) in chunks.iter().enumerate() {
        let len = chunk.byte_len();
        if i > first && (i - first == count || bytes > max_bytes - len) {
            groups.push(&chunks[first..i]);
            first = i;
            bytes = 0;
        }
        bytes += len;
    }
    if first < chunks.len() { groups.push(&chunks[first..]); }
    groups
}

#[cfg(test)]
mod scan_tests {
    use super::*;

    #[test]
    fn task_metadata_groups_small_inputs_without_overflowing_packed_positions() {
        let source = Arc::new(Source::new(PathBuf::new(), 0, false));
        let make = |sizes: &[usize]| sizes.iter().map(|&n| Chunk {
            source: source.clone(), start: 0, end: n as u64,
        }).collect::<Vec<_>>();
        let tiny = make(&[4096; 128]);
        assert_eq!(group_slices(&tiny, 1, 64, 128).len(), 2);
        let sizes = [1, 3 << 20, 1, 2 << 20, 2, 3 << 20];
        let varied = make(&sizes);
        let groups = group_slices(&varied, 1, 1024, 1);
        assert_eq!(groups.iter().map(|g| g.len()).sum::<usize>(), sizes.len());
        assert!(groups.iter().all(|g| g.iter().map(Chunk::byte_len).sum::<usize>() <= CHUNK_BYTES as usize));
        assert_eq!(group_slices(&varied, 1, 64, 1 << 20).len(), varied.len());
        assert!(group_slices(&[], 1, 64, 1).is_empty());
    }

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
        let rows = [(7, block - 8), (block, 2 * block + 17), (3 * block + 18, 1)];
        for chunk in chunks(&directory)? {
            let mut lengths = Vec::new();
            chunk.scan(|line| lengths.push(line.len()))?;
            let expected: Vec<_> = rows.iter().filter_map(|&(start, len)| {
                (chunk.start <= start as u64 && (start as u64) < chunk.end).then_some(len)
            }).collect();
            assert_eq!(lengths, expected, "chunk {}..{}", chunk.start, chunk.end);
        }
        fs::remove_file(part)?;
        fs::remove_dir(directory)?;
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
    let len = path.metadata()?.len();
    Chunk {
        source: Arc::new(Source::new(path.to_owned(), len, use_mmap())),
        start: 0,
        end: len,
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
pub(crate) fn event_payload(
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
                let (time, ingest) = calendar.event_times(f[2], f[3]);
                let payload = event_payload(&f, time, ingest, window, dim);
                emit(f[0], f[1], ingest, payload)
            });
        }
        self.spans(|span| parsed_groups::<7>(span, size, &mut |rows| {
            if calendar.paired_times {
                for f in rows {
                    let (time, ingest) = calendar.event_times(f[2], f[3]);
                    let payload = event_payload(f, time, ingest, window, dim);
                    emit(f[0], f[1], ingest, payload);
                }
                return;
            }
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
            if calendar.paired_times {
                for f in rows { valid(f, calendar.parse(f[3])); }
                return;
            }
            let mut times = [None; MAX_BATCH];
            calendar.times(rows, 3, &mut times[..rows.len()]);
            for (f, time) in rows.iter().zip(times) { valid(f, time); }
        }))
    }
}
