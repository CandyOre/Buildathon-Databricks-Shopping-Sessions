use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ahash::{AHashMap, RandomState};
use anyhow::{Context, Result};
use chrono::NaiveDate;
use rayon::prelude::*;
use serde_json::Value;

use crate::csv::{chunks, fields, perf_on, scan_file};

pub const DAY: i64 = 86_400_000;
pub const LOOKBACK: i64 = 7 * DAY;
pub const SHARDS: usize = 128;

const HEX: [u8; 256] = {
    let mut digits = [0x80; 256];
    let mut n = 0;
    while n < 16 {
        let byte = if n < 10 { b'0' + n } else { b'a' + (n - 10) };
        digits[byte as usize] = n;
        n += 1;
    }
    digits
};

pub fn hex_id(bytes: &[u8], prefix: u8) -> Option<[u64; 2]> {
    if bytes.len() != 33 || bytes[0] != prefix { return None; }
    let digits = <&[u8; 32]>::try_from(&bytes[1..]).unwrap();
    #[cfg(target_arch = "x86_64")]
    {
        static SIMD: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        if *SIMD.get_or_init(|| std::is_x86_feature_detected!("avx2")
                && std::env::var_os("PERF_HEX").is_some_and(|v| v == "simd")) {
            return unsafe { hex_avx2(digits) };
        }
    }
    hex_scalar(digits)
}

fn hex_scalar(digits: &[u8;32]) -> Option<[u64;2]> {
    let mut bytes = [0; 16];
    let mut invalid = 0;
    for (pair, dest) in digits.chunks_exact(2).zip(&mut bytes) {
        let high = HEX[pair[0] as usize];
        let low = HEX[pair[1] as usize];
        invalid |= high | low;
        *dest = (high << 4) | low;
    }
    if invalid & 128 != 0 { return None; }
    Some([u64::from_be_bytes(bytes[..8].try_into().unwrap()),
          u64::from_be_bytes(bytes[8..].try_into().unwrap())])
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable="avx2")]
unsafe fn hex_avx2(digits: &[u8; 32]) -> Option<[u64; 2]> {
    use std::arch::x86_64::*;
    unsafe {
        let input = _mm256_loadu_si256(digits.as_ptr().cast());
        let in_range = |lo: i8, hi: i8| {
            _mm256_and_si256(_mm256_cmpgt_epi8(input, _mm256_set1_epi8(lo - 1)),
                             _mm256_cmpgt_epi8(_mm256_set1_epi8(hi + 1), input))
        };
        let decimal = in_range(b'0' as i8, b'9' as i8);
        let letter = in_range(b'a' as i8, b'f' as i8);
        if _mm256_movemask_epi8(_mm256_or_si256(decimal, letter)) != -1 { return None; }
        let nibbles = _mm256_add_epi8(_mm256_and_si256(input, _mm256_set1_epi8(15)),
                                      _mm256_and_si256(letter, _mm256_set1_epi8(9)));
        let pairs = _mm256_maddubs_epi16(nibbles, _mm256_set1_epi16(0x0110));
        let bytes = _mm_packus_epi16(_mm256_castsi256_si128(pairs), _mm256_extracti128_si256::<1>(pairs));
        let mut key = [0u64; 2];
        _mm_storeu_si128(key.as_mut_ptr().cast(), bytes);
        Some(key.map(u64::from_be))
    }
}

#[derive(Clone, Copy)]
pub struct Window {
    pub start: i64,
    pub end: i64,
}

impl Window {
    pub fn read(data: &Path) -> Result<Self> {
        let manifest: Value = serde_json::from_reader(File::open(data.join("manifest.json"))?)?;
        let field = |key: &str| {
            manifest[key]
                .as_str()
                .and_then(|s| timestamp(s.as_bytes()))
                .with_context(|| format!("invalid manifest {key}"))
        };
        Ok(Self {
            start: field("window_start")?,
            end: field("window_end")?,
        })
    }
}

pub fn unsigned(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    // Every unsigned decimal with at most 18 digits fits in i64.
    if bytes.len() <= 18 {
        let mut value = 0;
        for &byte in bytes {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            value = value * 10 + i64::from(digit);
        }
        return Some(value);
    }
    let mut value = 0_i64;
    for &byte in bytes {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
    }
    Some(value)
}

fn two_digits(tens: u8, units: u8) -> Option<u32> {
    let tens = tens.wrapping_sub(b'0');
    let units = units.wrapping_sub(b'0');
    if tens > 9 || units > 9 {
        return None;
    }
    Some(u32::from(tens) * 10 + u32::from(units))
}

fn midnight(date: &[u8; 10]) -> Option<i64> {
    // Most rows in a chunk share a small set of calendar dates. A slot is keyed
    // by the full date so other months, years and malformed dates cannot alias it.
    type Entry = std::cell::Cell<Option<([u8; 10], i64)>>;
    thread_local! {
        static MIDNIGHTS: [Entry; 32] = const { [const { Entry::new(None) }; 32] };
    }
    MIDNIGHTS.with(|cache| {
        // Mix the raw day digits. Checking the full key makes a cache hit
        // sufficient to validate even an untrusted date.
        let slot = &cache[(usize::from(date[8]) * 10 + usize::from(date[9])) % cache.len()];
        if let Some((key, value)) = slot.get() {
            if key == *date {
                return Some(value);
            }
        }
        if date[4] != b'-' || date[7] != b'-' {
            return None;
        }
        let year = two_digits(date[0], date[1])? * 100 + two_digits(date[2], date[3])?;
        let month = two_digits(date[5], date[6])?;
        let day = two_digits(date[8], date[9])?;
        let value = NaiveDate::from_ymd_opt(year as i32, month, day)?
            .and_hms_opt(0, 0, 0)?
            .and_utc()
            .timestamp_millis();
        slot.set(Some((*date, value)));
        Some(value)
    })
}

fn all_digits(word: u64) -> bool {
    // Check the high nibbles for ASCII '0' and the low nibbles for <= 9.
    // No byte of low + 6 can carry into a neighbour.
    let low = word & 0x0f0f_0f0f_0f0f_0f0f;
    word & 0xf0f0_f0f0_f0f0_f0f0 == 0x3030_3030_3030_3030
        && (low + 0x0606_0606_0606_0606) & 0x1010_1010_1010_1010 == 0
}

fn eight_digits(word: u64) -> u32 {
    // Combine pairs of decimal digits, then neighbouring pairs. Each mask
    // leaves enough space for the next multiplication to stay within its lane.
    let digits = word & 0x0f0f_0f0f_0f0f_0f0f;
    let pairs = (digits * 10 + (digits >> 8)) & 0x00ff_00ff_00ff_00ff;
    let quads = (pairs * 100 + (pairs >> 16)) & 0x0000_ffff_0000_ffff;
    (quads as u32) * 10_000 + (quads >> 32) as u32
}

fn epoch_13(bytes: &[u8]) -> Option<i64> {
    let head = u64::from_le_bytes(bytes[..8].try_into().ok()?);
    let tail = u64::from_le_bytes(bytes[5..].try_into().ok()?);
    if !all_digits(head) || !all_digits(tail) {
        return None;
    }
    // The two loads overlap on three digits.
    Some(i64::from(eight_digits(head) / 1_000) * 100_000_000 + i64::from(eight_digits(tail)))
}

// Positions in the little-endian bytes of HH:MM:SS.
const CLOCK_COLONS: u64 = 0x0000_ff00_00ff_0000;

fn clock_components(word: u64) -> (u32, u32, u32) {
    let digits = word & 0x0f0f_000f_0f00_0f0f;
    let pairs = digits * 10 + (digits >> 8);
    (
        (pairs & 255) as u32,
        ((pairs >> 24) & 255) as u32,
        ((pairs >> 48) & 255) as u32,
    )
}

#[cfg(test)]
pub fn ingest_timestamp(bytes: &[u8]) -> i64 {
    // The input contract guarantees F1 for ingest_ts.
    let bytes: &[u8; 20] = bytes.try_into().expect("ingest_ts is valid F1");
    let day = midnight(bytes[..10].try_into().unwrap()).expect("ingest_ts has a valid date");
    let word = u64::from_le_bytes(bytes[11..19].try_into().unwrap());
    let (hour, minute, second) = clock_components(word);
    day + i64::from(hour * 3600 + minute * 60 + second) * 1000
}

pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    if bytes.len() == 13 {
        return epoch_13(bytes);
    }
    let offset_minutes = match bytes {
        [.., b'Z'] if bytes.len() == 20 => 0,
        [
            ..,
            sign @ (b'+' | b'-'),
            hour_a,
            hour_b,
            b':',
            minute_a,
            minute_b,
        ] if bytes.len() == 25 => {
            let hours = two_digits(*hour_a, *hour_b)?;
            let minutes = two_digits(*minute_a, *minute_b)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            (if *sign == b'+' { 1 } else { -1 }) * i64::from(hours * 60 + minutes)
        }
        _ => return unsigned(bytes),
    };
    let word = u64::from_le_bytes(bytes[11..19].try_into().ok()?);
    if bytes[10] != b'T'
        || word & CLOCK_COLONS != 0x0000_3a00_003a_0000
        || !all_digits((word & !CLOCK_COLONS) | (0x3030_3030_3030_3030 & CLOCK_COLONS))
    {
        return None;
    }
    let (hour, minute, second) = clock_components(word);
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let start = midnight(bytes[..10].try_into().ok()?)?;
    let seconds = i64::from(hour * 3600 + minute * 60 + second);
    Some(start + seconds * 1000 - offset_minutes * 60_000)
}

#[cfg(test)]
mod timestamp_tests {
    use super::{DAY, ingest_timestamp, timestamp, unsigned};

    #[test]
    fn packed_clock_and_epoch_keep_the_digit_boundaries() {
        let last = b"2028-02-29T23:59:59Z";
        let base = timestamp(b"2028-02-29T00:00:00Z").unwrap();
        assert_eq!(timestamp(last), Some(base + DAY - 1000));
        assert_eq!(ingest_timestamp(last), base + DAY - 1000);
        for byte in 0..=255u8 {
            for index in 0..13 {
                let mut epoch = *b"0123456789012";
                epoch[index] = byte;
                assert_eq!(timestamp(&epoch), unsigned(&epoch));
            }
            let mut clock = *last;
            clock[13] = byte;
            assert_eq!(
                timestamp(&clock),
                (byte == b':').then_some(base + DAY - 1000)
            );
        }
    }

    #[test]
    fn cached_midnights_distinguish_months_years_and_invalid_dates() {
        let leap_day = timestamp(b"2028-02-29T00:00:00Z").unwrap();
        assert_eq!(timestamp(b"2028-02-29T01:00:00+01:00"), Some(leap_day));
        assert_eq!(timestamp(b"2027-02-29T00:00:00Z"), None);
        assert_eq!(timestamp(b"2028/02-29T00:00:00Z"), None);
        assert_eq!(
            timestamp(b"2028-03-29T00:00:00Z"),
            Some(leap_day + 29 * DAY)
        );
        assert_eq!(timestamp(b"2028-02-29T00:00:00Z"), Some(leap_day));
    }

    #[test]
    fn epoch_numbers_support_padding_and_reject_overflow() {
        assert_eq!(unsigned(b"00000000000000000000000000001"), Some(1));
        assert_eq!(unsigned(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(unsigned(b"9223372036854775808"), None);
        assert_eq!(unsigned(b"00000000000000:1"), None);
    }
}

#[derive(Clone, Copy, Default)]
struct CachedDay {
    month: u64,
    day: u16,
    midnight: i64,
}

// Cache dates around the manifest window and common decimal epoch prefixes.
// Always compare the complete cached key before accepting a hit.
pub struct Calendar {
    days: [CachedDay; 64],
    epochs: [(u32, i64); 16],
}

impl Calendar {
    pub fn new(window: Window) -> Self {
        let mut table = Self {
            days: [CachedDay::default(); 64],
            epochs: [(0, 0); 16],
        };
        let first = window.start.saturating_sub(LOOKBACK + DAY).div_euclid(DAY);
        let last = window
            .end
            .saturating_add(DAY * 2)
            .div_euclid(DAY)
            .min(first.saturating_add(61));
        for d in first..=last {
            if let Some(date) = chrono::DateTime::from_timestamp(d * 86400, 0) {
                let text = date.format("%Y-%m-%d").to_string();
                let bytes = text.as_bytes();
                if bytes.len() == 10 {
                    table.days[Self::slot(bytes)] = CachedDay {
                        month: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                        day: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
                        midnight: d * DAY,
                    };
                }
            }
            let prefix = (d * DAY).div_euclid(1_000_000_000);
            for n in [prefix, prefix + 1] {
                if (1000..=9999).contains(&n) {
                    let code = n.to_string();
                    table.epochs[(code.as_bytes()[3] & 15) as usize] = (
                        u32::from_le_bytes(code.as_bytes().try_into().unwrap()),
                        n * 1_000_000_000,
                    );
                }
            }
        }
        table
    }

    #[inline(always)]
    fn slot(s: &[u8]) -> usize {
        usize::from(s[6] & 1) * 32 + ((usize::from(s[8]) * 10 + usize::from(s[9])) & 31)
    }

    #[inline(always)]
    fn date(&self, s: &[u8]) -> Option<i64> {
        let entry = &self.days[Self::slot(s)];
        let month = u64::from_le_bytes(s[..8].try_into().unwrap());
        let day = u16::from_le_bytes(s[8..10].try_into().unwrap());
        if month == entry.month && day == entry.day && month != 0 {
            Some(entry.midnight)
        } else {
            midnight(s[..10].try_into().unwrap())
        }
    }

    #[inline]
    pub fn ingest(&self, s: &[u8]) -> i64 {
        let s: &[u8; 20] = s.try_into().expect("ingest_ts is valid F1");
        let word = u64::from_le_bytes(s[11..19].try_into().unwrap());
        let (h, m, sec) = clock_components(word);
        self.date(s).expect("ingest_ts has a valid date")
            + i64::from(h * 3600 + m * 60 + sec) * 1000
    }

    #[inline]
    pub fn parse(&self, s: &[u8]) -> Option<i64> {
        if s.len() == 13 {
            let (key, base) = self.epochs[(s[3] & 15) as usize];
            if key != 0 && key == u32::from_le_bytes(s[..4].try_into().unwrap()) {
                let digit = s[4].wrapping_sub(b'0');
                let tail = u64::from_le_bytes(s[5..].try_into().unwrap());
                if digit > 9 || !all_digits(tail) {
                    return None;
                }
                return Some(base + i64::from(digit) * 100_000_000 + i64::from(eight_digits(tail)));
            }
        }
        if s.len() == 20 && s[19] == b'Z' {
            let word = u64::from_le_bytes(s[11..19].try_into().unwrap());
            if s[10] != b'T'
                || word & CLOCK_COLONS != 0x0000_3a00_003a_0000
                || !all_digits((word & !CLOCK_COLONS) | (0x3030_3030_3030_3030 & CLOCK_COLONS))
            {
                return None;
            }
            let (h, m, sec) = clock_components(word);
            if h > 23 || m > 59 || sec > 59 {
                return None;
            }
            return Some(self.date(s)? + i64::from(h * 3600 + m * 60 + sec) * 1000);
        }
        timestamp(s)
    }

    pub(crate) fn times<const N: usize>(
        &self, rows: &[[&[u8]; N]], column: usize, output: &mut [Option<i64>],
    ) {
        debug_assert_eq!(rows.len(), output.len());
        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("avx2") {
            let mut lanes = [0; 4];
            let mut count = 0;
            for (i, row) in rows.iter().enumerate() {
                let s = row[column];
                if (s.len() == 20 && s[19] == b'Z' || s.len() == 25 && matches!(s[19], b'+' | b'-'))
                    && s[10] == b'T'
                {
                    lanes[count] = i;
                    count += 1;
                    if count == 4 {
                        let words = lanes.map(|j| u64::from_le_bytes(rows[j][column][11..19].try_into().unwrap()));
                        let seconds = unsafe { four_clocks(words) };
                        for (j, sec) in lanes.into_iter().zip(seconds) {
                            let s = rows[j][column];
                            output[j] = if sec == u64::MAX { None } else {
                                self.date(s).zip(zone_offset(s)).map(|(day, zone)|
                                    day + (sec as i64) * 1000 - zone)
                            };
                        }
                        count = 0;
                    }
                } else {
                    output[i] = self.parse(s);
                }
            }
            for &j in &lanes[..count] { output[j] = self.parse(rows[j][column]); }
            return;
        }
        for (value, row) in output.iter_mut().zip(rows) { *value = self.parse(row[column]); }
    }
}

#[cfg(target_arch = "x86_64")]
fn zone_offset(s: &[u8]) -> Option<i64> {
    if s.len() == 20 { return Some(0); }
    let h = two_digits(s[20], s[21])?;
    let m = two_digits(s[23], s[24])?;
    if h > 23 || m > 59 || s[22] != b':' { return None; }
    Some(i64::from(h * 60 + m) * if s[19] == b'+' { 60_000 } else { -60_000 })
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable="avx2")]
unsafe fn four_clocks(words: [u64;4]) -> [u64;4] {
    use std::arch::x86_64::*;
    unsafe {
        let s = _mm256_loadu_si256(words.as_ptr().cast());
        let colons = _mm256_set1_epi64x(CLOCK_COLONS as i64);
        let colon_ok = _mm256_cmpeq_epi64(_mm256_and_si256(s, colons), _mm256_set1_epi64x(0x3a00_003a_0000));
        let digits = _mm256_blendv_epi8(s, _mm256_set1_epi8(b'0' as i8), colons);
        let decimal = _mm256_and_si256(_mm256_cmpgt_epi8(digits, _mm256_set1_epi8(b'/' as i8)),
                                       _mm256_cmpgt_epi8(_mm256_set1_epi8(b':' as i8), digits));
        let ok = _mm256_and_si256(colon_ok, _mm256_cmpeq_epi64(decimal, _mm256_set1_epi64x(-1)));
        let values = _mm256_and_si256(s, _mm256_set1_epi64x(0x0f0f_000f_0f00_0f0f));
        let pairs = _mm256_add_epi64(_mm256_srli_epi64::<8>(values),
                      _mm256_add_epi64(_mm256_slli_epi64::<3>(values), _mm256_slli_epi64::<1>(values)));
        let mask = _mm256_set1_epi64x(255);
        let h = _mm256_and_si256(pairs, mask);
        let m = _mm256_and_si256(_mm256_srli_epi64::<24>(pairs), mask);
        let sec = _mm256_and_si256(_mm256_srli_epi64::<48>(pairs), mask);
        let hm = _mm256_and_si256(_mm256_cmpgt_epi64(_mm256_set1_epi64x(24), h),
                                  _mm256_cmpgt_epi64(_mm256_set1_epi64x(60), m));
        let valid = _mm256_and_si256(ok, _mm256_and_si256(hm, _mm256_cmpgt_epi64(_mm256_set1_epi64x(60), sec)));
        let result = _mm256_add_epi64(sec, _mm256_add_epi64(_mm256_mul_epu32(h, _mm256_set1_epi64x(3600)),
                                                           _mm256_mul_epu32(m, _mm256_set1_epi64x(60))));
        let mut out = [0;4];
        _mm256_storeu_si256(out.as_mut_ptr().cast(), _mm256_blendv_epi8(_mm256_set1_epi64x(-1), result, valid));
        out
    }
}

pub fn text(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).expect("input fields must be UTF-8")
}

pub struct DimIndex<const CAMPAIGN: bool> {
    dense: DenseValues,
    other: AHashMap<String, u32>,
}

enum DenseValues {
    Wide(Vec<u32>),
    Byte(Vec<u8>),
}
impl<const C: bool> DimIndex<C> {
    #[inline]
    fn number(name: &[u8]) -> Option<usize> {
        let d = if C {
            name.strip_prefix(b"c_")?
        } else {
            name.strip_prefix(b"p")?
        };
        if d.len() > 7 || d.is_empty() || (d[0] == b'0' && d.len() != 1) {
            return None;
        }
        let mut n = 0;
        for &c in d {
            let x = c.wrapping_sub(b'0');
            if x > 9 {
                return None;
            }
            n = n * 10 + usize::from(x);
        }
        Some(n)
    }
    #[inline]
    pub fn lookup(&self, name: &[u8]) -> Option<u32> {
        if let Some(n) = Self::number(name) {
            match &self.dense {
                DenseValues::Wide(values) => if let Some(&id) = values.get(n) {
                    return (id != u32::MAX).then_some(id);
                },
                DenseValues::Byte(values) => if let Some(&id) = values.get(n) {
                    return (id != u8::MAX).then_some(u32::from(id));
                },
            }
        }
        self.other.get(text(name)).copied()
    }
}
impl<const C: bool> FromIterator<(String, u32)> for DimIndex<C> {
    fn from_iter<T: IntoIterator<Item = (String, u32)>>(iter: T) -> Self {
        let rows: Vec<_> = iter.into_iter().collect();
        let limit = rows.len().saturating_mul(4).min(2_000_000);
        let mut dense = Vec::new();
        let mut other = AHashMap::default();
        for (s, id) in rows {
            if let Some(n) = Self::number(s.as_bytes()).filter(|&n| n <= limit) {
                if n >= dense.len() {
                    dense.resize(n + 1, u32::MAX);
                }
                dense[n] = id;
            } else {
                other.insert(s, id);
            }
        }
        let dense = if !C && perf_on("PERF_DIM_NARROW")
            && dense.iter().all(|&value| value < 255 || value == u32::MAX) {
            // Casting the wide missing-value sentinel yields the byte sentinel.
            DenseValues::Byte(dense.into_iter().map(|value| value as u8).collect())
        } else {
            DenseValues::Wide(dense)
        };
        Self { dense, other }
    }
}

pub struct Dimensions {
    pub categories: Vec<String>,
    pub products: DimIndex<false>,
    pub unknown_category: u32,
    pub campaigns: Vec<(String, String)>,
    pub campaign_ids: DimIndex<true>,
}

impl Dimensions {
    pub fn read(data: &Path) -> Result<Self> {
        let read_products = || -> Result<_> {
            let mut pairs = Vec::new();
            scan_file(&data.join("products.csv"), |row| {
                if let Some([product, category]) = fields(row) {
                    pairs.push((text(product).to_owned(), text(category).to_owned()));
                }
            })?;
            let mut categories: Vec<_> = pairs.iter().map(|(_, category)| category.clone()).collect();
            categories.push("unknown".into());
            categories.sort_unstable();
            categories.dedup();
            let unknown_category = categories
                .binary_search_by(|s| s.as_str().cmp("unknown"))
                .unwrap() as u32;
            let products = pairs
                .into_iter()
                .map(|(product, category)| {
                    (product, categories.binary_search(&category).unwrap() as u32)
                })
                .collect();
            Ok((categories, products, unknown_category))
        };
        let read_campaigns = || -> Result<_> {
            let mut campaigns = vec![("organic".into(), "organic".into())];
            scan_file(&data.join("campaigns.csv"), |row| {
                if let Some([campaign, channel]) = fields(row) {
                    campaigns.push((text(campaign).to_owned(), text(channel).to_owned()));
                }
            })?;
            let campaign_ids = campaigns
                .iter()
                .enumerate()
                .skip(1)
                .map(|(id, (campaign, _))| (campaign.clone(), id as u32))
                .collect();
            Ok((campaigns, campaign_ids))
        };
        let (products, campaigns) = if perf_on("PERF_DIM_PARALLEL") {
            rayon::join(read_products, read_campaigns)
        } else {
            (read_products(), read_campaigns())
        };
        let (categories, products, unknown_category) = products?;
        let (campaigns, campaign_ids) = campaigns?;
        Ok(Self {
            categories,
            products,
            unknown_category,
            campaigns,
            campaign_ids,
        })
    }
}

#[derive(Default)]
struct NameShard {
    offset: usize,
    keys: Vec<u64>,
    ids: AHashMap<Arc<str>, u32>,
    wide: Option<AHashMap<[u64;2], u32>>,
    names: Vec<Arc<str>>,
}

impl NameShard {
    fn intern(&mut self, shard: usize, name: &str) -> u32 {
        if let Some(&id) = self.ids.get(name) {
            return id;
        }
        let id = id_for(shard, self.offset + self.keys.len());
        self.keys.push(EXTERNAL_NAME | self.names.len() as u64);
        let name: Arc<str> = name.into();
        self.ids.insert(name.clone(), id);
        self.names.push(name);
        id
    }
}

pub struct Names {
    packed: Option<Arc<NameTable>>,
    wide_users: bool,
    hash: RandomState,
    shards: Vec<Mutex<NameShard>>,
}

impl Default for Names {
    fn default() -> Self {
        Self {
            packed: None,
            wide_users: false,
            hash: RandomState::new(),
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
        }
    }
}

impl Names {
    pub fn intern(&self, name: &str) -> u32 {
        if self.wide_users {
            if let Some(key) = hex_id(name.as_bytes(), b'u') {
                let shard = self.hash.hash_one(key) as usize % SHARDS;
                return self.intern_wide(shard, key, name);
            }
        }
        let shard = self.hash.hash_one(name.as_bytes()) as usize % SHARDS;
        self.intern_in(shard, name)
    }

    fn intern_in(&self, shard: usize, name: &str) -> u32 {
        self.shards[shard].lock().unwrap().intern(shard, name)
    }

    fn intern_wide(&self, shard: usize, key: [u64;2], name: &str) -> u32 {
        let mut state = self.shards[shard].lock().unwrap();
        if let Some(&id) = state.wide.as_ref().and_then(|map| map.get(&key)) { return id; }
        let id = id_for(shard, state.offset + state.keys.len());
        let spelling = EXTERNAL_NAME | state.names.len() as u64;
        state.keys.push(spelling);
        let name: Arc<str> = name.into();
        // Graph construction still needs the spelling both for its lexical
        // representative and for remapping all external endpoints at finish.
        if self.packed.is_none() { state.ids.insert(name.clone(), id); }
        state.names.push(name);
        state.wide.get_or_insert_with(AHashMap::default).insert(key, id);
        id
    }

    fn register_bounded(&self, key: u64, hash: usize, limit: u64) -> Option<u32> {
        let shard = hash % SHARDS;
        let mut state = self.shards[shard].lock().unwrap();
        let id = id_for(shard, state.offset + state.keys.len());
        if u64::from(id) >= limit {
            return None;
        }
        state.keys.push(key);
        Some(id)
    }

    pub fn into_names(self) -> Vec<Vec<Arc<str>>> {
        self.shards
            .into_iter()
            .map(|s| s.into_inner().unwrap().names)
            .collect()
    }

    pub fn person_slots_per_shard(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let state = shard.lock().unwrap();
                state.offset + state.keys.len()
            })
            .max()
            .unwrap_or(0)
    }

    pub fn into_people(mut self) -> PersonNames {
        let table = self.packed.take().expect("link graph must be initialized");
        PersonNames {
            table,
            dynamic: self
                .shards
                .into_iter()
                .map(|s| {
                    let state = s.into_inner().unwrap();
                    (state.offset, state.keys, state.names)
                })
                .collect(),
        }
    }
}

pub struct PersonNames {
    table: Arc<NameTable>,
    dynamic: Vec<(usize, Vec<u64>, Vec<Arc<str>>)>,
}

impl PersonNames {
    pub fn get<'a>(&'a self, id: u32, buffer: &'a mut [u8; 9]) -> &'a str {
        if let Some(&key) = self.table.canonical.get(id as usize) {
            self.table.spelling(key, buffer)
        } else {
            let (offset, keys, names) = &self.dynamic[id as usize % SHARDS];
            let key = keys[id as usize / SHARDS - *offset];
            if key >= EXTERNAL_NAME {
                &names[key as u32 as usize]
            } else {
                unpack_name(key, buffer)
            }
        }
    }
}

fn id_for(shard: usize, slot: usize) -> u32 {
    u32::try_from(slot * SHARDS + shard)
        .ok()
        .filter(|&id| id != u32::MAX)
        .expect("too many distinct names")
}

pub fn name_for<T: AsRef<[Arc<str>]>>(names: &[T], id: u32) -> &str {
    let id = id as usize;
    &names[id % SHARDS].as_ref()[id / SHARDS]
}

// The high 32 bits cannot be all ones for any packed UTF-8 key.
const EXTERNAL_NAME: u64 = 0xffff_ffff_0000_0000;
const EMPTY_KEY: u64 = u64::MAX;
struct Entry {
    key: AtomicU64,
    // During union this is a slot. After flattening it is a dense public person ID.
    parent: AtomicU32,
}

struct LinkGraph {
    entries: Vec<Entry>,
    external_names: Vec<Vec<Arc<str>>>,
}

struct NameTable {
    directory: Directory,
    canonical: Vec<u64>,
    external_names: Vec<Vec<Arc<str>>>,
    overflow: AHashMap<u64, u32>,
}

impl NameTable {
    fn spelling<'a>(&'a self, key: u64, buffer: &'a mut [u8; 9]) -> &'a str {
        linked_spelling(&self.external_names, key, buffer)
    }
}

fn linked_spelling<'a>(names: &'a [Vec<Arc<str>>], key: u64, buffer: &'a mut [u8; 9]) -> &'a str {
    if key >= EXTERNAL_NAME {
        name_for(names, key as u32)
    } else {
        unpack_name(key, buffer)
    }
}

pub struct Aliases {
    table: Arc<NameTable>,
    external: Vec<AHashMap<Arc<str>, u32>>,
    wide: Option<Vec<AHashMap<[u64;2],u32>>>,
}

impl Aliases {
    #[inline]
    fn resolve_external(&self, users: &Names, bytes: &[u8]) -> u32 {
        if let Some(wide) = &self.wide {
            if let Some(key) = hex_id(bytes, b'u') {
                let shard = users.hash.hash_one(key) as usize % SHARDS;
                return wide[shard].get(&key).copied()
                    .unwrap_or_else(|| users.intern_wide(shard,key,text(bytes)));
            }
        }
        let shard = users.hash.hash_one(bytes) as usize % SHARDS;
        let name = text(bytes);
        self.external[shard].get(name).copied().unwrap_or_else(|| users.intern_in(shard,name))
    }

    pub(crate) fn is_linked_person(&self, id: u32) -> bool {
        (id as usize) < self.table.canonical.len()
    }
}

// A short ASCII spelling needs 57 bits, including the U/u discriminator.
// The lookup word is reversibly mixed. The bucket address supplies its low
// bits, so the stored quotient and the returned dense ID share one u64.
// One aligned cache line holds the seven one-byte fingerprints and all
// seven complete quotient/ID words. A byte of zero means unpublished.
#[repr(C, align(64))]
struct Line {
    tags: AtomicU64,
    cells: [AtomicU64; 7],
}

impl Line {
    // Zero is an unpublished slot. Encoded tags are immutable after publication.
    #[inline]
    fn fingerprint(tag: u64) -> u64 {
        (tag >> 56) | 1
    }

    #[inline]
    fn publish(&self, index: usize, tag: u64) {
        self.tags
            .fetch_or(Self::fingerprint(tag) << (8 * index), Ordering::Release);
    }

    #[inline]
    fn cached(&self, tag: u64, limit: u64, tags: u64) -> Option<u32> {
        const ONES: u64 = 0x0101_0101_0101_0101;
        const HIGH: u64 = 0x0080_8080_8080_8080;
        let diff = tags ^ (Self::fingerprint(tag) * ONES);
        // Borrow propagation may mark a neighbouring byte; the full stored
        // quotient is checked for every candidate and rejects such false hits.
        let mut hits = diff.wrapping_sub(ONES) & !diff & HIGH;
        while hits != 0 {
            let lane = (hits.trailing_zeros() / 8) as usize;
            let id = self.cells[lane].load(Ordering::Relaxed) ^ tag;
            if id < limit {
                return Some(id as u32);
            }
            hits &= hits - 1;
        }
        None
    }
}

struct Directory {
    lines: Vec<Line>,
    address_bits: u32,
    id_mask: u64,
}
const RESERVED: u64 = u64::MAX - 1;
const CLOSED: u64 = u64::MAX - 2;
const ASCII_WORD: u64 = 0x7f7f_7f7f_7f7f_7f7f;
#[inline]
fn ascii_code(key: u64) -> Option<u64> {
    let suffix = key & !(1 << 63);
    if suffix & !ASCII_WORD != 0 {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("bmi2") {
        return Some(
            unsafe { std::arch::x86_64::_pext_u64(suffix, ASCII_WORD) } | ((key >> 63) << 56),
        );
    }
    let pairs = (suffix & 0x007f_007f_007f_007f) | ((suffix >> 1) & 0x3f80_3f80_3f80_3f80);
    let quads = (pairs & 0x0000_3fff_0000_3fff) | ((pairs >> 2) & 0x0fff_c000_0fff_c000);
    Some((quads & 0x0fff_ffff) | ((quads >> 4) & 0x00ff_ffff_f000_0000) | ((key >> 63) << 56))
}
#[inline]
fn mixed_word(key: u64) -> Option<u64> {
    let x = ascii_code(key)?;
    let x = ((x ^ (x >> 25)).wrapping_mul(0xa24b_aed4_963e_e407)) & ((1u64 << 57) - 1);
    Some(x ^ (x >> 24))
}

#[cfg(feature = "large-input")]
#[inline]
fn materialize_key(key: u64, mixed: u64, bytes: &[u8]) -> u64 {
    if key != EMPTY_KEY {
        return key;
    }
    if !bytes.is_empty() {
        return packed_name(bytes).expect("packed query");
    }
    // A queued lookup carries the reversible 57-bit result from mixed_word.
    // Undo the two XOR shifts and the odd multiplication, then expand the
    // seven-bit lanes back into the spelling. This is needed only on insertion.
    let multiplied = mixed ^ (mixed >> 24) ^ (mixed >> 48);
    let xor = multiplied.wrapping_mul(0x0195_1323_f693_49b7) & ((1 << 57) - 1);
    let code = xor ^ (xor >> 25) ^ (xor >> 50);
    let sign = (code >> 56) << 63;
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("bmi2") {
        return sign | unsafe { std::arch::x86_64::_pdep_u64(code, ASCII_WORD) };
    }
    let mut packed = sign;
    for lane in 0..8 {
        packed |= ((code >> (lane * 7)) & 127) << (lane * 8);
    }
    packed
}

#[cfg(feature = "large-input")]
#[cold]
fn intern_on_overflow(users: &Names, key: u64, mixed: u64, bytes: &[u8]) -> u32 {
    if bytes.is_empty() {
        let key = materialize_key(key, mixed, bytes);
        users.intern(unpack_name(key, &mut [0; 9]))
    } else {
        users.intern(text(bytes))
    }
}

#[cfg(feature = "large-input")]
impl Prepared {
    #[inline]
    pub fn needs_lookup(self) -> bool {
        self.0 < READY
    }
}

impl Directory {
    fn new(slots: usize) -> Self {
        let buckets = slots / 8;
        let address_bits = buckets.trailing_zeros();
        let lines = (0..buckets)
            .into_par_iter()
            .map(|_| Line {
                tags: AtomicU64::new(0),
                cells: std::array::from_fn(|_| AtomicU64::new(EMPTY_KEY)),
            })
            .collect();
        Self {
            lines,
            address_bits,
            id_mask: (1u64 << (address_bits + 6)) - 1,
        }
    }

    #[inline]
    fn home(&self, mixed: u64) -> usize {
        mixed as usize & (self.lines.len() - 1)
    }

    #[inline]
    fn alternate(&self, home: usize, quotient: u64) -> usize {
        let delta = (quotient ^ (quotient >> 23)).wrapping_mul(0x94d0_49bb_1331_11eb) as usize;
        home ^ ((delta | 1) & (self.lines.len() - 1))
    }

    fn locations(&self, mixed: u64) -> [(usize, u64); 2] {
        let home = self.home(mixed);
        let quotient = mixed >> self.address_bits;
        let tag = quotient << (self.address_bits + 7);
        [
            (home, tag),
            (self.alternate(home, quotient), tag | (self.id_mask + 1)),
        ]
    }

    fn fill(&self, mixed: u64, id: u32) -> bool {
        for (index, tag) in self.locations(mixed) {
            for (lane, cell) in self.lines[index].cells.iter().enumerate() {
                if cell.load(Ordering::Relaxed) == EMPTY_KEY
                    && cell
                        .compare_exchange(
                            EMPTY_KEY,
                            tag | u64::from(id),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                {
                    self.lines[index].publish(lane, tag);
                    return true;
                }
            }
        }
        false
    }

    #[inline]
    fn lookup_line(
        &self,
        line: &Line,
        tag: u64,
        key: u64,
        mixed: u64,
        bytes: &[u8],
        users: &Names,
    ) -> Option<u32> {
        // All real IDs are strictly below the last three values in the ID
        // field. XOR therefore checks both the complete quotient and the
        // sentinel exclusion with a single unsigned comparison.
        let limit = self.id_mask - 2;
        let tags = line.tags.load(Ordering::Acquire);
        if let Some(id) = line.cached(tag, limit, tags) {
            return Some(id);
        }
        // All seven nonzero tags certify that every slot completed publication.
        // A miss in a partially published line must still take the normal path.
        let filled = tags | 0xff00_0000_0000_0000;
        if (filled.wrapping_sub(0x0101_0101_0101_0101) & !filled & 0x8080_8080_8080_8080) == 0 {
            return None;
        }
        for (lane, cell) in line.cells.iter().enumerate() {
            loop {
                let word = cell.load(Ordering::Acquire);
                let id = word ^ tag;
                if id < limit {
                    return Some(id as u32);
                }
                if word <= CLOSED {
                    break;
                }
                if word == RESERVED {
                    std::hint::spin_loop();
                    continue;
                }
                if cell
                    .compare_exchange(EMPTY_KEY, RESERVED, Ordering::Acquire, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }
                #[cfg(feature = "large-input")]
                let registered_key = materialize_key(key, mixed, bytes);
                #[cfg(not(feature = "large-input"))]
                let registered_key = key;
                if let Some(id) = users.register_bounded(registered_key, mixed as usize, limit) {
                    cell.store(tag | u64::from(id), Ordering::Release);
                    line.publish(lane, tag);
                    return Some(id);
                }
                cell.store(CLOSED, Ordering::Release);
                line.publish(lane, tag);
                #[cfg(not(feature = "large-input"))]
                {
                    return Some(users.intern(text(bytes)));
                }
                #[cfg(feature = "large-input")]
                {
                    return Some(intern_on_overflow(users, key, mixed, bytes));
                }
            }
        }
        None
    }

    #[inline]
    fn lookup(
        &self,
        mixed: u64,
        key: u64,
        bytes: &[u8],
        spill: &AHashMap<u64, u32>,
        users: &Names,
    ) -> u32 {
        let home = self.home(mixed);
        let quotient = mixed >> self.address_bits;
        let tag = quotient << (self.address_bits + 7);
        if let Some(id) = self.lookup_line(&self.lines[home], tag, key, mixed, bytes, users) {
            return id;
        }
        let other = self.alternate(home, quotient);
        let tag = tag | (self.id_mask + 1);
        let result = self.lookup_line(&self.lines[other], tag, key, mixed, bytes, users);
        #[cfg(not(feature = "large-input"))]
        {
            result
                .or_else(|| spill.get(&key).copied())
                .unwrap_or_else(|| users.intern(text(bytes)))
        }
        #[cfg(feature = "large-input")]
        {
            result
                .or_else(|| spill.get(&materialize_key(key, mixed, bytes)).copied())
                .unwrap_or_else(|| intern_on_overflow(users, key, mixed, bytes))
        }
    }
}

impl LinkGraph {
    fn new(edges: usize, external_names: Vec<Vec<Arc<str>>>) -> Self {
        // There are at most two distinct endpoints per edge. Leave a free slot
        // for the terminating search, even when every endpoint is distinct.
        let capacity = (2 * edges + 1).max(1024).next_power_of_two();
        Self::with_capacity(capacity, external_names)
    }

    fn with_capacity(capacity: usize, external_names: Vec<Vec<Arc<str>>>) -> Self {
        assert!(capacity <= u32::MAX as usize, "too many linked names");
        let entries = (0..capacity)
            .into_par_iter()
            .map(|index| Entry {
                key: AtomicU64::new(EMPTY_KEY),
                parent: AtomicU32::new(index as u32),
            })
            .collect();
        Self {
            entries,
            external_names,
        }
    }

    fn slot_for_link(&self, key: u64, hash: usize) -> u32 {
        self.find_slot::<false>(key, hash).expect("the full graph has room for all endpoints")
    }

    fn find_slot<const BOUNDED: bool>(&self, key: u64, hash: usize) -> Option<u32> {
        let mask = self.entries.len() - 1;
        let mut index = hash & mask;
        let mut probes = 0;
        loop {
            if BOUNDED {
                probes += 1;
                if probes > 64 { return None; }
            }
            let slot = &self.entries[index];
            let stored = slot.key.load(Ordering::Acquire);
            if stored == key {
                return Some(index as u32);
            }
            if stored == EMPTY_KEY {
                match slot
                    .key
                    .compare_exchange(EMPTY_KEY, key, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => return Some(index as u32),
                    Err(raced) if raced == key => return Some(index as u32),
                    Err(_) => {}
                }
            }
            index = (index + 1) & mask;
        }
    }

    fn connect<const BOUNDED: bool>(
        &self, batches: &[Vec<(u64, u64)>], hash: &RandomState, prefetch: bool, prefetched: &AtomicU64,
    ) -> bool {
        let edge = |l, lh, r, rh| -> Option<()> {
            self.join(self.find_slot::<BOUNDED>(l, lh)?, self.find_slot::<BOUNDED>(r, rh)?);
            Some(())
        };
        let profile = crate::csv::work_enabled();
        batches.par_iter().all(|edges| edges.par_chunks(4096).all(|part| {
            let mut local = 0;
            let complete = (|| {
            if !prefetch {
                return part.iter().all(|&(l,r)| {
                    edge(l, hash.hash_one(l) as usize, r, hash.hash_one(r) as usize).is_some()
                });
            }
            for group in part.chunks(32) {
                let mut short = [(0, 0, 0, 0); 32];
                let mut n = 0;
                for &(l,r) in group {
                    let lh = hash.hash_one(l) as usize;
                    let rh = hash.hash_one(r) as usize;
                    if l >= EXTERNAL_NAME || r >= EXTERNAL_NAME {
                        if edge(l, lh, r, rh).is_none() { return false; }
                    } else {
                        if profile { local += 2; }
                        #[cfg(target_arch = "x86_64")]
                        for h in [lh,rh] {
                            let address = &self.entries[h & (self.entries.len()-1)];
                            unsafe { std::arch::x86_64::_mm_prefetch::<{std::arch::x86_64::_MM_HINT_T0}>(
                                (address as *const Entry).cast()); }
                        }
                        short[n] = (l,lh,r,rh);
                        n += 1;
                    }
                }
                for &(l,lh,r,rh) in &short[..n] {
                    if edge(l, lh, r, rh).is_none() { return false; }
                }
            }
            true
            })();
            if profile { prefetched.fetch_add(local, Ordering::Relaxed); }
            complete
        }))
    }

    fn root(&self, mut id: u32) -> u32 {
        loop {
            let entry = &self.entries[id as usize];
            let parent = entry.parent.load(Ordering::Acquire);
            if parent == id {
                return id;
            }
            let grandparent = self.entries[parent as usize].parent.load(Ordering::Acquire);
            if grandparent != parent {
                let _ = entry.parent.compare_exchange(
                    parent,
                    grandparent,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            id = parent;
        }
    }

    fn join(&self, mut left: u32, mut right: u32) {
        loop {
            left = self.root(left);
            right = self.root(right);
            if left == right {
                break;
            }
            let a = self.entries[left as usize].key.load(Ordering::Relaxed);
            let b = self.entries[right as usize].key.load(Ordering::Relaxed);
            let left_first = if a < EXTERNAL_NAME && b < EXTERNAL_NAME {
                a < b
            } else {
                linked_spelling(&self.external_names, a, &mut [0; 9])
                    < linked_spelling(&self.external_names, b, &mut [0; 9])
            };
            let (lower, higher) = if left_first {
                (left, right)
            } else {
                (right, left)
            };
            // Edges always point toward a smaller spelling. This prevents cycles
            // even when another worker concurrently changes the lower root.
            if self.entries[higher as usize]
                .parent
                .compare_exchange(higher, lower, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    fn finish(self, external: &mut [AHashMap<Arc<str>, u32>], hash: &RandomState) -> NameTable {
        // Flatten all physical parents before reading any as final roots.
        let roots: Vec<_> = self
            .entries
            .par_iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                let key = entry.key.load(Ordering::Relaxed);
                if key == EMPTY_KEY {
                    return None;
                }
                let root = self.root(index as u32);
                entry.parent.store(root, Ordering::Relaxed);
                (root as usize == index).then_some((index, key))
            })
            .collect();
        let mut dense = vec![0u32; self.entries.len()];
        let canonical: Vec<_> = roots
            .into_iter()
            .enumerate()
            .map(|(id, (slot, key))| {
                dense[slot] = id as u32;
                key
            })
            .collect();

        // Each root owns at least one occupied slot. The directory reserves
        // more than eight IDs per graph slot, so all linked IDs fit.
        let directory = Directory::new(self.entries.len());
        assert!(canonical.len() as u64 <= directory.id_mask - 2);
        let spilled: Vec<_> = self
            .entries
            .par_iter()
            .filter_map(|entry| {
                let key = entry.key.load(Ordering::Relaxed);
                if key >= EXTERNAL_NAME {
                    return None;
                }
                let id = dense[entry.parent.load(Ordering::Relaxed) as usize];
                if mixed_word(key).is_some_and(|mixed| directory.fill(mixed, id)) {
                    None
                } else {
                    Some((key, id))
                }
            })
            .collect();
        external.par_iter_mut().for_each(|names| {
            for id in names.values_mut() {
                let key = EXTERNAL_NAME | u64::from(*id);
                let slot = self.slot_for_link(key, hash.hash_one(key) as usize);
                let root = self.entries[slot as usize].parent.load(Ordering::Relaxed);
                *id = dense[root as usize];
            }
        });
        NameTable {
            directory,
            canonical,
            external_names: self.external_names,
            overflow: spilled.into_iter().collect(),
        }
    }
}

// For short u/U-prefixed names, store the suffix in big-endian byte order.
// The high bit distinguishes the prefixes. Padding is unambiguous because
// nonempty suffixes must end in a nonzero byte. Numeric order is byte order.
fn packed_name(name: &[u8]) -> Option<u64> {
    let (&prefix, suffix) = name.split_first()?;
    let prefix_bit = match prefix {
        b'u' => 1 << 63,
        b'U' => 0,
        _ => return None,
    };
    if suffix.len() > 8 || suffix.first().is_some_and(|b| *b >= 128) || suffix.last() == Some(&0) {
        return None;
    }
    let word = if let Ok(full) = <&[u8; 8]>::try_from(suffix) {
        u64::from_be_bytes(*full)
    } else {
        let mut padded = [0u8; 8];
        padded[..suffix.len()].copy_from_slice(suffix);
        u64::from_be_bytes(padded)
    };
    Some(prefix_bit | word)
}

fn unpack_name(key: u64, buffer: &mut [u8; 9]) -> &str {
    buffer[0] = if key >> 63 == 0 { b'U' } else { b'u' };
    buffer[1..].copy_from_slice(&(key & !(1 << 63)).to_be_bytes());
    let length = 1 + buffer.iter().rposition(|&byte| byte != 0).unwrap();
    text(&buffer[..length])
}

pub fn read_aliases(data: &Path, users: &mut Names) -> Result<Aliases> {
    read_aliases_with_wide(data, users, perf_on("PERF_USER_HEX"))
}

fn read_aliases_with_wide(data: &Path, users: &mut Names, wide_users: bool) -> Result<Aliases> {
    let external = Names {
        hash: users.hash.clone(),
        wide_users,
        ..Names::default()
    };
    let key_for = |bytes: &[u8]| {
        packed_name(bytes)
            .unwrap_or_else(|| EXTERNAL_NAME | u64::from(external.intern(text(bytes))))
    };
    let batches = chunks(&data.join("links"))?
        .into_par_iter()
        .map(|chunk| {
            let mut edges = Vec::new();
            chunk.scan(|row| {
                if let Some([left, right]) = fields(row) {
                    if !left.is_empty() && !right.is_empty() && left != right {
                        edges.push((key_for(left), key_for(right)));
                    }
                }
            })?;
            Ok(edges)
        })
        .collect::<Result<Vec<_>>>()?;

    let edge_count = batches.iter().map(Vec::len).sum::<usize>();
    let (mut external, external_names): (Vec<_>, Vec<_>) = external
        .shards
        .into_iter()
        .map(|shard| {
            let state = shard.into_inner().unwrap();
            (state.ids, state.names)
        })
        .unzip();
    let prefetch = perf_on("PERF_LINK_PREFETCH");
    let compact = perf_on("PERF_LINK_COMPACT");
    let prefetched = AtomicU64::new(0);
    let mut retried = false;
    let graph = if compact {
        let capacity = (edge_count + edge_count / 4 + 1).max(1024).next_power_of_two();
        let graph = LinkGraph::with_capacity(capacity, external_names);
        if graph.connect::<true>(&batches, &users.hash, prefetch, &prefetched) {
            graph
        } else {
            retried = true;
            let names = graph.external_names;
            drop(graph.entries);
            let full = LinkGraph::new(edge_count, names);
            assert!(full.connect::<false>(&batches, &users.hash, prefetch, &prefetched));
            full
        }
    } else {
        let graph = LinkGraph::new(edge_count, external_names);
        assert!(graph.connect::<false>(&batches, &users.hash, prefetch, &prefetched));
        graph
    };
    if crate::csv::work_enabled() {
        let mut sizes: Vec<_> = batches.iter().map(Vec::len).collect();
        sizes.sort_unstable();
        let short_pairs: usize = batches.iter().map(|b| b.iter().filter(|&&(l,r)| l<EXTERNAL_NAME && r<EXTERNAL_NAME).count()).sum();
        eprintln!("profile {}", serde_json::json!({
            "stage":"work_link_graph", "edges":edge_count, "short_pairs":short_pairs,
            "max_task_edges":sizes.last().copied().unwrap_or(0),
            "median_task_edges":sizes.get(sizes.len()/2).copied().unwrap_or(0),
            "requested_prefetches":prefetched.load(Ordering::Relaxed),
            "capacity_retries":usize::from(retried), "final_slots":graph.entries.len(),
        }));
    }
    drop(batches);
    let table = graph.finish(&mut external, &users.hash);
    let wide = wide_users.then(|| external.par_iter_mut().map(|names| {
        let mut packed = AHashMap::default();
        names.retain(|spelling,id| {
            if let Some(key) = hex_id(spelling.as_bytes(), b'u') {
                packed.insert(key,*id);
                false
            } else { true }
        });
        packed
    }).collect());
    let dynamic_start = table.canonical.len().div_ceil(SHARDS);
    for shard in &mut users.shards {
        shard.get_mut().unwrap().offset = dynamic_start;
    }
    let table = Arc::new(table);
    users.packed = Some(Arc::clone(&table));
    users.wide_users = wide_users;
    Ok(Aliases { table, external, wide })
}

#[derive(Clone, Copy)]
struct CachedPerson {
    key: u64,
    person: u32,
}

impl Default for CachedPerson {
    fn default() -> Self {
        Self {
            key: EMPTY_KEY,
            person: 0,
        }
    }
}

pub struct PersonCache(Vec<CachedPerson>);

impl Default for PersonCache {
    fn default() -> Self {
        Self(vec![CachedPerson::default(); 4096])
    }
}

#[cfg(not(feature = "large-input"))]
#[derive(Clone, Copy)]
pub enum Prepared {
    Ready(u32),
    Compact(u64, u64),
    Spill(u64),
    External,
}

// Ordinary compact requests contain only the bijective 57-bit spelling hash.
#[cfg(feature = "large-input")]
#[derive(Clone, Copy)]
pub struct Prepared(u64);
#[cfg(feature = "large-input")]
const READY: u64 = 1 << 63;
#[cfg(feature = "large-input")]
const SPILL: u64 = u64::MAX - 1;
#[cfg(feature = "large-input")]
const EXTERNAL: u64 = u64::MAX;

impl Prepared {
    #[cfg(not(feature = "large-input"))]
    pub(crate) fn work_class(self) -> usize {
        match self { Self::Ready(_) => 0, Self::Compact(..) => 1, Self::Spill(_) => 2, Self::External => 3 }
    }

    #[cfg(feature = "large-input")]
    pub(crate) fn work_class(self) -> usize {
        match self.0 { SPILL => 2, EXTERNAL => 3, word if word & READY != 0 => 0, _ => 1 }
    }
}

impl PersonCache {
    #[inline]
    pub fn prepare(&self, _users: &Names, aliases: &Aliases, bytes: &[u8]) -> Prepared {
        let Some(key) = packed_name(bytes) else {
            #[cfg(not(feature = "large-input"))]
            {
                return Prepared::External;
            }
            #[cfg(feature = "large-input")]
            {
                return Prepared(EXTERNAL);
            }
        };
        let Some(mixed) = mixed_word(key) else {
            #[cfg(not(feature = "large-input"))]
            {
                return Prepared::Spill(key);
            }
            #[cfg(feature = "large-input")]
            {
                return Prepared(SPILL);
            }
        };
        let entry = &self.0[mixed as usize & (self.0.len() - 1)];
        #[cfg(not(feature = "large-input"))]
        {
            if entry.key == key {
                return Prepared::Ready(entry.person);
            }
        }
        #[cfg(feature = "large-input")]
        {
            if entry.key == mixed {
                return Prepared(READY | u64::from(entry.person));
            }
        }
        let directory = &aliases.table.directory;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                (&directory.lines[directory.home(mixed)] as *const Line).cast(),
            );
        }
        #[cfg(not(feature = "large-input"))]
        {
            Prepared::Compact(key, mixed)
        }
        #[cfg(feature = "large-input")]
        {
            Prepared(mixed)
        }
    }

    #[inline]
    pub fn prepared(
        &mut self,
        job: Prepared,
        users: &Names,
        aliases: &Aliases,
        bytes: &[u8],
    ) -> u32 {
        let table = &aliases.table;
        #[cfg(not(feature = "large-input"))]
        {
            match job {
                Prepared::Ready(id) => id,
                Prepared::Compact(key, mixed) => {
                    let id = table
                        .directory
                        .lookup(mixed, key, bytes, &table.overflow, users);
                    let slot = mixed as usize & (self.0.len() - 1);
                    self.0[slot] = CachedPerson { key, person: id };
                    id
                }
                Prepared::Spill(key) => table
                    .overflow
                    .get(&key)
                    .copied()
                    .unwrap_or_else(|| users.intern(text(bytes))),
                Prepared::External => aliases.resolve_external(users, bytes),
            }
        }
        #[cfg(feature = "large-input")]
        {
            let word = job.0;
            if word < READY {
                let id = table
                    .directory
                    .lookup(word, EMPTY_KEY, bytes, &table.overflow, users);
                let slot = word as usize & (self.0.len() - 1);
                self.0[slot] = CachedPerson {
                    key: word,
                    person: id,
                };
                return id;
            }
            if word < SPILL {
                return word as u32;
            }
            if word == SPILL {
                let key = packed_name(bytes).expect("spill kept original spelling");
                return table
                    .overflow
                    .get(&key)
                    .copied()
                    .unwrap_or_else(|| users.intern(text(bytes)));
            }
            aliases.resolve_external(users, bytes)
        }
    }

    #[cfg(test)]
    fn resolve(&mut self, users: &Names, aliases: &Aliases, bytes: &[u8]) -> u32 {
        self.prepared(self.prepare(users, aliases, bytes), users, aliases, bytes)
    }
}

#[cfg(test)]
mod person_tests {
    use super::*;

    #[test]
    fn wide_people_preserve_linked_dynamic_and_unusual_spellings() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("shopping-wide-user-{}",std::process::id()));
        let links = dir.join("links");
        std::fs::create_dir_all(&links)?;
        let paired = "u0000000000000000000000000000000f";
        let other = "u0000000000000001000000000000000f";
        let different_case = "u0000000000000000000000000000000F";
        let path = links.join("part.csv");
        std::fs::write(&path,format!("user_id_a,user_id_b\n{paired},U7\nU7,ordinary-alias\n"))?;
        let mut users = Names::default();
        let aliases = read_aliases_with_wide(&dir, &mut users, true)?;
        let mut cache = PersonCache::default();
        let owner = cache.resolve(&users,&aliases,paired.as_bytes());
        assert_eq!(owner, cache.resolve(&users,&aliases,b"ordinary-alias"));
        assert_eq!(owner, cache.resolve(&users,&aliases,b"U7"));
        let new = cache.resolve(&users,&aliases,other.as_bytes());
        let mixed = cache.resolve(&users,&aliases,different_case.as_bytes());
        assert_ne!(new,owner);
        assert_ne!(new,mixed);
        assert_ne!(owner,mixed);
        assert_eq!(new, cache.resolve(&users,&aliases,other.as_bytes()));
        let names = users.into_people();
        assert_eq!(names.get(owner,&mut [0;9]),"U7");
        assert_eq!(names.get(new,&mut [0;9]),other);
        assert_eq!(names.get(mixed,&mut [0;9]),different_case);
        std::fs::remove_file(path)?;
        std::fs::remove_dir(links)?;
        std::fs::remove_dir(dir)?;
        Ok(())
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn hex_vectors_and_batch_dates_match_scalar_even_on_bad_bytes() {
        if !std::is_x86_feature_detected!("avx2") { return; }
        let word = *b"0123456789abcdeffedcba9876543210";
        for i in 0..32 {
            for b in 0..=255 {
                let mut test = word;
                test[i] = b;
                assert_eq!(unsafe { hex_avx2(&test) }, hex_scalar(&test));
            }
        }
        let cal = Calendar::new(Window{start:0,end:DAY});
        let valid: &[u8] = b"2028-02-29T00:00:00Z";
        let offset = *b"2028-02-29T23:59:59-23:59";
        for i in 0..offset.len() {
            for b in 0..=255 {
                let mut test=offset;
                test[i]=b;
                let rows=[[valid],[&test],[b"0000000001234".as_slice()],[b"123".as_slice()],[valid],[&test]];
                let mut values=[None;6];
                cal.times(&rows,0,&mut values);
                assert_eq!(values,rows.map(|row|cal.parse(row[0])));
            }
        }
    }

    #[test]
    fn bounded_link_graph_retries_without_losing_components() {
        let hash = RandomState::new();
        let batches = vec![(0u64..300).map(|n| (n * 2, n * 2 + 1)).collect::<Vec<_>>()];
        let small = LinkGraph::with_capacity(16, Vec::new());
        let counted = AtomicU64::new(0);
        assert!(!small.connect::<true>(&batches, &hash, true, &counted));
        let full = LinkGraph::new(batches[0].len(), Vec::new());
        assert!(full.connect::<false>(&batches, &hash, true, &counted));
        for &(a,b) in &batches[0] {
            let a_slot = full.slot_for_link(a, hash.hash_one(a) as usize);
            let b_slot = full.slot_for_link(b, hash.hash_one(b) as usize);
            assert_eq!(full.root(a_slot), full.root(b_slot));
            assert_eq!(full.entries[full.root(a_slot) as usize].key.load(Ordering::Relaxed), a);
        }
    }

    #[cfg(feature = "large-input")]
    #[test]
    fn deferred_query_recovers_both_prefixes_and_zero_padding() {
        for prefix in ['u', 'U'] {
            for n in 0..65536 {
                let name = format!("{prefix}{n:x}");
                let raw = packed_name(name.as_bytes()).unwrap();
                assert_eq!(
                    materialize_key(EMPTY_KEY, mixed_word(raw).unwrap(), &[]),
                    raw
                );
            }
            let mut bytes = [127u8; 9];
            bytes[0] = prefix as u8;
            for i in 1..8 {
                bytes[i] = 0;
                let raw = packed_name(&bytes).unwrap();
                assert_eq!(
                    materialize_key(EMPTY_KEY, mixed_word(raw).unwrap(), &[]),
                    raw
                );
            }
        }
    }
    #[test]
    fn directory_quotient_and_side_encode_the_complete_key() {
        let dir = Directory::new(1 << 16);
        for i in 0..10000 {
            let name = format!("u{i:07x}");
            let key = packed_name(name.as_bytes()).unwrap();
            let bytes = (key & !(1 << 63)).to_be_bytes();
            let reference = bytes
                .into_iter()
                .fold(0u64, |value, b| (value << 7) | u64::from(b))
                | ((key >> 63) << 56);
            assert_eq!(ascii_code(key), Some(reference));
            let mixed = mixed_word(key).unwrap();
            let locations = dir.locations(mixed);
            assert_ne!(locations[0].0, locations[1].0);
            assert_ne!(locations[0].1, locations[1].1);
            assert_eq!(
                (locations[0].1 >> (dir.address_bits + 7)) << dir.address_bits
                    | locations[0].0 as u64,
                mixed
            );
        }
    }

    #[test]
    fn local_cache_keeps_linked_and_unlinked_ids_exact_on_collision() {
        let mut users = Names::default();
        let table = NameTable {
            directory: Directory::new(1024),
            canonical: vec![EXTERNAL_NAME],
            external_names: vec![vec![Arc::from("canonical")]],
            overflow: AHashMap::default(),
        };
        for shard in &mut users.shards {
            shard.get_mut().unwrap().offset = 1;
        }
        let canonical = 0;
        let key = packed_name(b"ualias").unwrap();
        assert!(table.directory.fill(mixed_word(key).unwrap(), canonical));
        let table = Arc::new(table);
        users.packed = Some(Arc::clone(&table));
        let aliases = Aliases {
            wide: None,
            table,
            external: (0..SHARDS).map(|_| AHashMap::default()).collect(),
        };
        let mut cache = PersonCache(vec![CachedPerson::default(); 1]);
        let spellings = ["ux", "uy", "ux", "0123456789abcdefx", "0123456789abcdefy"];
        let ids = spellings.map(|name| {
            assert_eq!(cache.resolve(&users, &aliases, b"ualias"), canonical);
            cache.resolve(&users, &aliases, name.as_bytes())
        });
        assert_eq!(ids[0], ids[2]);
        let names = users.into_people();
        for (key, id) in spellings.into_iter().zip(ids) {
            assert_eq!(names.get(id, &mut [0; 9]), key);
        }
    }

    #[test]
    fn packed_spelling_preserves_bytes_and_lexicographic_order() {
        let ordered = [
            "U",
            "U0",
            "Uabc",
            "u",
            "u\0x",
            "u1",
            "u1\0x",
            "u2",
            "uabcdefgh",
        ];
        let keys = ordered.map(|s| packed_name(s.as_bytes()).unwrap());
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        for (name, key) in ordered.into_iter().zip(keys) {
            assert_eq!(unpack_name(key, &mut [0; 9]), name);
        }
        for fallback in ["z", "uuuuuuuuuu", "u\0", "ué"] {
            assert!(packed_name(fallback.as_bytes()).is_none());
        }
    }

    #[test]
    fn mixed_link_keys_choose_the_true_minimum_and_preserve_unlinked_names() -> Result<()> {
        use std::{
            fs,
            time::{SystemTime, UNIX_EPOCH},
        };
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let directory =
            std::env::temp_dir().join(format!("shopping-names-{}-{nonce}", std::process::id()));
        let links = directory.join("links");
        fs::create_dir_all(&links)?;
        let part = links.join("part.csv");
        fs::write(&part, b"left,right\nu3,ua\nlong-name-at-least-ten,ua\nlong-name-at-least-ten,U7\nu9,u\0\nu\0, a\nu4,u4\n")?;
        let mut users = Names::default();
        let aliases = read_aliases(&directory, &mut users)?;
        let mut cache = PersonCache::default();
        let cases = [
            ("u3", "U7"),
            ("ua", "U7"),
            ("long-name-at-least-ten", "U7"),
            ("U7", "U7"),
            ("u9", " a"),
            ("u\0", " a"),
            ("u4", "u4"),
            ("u12345678", "u12345678"),
            ("unlinked and long", "unlinked and long"),
        ];
        let resolved = cases.map(|(key, _)| cache.resolve(&users, &aliases, key.as_bytes()));
        let names = users.into_people();
        for ((_, expected), id) in cases.into_iter().zip(resolved) {
            assert_eq!(names.get(id, &mut [0; 9]), expected);
        }
        fs::remove_file(part)?;
        fs::remove_dir(links)?;
        fs::remove_dir(directory)?;
        Ok(())
    }

    #[test]
    fn parallel_unions_choose_minimum_on_overlapping_chains() {
        let n = 4096;
        let hash = RandomState::new();
        let keys: Vec<_> = (0..n)
            .map(|i| packed_name(format!("u{i:04}").as_bytes()).unwrap())
            .collect();
        let table = LinkGraph::new(n, Vec::new());
        (1..n).into_par_iter().for_each(|i| {
            for (left, right) in [(i, i - 1), (n - 1 - i, n - i)] {
                let a = table.slot_for_link(keys[left], hash.hash_one(keys[left]) as usize);
                let b = table.slot_for_link(keys[right], hash.hash_one(keys[right]) as usize);
                table.join(a, b);
            }
        });
        let table = table.finish(&mut [], &hash);
        let users = Names::default();
        assert_eq!(table.canonical, [keys[0]]);
        for key in keys {
            let mixed = mixed_word(key).unwrap();
            assert_eq!(
                table
                    .directory
                    .lookup(mixed, key, b"", &table.overflow, &users),
                0
            );
        }
    }

    #[test]
    fn concurrent_colliding_unlinked_names_keep_distinct_stable_ids() {
        let table = NameTable {
            directory: Directory::new(1024),
            canonical: Vec::new(),
            external_names: Vec::new(),
            overflow: AHashMap::default(),
        };
        let mut users = Names::default();
        let names: Vec<_> = (0..)
            .map(|i| format!("u{i:06}"))
            .filter(|s| {
                let mixed = mixed_word(packed_name(s.as_bytes()).unwrap()).unwrap();
                table.directory.home(mixed) == 0
            })
            .take(48)
            .collect();
        let results = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..8)
                .map(|offset| {
                    let (table, users, names) = (&table, &users, &names);
                    scope.spawn(move || {
                        let mut ids = vec![0; names.len()];
                        for j in 0..names.len() {
                            let i = (j + offset) % names.len();
                            let bytes = names[i].as_bytes();
                            let key = packed_name(bytes).unwrap();
                            let mixed = mixed_word(key).unwrap();
                            ids[i] =
                                table
                                    .directory
                                    .lookup(mixed, key, bytes, &table.overflow, users);
                        }
                        ids
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(results.iter().all(|ids| *ids == results[0]));
        let distinct: std::collections::HashSet<_> = results[0].iter().collect();
        assert_eq!(distinct.len(), names.len());
        users.packed = Some(Arc::new(table));
        let people = users.into_people();
        for (id, expected) in results[0].iter().zip(&names) {
            assert_eq!(people.get(*id, &mut [0; 9]), expected);
        }
    }
}

#[cfg(test)]
mod calendar_tests {
    use super::*;
    #[test]
    fn fast_dates_and_epochs_match_the_general_parser() {
        let window = Window {
            start: timestamp(b"2028-02-28T00:00:00Z").unwrap(),
            end: timestamp(b"2028-03-05T00:00:00Z").unwrap(),
        };
        let calendar = Calendar::new(window);
        let examples: &[&[u8]] = &[
            b"2028-02-29T23:59:59Z",
            b"2027-02-29T00:00:00Z",
            b"2028-03-01T00:00:00+01:00",
            b"2028-03-02T24:00:00Z",
            b"2028-03-01T00:60:00Z",
            b"1835481600000",
            b"18350a1600000",
            b"0000000000000",
            b"00000000000000012",
        ];
        for &sample in examples {
            assert_eq!(calendar.parse(sample), timestamp(sample));
        }
        for &sample in [
            b"2028-02-29T23:59:59Z",
            b"2028-03-01T00:00:00Z",
            b"1984-01-01T02:03:04Z",
        ]
        .iter()
        {
            assert_eq!(calendar.ingest(sample), ingest_timestamp(sample));
        }
    }
}

#[cfg(test)]
mod large_identity_regression {
    use super::*;
    use std::fmt::Write as _;

    #[test]
    fn linked_and_new_people_round_trip_after_concurrent_lookups() -> Result<()> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("shopping-linked-{}-{nonce}", std::process::id()));
        let links = dir.join("links");
        std::fs::create_dir_all(&links)?;
        let mut csv = String::from("user_id_a,user_id_b\n");
        for i in 0..9000 {
            writeln!(csv, "u{i:06},u{:06}", i + 1)?;
        }
        csv.push_str("u004500,u中\nu中,00外\nuAA,Urr\n");
        let part = links.join("part.csv");
        std::fs::write(&part, csv)?;
        let mut users = Names::default();
        let aliases = read_aliases(&dir, &mut users)?;
        std::fs::remove_file(part)?;
        std::fs::remove_dir(links)?;
        std::fs::remove_dir(dir)?;
        let cases = [
            "u000000", "u009000", "u中", "00外", "uAA", "Urr", "u0", "u00", "u0\0", "uÚ", "uXX",
            "uXX ", "", " U",
        ];
        let expected = [
            "00外", "00外", "00外", "00外", "Urr", "Urr", "u0", "u00", "u0\0", "uÚ", "uXX", "uXX ",
            "", " U",
        ];
        // The event reader rejects blank user IDs before resolution.
        let ids = std::thread::scope(|scope| {
            let jobs = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        let mut cache = PersonCache::default();
                        let mut ids = Vec::new();
                        for _ in 0..8 {
                            let round = cases
                                .iter()
                                .filter(|s| !s.is_empty())
                                .map(|s| cache.resolve(&users, &aliases, s.as_bytes()))
                                .collect::<Vec<_>>();
                            if ids.is_empty() {
                                ids = round;
                            } else {
                                assert_eq!(ids, round);
                            }
                        }
                        ids
                    })
                })
                .collect::<Vec<_>>();
            jobs.into_iter()
                .map(|job| job.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(ids.windows(2).all(|pair| pair[0] == pair[1]));
        drop(aliases);
        let names = users.into_people();
        for (id, expected) in ids[0].iter().zip(expected.iter().filter(|s| !s.is_empty())) {
            assert_eq!(names.get(*id, &mut [0; 9]), *expected);
        }
        Ok(())
    }
}

#[cfg(test)]
mod dimension_key_regression {
    use super::*;
    #[test]
    fn lookup_preserves_noncanonical_and_utf8_keys() -> Result<()> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("shopping-dim-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&dir)?;
        let products = dir.join("products.csv");
        let campaigns = dir.join("campaigns.csv");
        std::fs::write(
            &products,
            "p,category\np1,a\np01,b\nP1,c\np10000000,d\n瓷,e\np0,f\n",
        )?;
        std::fs::write(
            &campaigns,
            "c,channel\nc_1,a\nc_01,b\nc_10000000,c\n活動,d\n",
        )?;
        let dims = Dimensions::read(&dir)?;
        std::fs::remove_file(products)?;
        std::fs::remove_file(campaigns)?;
        std::fs::remove_dir(dir)?;
        for (key, value) in [
            ("p1", "a"),
            ("p01", "b"),
            ("P1", "c"),
            ("p10000000", "d"),
            ("瓷", "e"),
            ("p0", "f"),
        ] {
            let id = dims.products.lookup(key.as_bytes()).unwrap();
            assert_eq!(dims.categories[id as usize], value);
        }
        for (key, value) in [
            ("c_1", "a"),
            ("c_01", "b"),
            ("c_10000000", "c"),
            ("活動", "d"),
        ] {
            let id = dims.campaign_ids.lookup(key.as_bytes()).unwrap();
            assert_eq!(dims.campaigns[id as usize].1, value);
        }
        assert!(dims.products.lookup(b"p2").is_none());
        assert!(dims.campaign_ids.lookup(b"c_0").is_none());
        Ok(())
    }
}

#[cfg(test)]
mod tagged_line_regression {
    use super::*;

    #[test]
    fn equal_fingerprints_never_alias_distinct_quotients() {
        let directory = Directory::new(1024);
        let home = 19_u64;
        let mut mixed = Vec::new();
        // These distinct keys share the home bucket and top tag byte.
        for q in 1..=11u64 {
            let key = (q << directory.address_bits) | home;
            assert!(directory.fill(key, q as u32));
            mixed.push(key);
        }
        let users = Names::default();
        let spill = AHashMap::default();
        for (i, &key) in mixed.iter().enumerate() {
            assert_eq!(
                directory.lookup(key, key, b"u_unused", &spill, &users),
                i as u32 + 1
            );
        }
        assert_eq!(
            directory.lines[home as usize].tags.load(Ordering::Acquire) & 0x00ff_ffff_ffff_ffff,
            0x0001_0101_0101_0101
        );
    }

    #[test]
    fn live_partial_and_fully_published_lines_return_one_person_per_spelling() {
        let directory = Directory::new(1024);
        let users = Names::default();
        let spill = AHashMap::default();
        let results = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        let mut found = [0u32; 11];
                        for (i, slot) in found.iter_mut().enumerate() {
                            let key = ((i as u64 + 1) << directory.address_bits) | 13;
                            let name = format!("u{i}");
                            *slot = directory.lookup(key, key, name.as_bytes(), &spill, &users);
                        }
                        found
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|t| t.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(results.windows(2).all(|p| p[0] == p[1]));
        let unique: std::collections::HashSet<_> = results[0].into_iter().collect();
        assert_eq!(unique.len(), 11);
    }
}
