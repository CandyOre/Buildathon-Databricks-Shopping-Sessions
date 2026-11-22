//! Optional paired decoding for the event timestamp and its valid F1 ingest.
//! Each 64-bit lane holds an independent HH:MM:SS. Dates are reused only
//! after comparing all ten date bytes, including punctuation.

pub(super) fn available() -> bool {
    #[cfg(target_arch = "x86_64")]
    { std::is_x86_feature_detected!("avx2") }
    #[cfg(not(target_arch = "x86_64"))]
    { false }
}

#[cfg(target_arch = "x86_64")]
pub(super) use x86::{pair, single};

#[cfg(target_arch = "x86_64")]
mod x86 {
    use super::super::{Calendar, zone_offset};
    use std::arch::x86_64::*;

    fn iso(s: &[u8]) -> bool {
        (s.len() == 20 && s[19] == b'Z' || s.len() == 25 && matches!(s[19], b'+' | b'-'))
            && s[10] == b'T' && s[13] == b':' && s[16] == b':'
    }

    #[inline]
    fn clock_word(s: &[u8]) -> i64 {
        i64::from_le_bytes(s[11..19].try_into().unwrap())
    }

    #[target_feature(enable = "ssse3,sse4.1")]
    unsafe fn clocks(left: i64, right: i64) -> Option<(i64, i64)> {
        unsafe {
            let ascii = _mm_set_epi64x(right, left);
            let digits = _mm_shuffle_epi8(
                _mm_sub_epi8(ascii, _mm_set1_epi8(b'0' as i8)),
                _mm_setr_epi8(0,1,3,4,6,7,-1,-1,8,9,11,12,14,15,-1,-1),
            );
            let bad_digit = _mm_subs_epu8(digits, _mm_set1_epi8(9));
            let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
            let bad_range = _mm_subs_epu16(pairs, _mm_setr_epi16(23,59,59,0,23,59,59,0));
            let bad = _mm_or_si128(bad_digit,bad_range);
            if _mm_testz_si128(bad, bad) == 0 { return None; }
            let partial = _mm_madd_epi16(pairs, _mm_setr_epi16(3600,60,1,0,3600,60,1,0));
            let seconds = _mm_hadd_epi32(partial,partial);
            Some((i64::from(_mm_cvtsi128_si32(seconds)), i64::from(_mm_extract_epi32::<1>(seconds))))
        }
    }

    #[target_feature(enable = "ssse3,sse4.1")]
    unsafe fn epoch(s: &[u8; 13]) -> Option<i64> {
        unsafe {
            // Two legal overlapping loads supply 5 leading and 8 trailing
            // digits. Shuffle after subtracting ASCII zero to supply three
            // numeric leading zeroes without reading outside the field.
            let head = i64::from_le_bytes(s[..8].try_into().unwrap());
            let tail = i64::from_le_bytes(s[5..].try_into().unwrap());
            let ascii = _mm_set_epi64x(tail,head);
            let digits = _mm_shuffle_epi8(
                _mm_sub_epi8(ascii,_mm_set1_epi8(b'0' as i8)),
                _mm_setr_epi8(-1,-1,-1,0,1,2,3,4,8,9,10,11,12,13,14,15),
            );
            let bad = _mm_subs_epu8(digits,_mm_set1_epi8(9));
            if _mm_testz_si128(bad,bad) == 0 { return None; }
            let pairs = _mm_maddubs_epi16(digits,_mm_set1_epi16(0x010a));
            let quads = _mm_madd_epi16(pairs,_mm_set1_epi32(0x0001_0064));
            let octets = _mm_madd_epi16(_mm_packs_epi32(quads,quads),_mm_set1_epi32(0x0001_2710));
            Some(i64::from(_mm_cvtsi128_si32(octets))*100_000_000 + i64::from(_mm_extract_epi32::<1>(octets)))
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn epoch_and_ingest(s: &[u8;13], ingest: &[u8]) -> Option<(i64, i64)> {
        unsafe {
            let head = i64::from_le_bytes(s[..8].try_into().unwrap());
            let tail = i64::from_le_bytes(s[5..].try_into().unwrap());
            let ascii = _mm256_set_epi64x(0,clock_word(ingest),tail,head);
            let digits = _mm256_shuffle_epi8(
                _mm256_sub_epi8(ascii,_mm256_set1_epi8(b'0' as i8)),
                _mm256_setr_epi8(-1,-1,-1,0,1,2,3,4,8,9,10,11,12,13,14,15,
                                 0,1,3,4,6,7,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1),
            );
            let bad = _mm256_subs_epu8(digits,_mm256_set1_epi8(9));
            if _mm256_testz_si256(bad,bad) == 0 { return None; }
            let pairs = _mm256_maddubs_epi16(digits,_mm256_set1_epi16(0x010a));
            let quads = _mm256_madd_epi16(pairs,
                _mm256_setr_epi16(100,1,100,1,100,1,100,1,3600,60,1,0,0,0,0,0));
            let epoch_quads = _mm256_castsi256_si128(quads);
            let octets = _mm_madd_epi16(_mm_packs_epi32(epoch_quads,epoch_quads),
                                         _mm_set1_epi32(0x0001_2710));
            let elapsed = _mm256_extracti128_si256::<1>(quads);
            let second = _mm_cvtsi128_si32(_mm_hadd_epi32(elapsed,elapsed));
            let timestamp = i64::from(_mm_cvtsi128_si32(octets))*100_000_000
                + i64::from(_mm_extract_epi32::<1>(octets));
            Some((timestamp,i64::from(second)))
        }
    }

    #[target_feature(enable = "ssse3,sse4.1")]
    pub(crate) unsafe fn single(cal: &Calendar, s: &[u8]) -> Option<i64> {
        if let Ok(digits) = <&[u8;13]>::try_from(s) {
            return unsafe { epoch(digits) };
        }
        if !iso(s) { return cal.parse_reference(s); }
        let (seconds, _) = unsafe { clocks(clock_word(s), 0x3030_3a30_303a_3030) }?;
        Some(cal.date(s)? + seconds*1000 - zone_offset(s)?)
    }

    #[target_feature(enable = "avx2")]
    pub(crate) unsafe fn pair(cal: &Calendar, event: &[u8], ingest: &[u8]) -> (Option<i64>, i64) {
        if let Ok(digits) = <&[u8;13]>::try_from(event) {
            if ingest.len() == 20 {
                if let Some((time, sec)) = unsafe { epoch_and_ingest(digits, ingest) } {
                    let day = cal.date(ingest).expect("ingest_ts has a valid date");
                    return (Some(time), day+sec*1000);
                }
            }
            return (cal.parse_reference(event),cal.ingest(ingest));
        }
        if !iso(event) || ingest.len() != 20 {
            return (unsafe { single(cal,event) }, cal.ingest(ingest));
        }
        // The reference parses ingest under its guaranteed F1 contract. If
        // the SIMD check finds a noncanonical clock in either lane, delegate
        // both fields so the optional path has identical behaviour.
        let Some((event_sec,ingest_sec)) = (unsafe { clocks(clock_word(event),clock_word(ingest)) })
            else { return (cal.parse_reference(event),cal.ingest(ingest)); };
        let day = cal.date(ingest).expect("ingest_ts has a valid date");
        let event_day = if event[..10] == ingest[..10] { Some(day) } else { cal.date(event) };
        let time = event_day.zip(zone_offset(event)).map(|(day,offset)| day+event_sec*1000-offset);
        (time, day+ingest_sec*1000)
    }
}

#[cfg(all(test,target_arch = "x86_64"))]
mod tests {
    use super::*;
    use super::super::{Calendar, Window, timestamp};

    #[test]
    fn fused_timestamps_match_each_reference_with_corrupted_fields() {
        if !available() { return; }
        let base = timestamp(b"2028-02-27T00:00:00Z").unwrap();
        let cal = Calendar::new(Window { start:base, end:base+8*86_400_000 });
        let inputs: &[&[u8]] = &[
            b"2028-02-29T23:59:59Z", b"2028-03-01T00:00:00+01:30",
            b"1970-01-01T22:31:33-23:59", b"0000000000012", b"1847399999999",
            b"000000000000000001", b"9223372036854775807", b"", b"bad",
        ];
        for &ingest in &[&b"2028-02-29T12:34:56Z"[..],b"2028-03-01T00:02:00Z"] {
            for &input in inputs {
                let check = |s:&[u8]| {
                    let reference = cal.parse_reference(s);
                    assert_eq!(unsafe { single(&cal,s) },reference);
                    assert_eq!(unsafe { pair(&cal,s,ingest) },(reference,cal.ingest(ingest)));
                };
                check(input);
                for pos in 0..input.len() {
                    let mut text = input.to_vec();
                    for byte in [0,1,b'0',b'9',b'Z',b'+',b':',b'/',0x7f,0x80,0xff] {
                        text[pos]=byte;
                        check(&text);
                    }
                }
            }
        }
    }
}
