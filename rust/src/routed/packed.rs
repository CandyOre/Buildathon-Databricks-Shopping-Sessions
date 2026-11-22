use super::{EventData, EventKeys, EventRow, INVALID, OTHER, RawClick, WIDE, mix};

const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const VALUES: [u8; 256] = {
    let mut table = [0; 256];
    let mut digit = 0;
    while digit < 36 {
        table[DIGITS[digit] as usize] = digit as u8 + 1;
        digit += 1;
    }
    table
};

/// Bijective base 36 gives each spelling, including leading zeros and the
/// empty suffix, a distinct value. Eight digits fit in 42 bits.
pub(super) fn number(tail: &[u8]) -> Option<u64> {
    match tail.len() {
        8 => return eight_base36(u64::from_le_bytes(tail.try_into().unwrap())),
        7 => {
            // Overlapping in-bounds loads, prefixed with one artificial '0'.
            let low = u64::from(u32::from_le_bytes(tail[..4].try_into().unwrap()));
            let high = u64::from(u32::from_le_bytes(tail[3..].try_into().unwrap()));
            let word = u64::from(b'0') | (low << 8) | (high << 32);
            return eight_base36(word).map(|n| n - 36u64.pow(7));
        }
        9.. => return None,
        _ => {}
    }
    let mut number = 0u64;
    let mut valid = true;
    for &c in tail {
        let digit = VALUES[c as usize];
        valid &= digit != 0;
        number = number * 36 + u64::from(digit);
    }
    valid.then_some(number)
}

#[inline]
fn eight_base36(word: u64) -> Option<u64> {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    const EVEN: u64 = 0x00ff_00ff_00ff_00ff;
    const PAIRS: u64 = 0x0000_ffff_0000_ffff;
    let ascii = word & !HIGH;
    // Every unsigned lane starts below 128. Adding the threshold bias
    // can set its high bit but cannot carry into the next lane.
    let ge = |c: u8| (ascii + (128 - u64::from(c)) * ONES) & HIGH;
    let numeric = ge(b'0') & !ge(b':');
    let alpha = ge(b'a') & !ge(b'{');
    if (word & HIGH) != 0 || (numeric | alpha) != HIGH {
        return None;
    }
    // '0' maps to 1 and 'a' to 11, retaining leading zeroes in the rank.
    let digits = ascii - 47 * ONES - (alpha >> 7) * 39;
    let pairs = (digits & EVEN) * 36 + ((digits >> 8) & EVEN);
    let quads = (pairs & PAIRS) * (36 * 36) + ((pairs >> 16) & PAIRS);
    Some(u64::from(quads as u32) * 36u64.pow(4) + (quads >> 32))
}

/// Load the two common v5 suffix widths without reading across a CSV field.
/// A synthetic leading '0' contributes exactly 36^7 to the bijective rank.
#[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
#[inline(always)]
fn suffix_word(id: &[u8], prefix: u8) -> Option<(u64, u64)> {
    if id.first() != Some(&prefix) {
        return None;
    }
    match id.len() {
        8 => {
            let word = u64::from_le_bytes(id.try_into().unwrap());
            Some(((word & !255) | u64::from(b'0'), 36u64.pow(7)))
        }
        9 => Some((u64::from_le_bytes(id[1..].try_into().unwrap()), 0)),
        _ => None,
    }
}

/// Validate and decode two short keys simultaneously on optimized binaries.
#[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
#[inline]
fn key_and_user<const PREFIX: u8>(event: &[u8], user: &[u8]) -> Option<(u64, u64)> {
    use std::arch::x86_64::*;
    let (event, pad_event) = suffix_word(event, PREFIX)?;
    let (user, pad_user) = suffix_word(user, b'u')?;
    unsafe {
        let ascii = _mm_set_epi64x(user as i64, event as i64);
        let in_range = |low: i8, high: i8| {
            _mm_and_si128(
                _mm_cmpgt_epi8(ascii, _mm_set1_epi8(low - 1)),
                _mm_cmpgt_epi8(_mm_set1_epi8(high + 1), ascii),
            )
        };
        let numeric = in_range(b'0' as i8, b'9' as i8);
        let alpha = in_range(b'a' as i8, b'z' as i8);
        if _mm_movemask_epi8(_mm_or_si128(numeric, alpha)) != 65535 {
            return None;
        }
        let digits = _mm_sub_epi8(
            _mm_sub_epi8(ascii, _mm_set1_epi8(47)),
            _mm_and_si128(alpha, _mm_set1_epi8(39)),
        );
        let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x0124));
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32((1 << 16) | 36 * 36));
        let ranks = _mm_add_epi64(
            _mm_mul_epu32(quads, _mm_set1_epi32(36i32.pow(4))),
            _mm_srli_epi64::<32>(quads),
        );
        let event = _mm_cvtsi128_si64(ranks) as u64 - pad_event;
        let user = _mm_cvtsi128_si64(_mm_unpackhi_epi64(ranks, ranks)) as u64 - pad_user;
        Some((event, user))
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3")))]
#[inline]
fn key_and_user<const PREFIX: u8>(_event: &[u8], _user: &[u8]) -> Option<(u64, u64)> {
    None
}

#[inline]
pub(super) fn event_and_user(event: &[u8], user: &[u8]) -> Option<(u64, u64)> {
    key_and_user::<b'e'>(event, user)
}

#[inline]
pub(super) fn click_owner(
    click: &[u8],
    user: &[u8],
    wide_users: &mut Vec<[u64; 2]>,
    users: &super::Names,
    ids: &mut super::ClickKeyChunk,
    fallback: &super::Names,
) -> (u64, u64) {
    use crate::records::NUMERIC_CLICK;
    if let Some((rank, owner)) = key_and_user::<b'k'>(click, user) {
        return (NUMERIC_CLICK | rank, owner);
    }
    let owner = super::encode(user, b'u', wide_users, users, true);
    let id = click
        .strip_prefix(b"k")
        .and_then(number)
        .map_or_else(|| ids.encode(click, fallback), |rank| NUMERIC_CLICK | rank);
    (id, owner)
}

pub(super) use crate::input::write_base36 as write_number;

fn code(handle: u64, width: u32) -> Option<u64> {
    let marker = 1u64 << (width - 1);
    if handle < marker {
        Some(handle)
    } else if (WIDE..OTHER).contains(&handle) {
        Some(marker | u64::from(handle as u32))
    } else {
        None
    }
}

fn from_code(value: u64, width: u32, wide: &[[u64; 2]]) -> u64 {
    if value < (1u64 << (width - 1)) {
        return value;
    }
    let slot = value as u32;
    let [hi, lo] = wide[slot as usize];
    WIDE | (u64::from(mix(hi ^ lo.rotate_left(23))) << 32) | u64::from(slot)
}

#[derive(Clone, Copy, Default)]
pub(super) struct ShortEvent(u64, u64);

const MARK: u64 = 1 << 41;

/// Downstream aggregation never emits an event id. A permutation within the
/// inline-key domain keeps its spelling identity exact and leaves a usable
/// bucket fingerprint in the low bits for both passes over the records.
#[inline]
pub(super) fn distributed_key(id: u64) -> u64 {
    if id >= MARK {
        return id;
    }
    let v = (id ^ (id >> 21)).wrapping_mul(0x9e37_79b9_7f4a_7c15) & (MARK - 1);
    v ^ (v >> 17)
}

#[inline]
pub(super) fn fingerprint(id: u64) -> u32 {
    if id < MARK {
        (id & super::HASH_MASK) as u32
    } else {
        super::hash(id)
    }
}

impl ShortEvent {
    /// A verified, in-window view/cart with two inline spelling ranks. Its
    /// caller has already decided whether the millisecond offset is integral.
    #[inline(always)]
    pub fn ordinary(
        id: u64,
        user: u64,
        seconds: u32,
        millis: u32,
        whole: bool,
        ingest: u32,
    ) -> Option<Self> {
        if (id | user) >= MARK {
            return None;
        }
        let first = id | (user << 42);
        if whole && seconds < (1 << 21) - 65_536 && ingest < 1 << 21 {
            return Some(Self(
                first,
                (user >> 22) | (u64::from(seconds) << 20) | (u64::from(ingest) << 41),
            ));
        }
        let biased = i64::from(ingest) - i64::from(seconds) + 32;
        if ingest < super::SIDE && (0..4096).contains(&biased) {
            return Some(Self(
                first,
                (1 << 63) | (user >> 22) | (u64::from(millis) << 20) | ((biased as u64) << 51),
            ));
        }
        None
    }

    #[inline]
    pub fn key(self, k: &EventKeys) -> u64 {
        from_code(self.0 & ((1 << 42) - 1), 42, &k.wide)
    }
    pub fn pack(e: EventRow) -> Option<Self> {
        Self::encode::<false>(e, code(e.id, 42)?)
    }

    #[cfg(test)]
    pub fn pack_precise(e: EventRow) -> Option<Self> {
        Self::encode::<true>(e, code(e.id, 42)?)
    }

    /// Upper fingerprint bits are shared by neither the bucket nor the slot.
    /// Short spelling ranks may use every bit below MARK; wide keys have an
    /// explicit MARK tag and spill into the full row if their slot overflows.
    #[inline]
    pub fn pack_indexed(e: EventRow, hash: u32, bucket_bits: u32) -> Option<Self> {
        let id = if e.id < MARK {
            e.id
        } else if (WIDE..OTHER).contains(&e.id) {
            let slot = u64::from(e.id as u32);
            if slot >= 1 << (11 + bucket_bits) {
                return None;
            }
            MARK | (slot << (30 - bucket_bits)) | u64::from(hash >> bucket_bits)
        } else {
            return None;
        };
        Self::encode::<true>(e, id)
    }

    #[inline]
    pub fn bitmap_fingerprint(self, bucket_bits: u32) -> usize {
        let code = self.0 & (2 * MARK - 1);
        // The caller masks to at most 30 - bucket_bits bits.
        if code < MARK {
            (code >> bucket_bits) as usize
        } else {
            code as usize
        }
    }

    #[inline]
    pub fn indexed_key(self, bucket_bits: u32, bucket: u16) -> u64 {
        let code = self.0 & (2 * MARK - 1);
        if code < MARK {
            return code;
        }
        let fingerprint = ((code << bucket_bits) & super::HASH_MASK) | u64::from(bucket);
        let slot = (code & (MARK - 1)) >> (30 - bucket_bits);
        WIDE | (fingerprint << 32) | slot
    }

    #[inline]
    fn encode<const FRACTIONAL: bool>(e: EventRow, id: u64) -> Option<Self> {
        const NO_TIME: u32 = (1 << 21) - 1;
        const PAYLOAD_REF: u32 = NO_TIME - 65535;
        let user = code(e.user, 42)?;
        let stamp = if e.stamp == INVALID {
            NO_TIME
        } else if e.stamp >= super::SIDE && e.stamp - super::SIDE < NO_TIME - PAYLOAD_REF {
            PAYLOAD_REF + (e.stamp - super::SIDE)
        } else if e.stamp < super::SIDE && e.stamp % 1000 == 0 && e.stamp / 1000 < PAYLOAD_REF {
            e.stamp / 1000
        } else if FRACTIONAL && e.stamp < super::SIDE && e.ingest < super::SIDE {
            // A fractional view/cart can carry its exact 31-bit millisecond
            // offset and its nearby ingestion time in the same 16-byte lane.
            // The seconds layout leaves the high two bits unused.
            let seconds = e.stamp / 1000;
            let delta = i64::from(e.ingest) - i64::from(seconds) + 32;
            if !(0..4096).contains(&delta) {
                return None;
            }
            return Some(Self(
                id | (user << 42),
                (1 << 63) | (user >> 22) | (u64::from(e.stamp) << 20) | ((delta as u64) << 51),
            ));
        } else {
            return None;
        };
        if e.ingest >= 1 << 21 {
            return None;
        }
        Some(Self(
            id | (user << 42),
            (user >> 22) | (u64::from(stamp) << 20) | (u64::from(e.ingest) << 41),
        ))
    }

    pub fn unpack(self, k: &EventKeys, p: &EventData) -> EventRow {
        self.unpack_with_id(self.key(k), p)
    }

    #[inline(always)]
    pub fn winner(self, chunk: u32, p: &EventData) -> super::WinnerRow {
        let row = self.unpack_with_id(0, p);
        super::WinnerRow {
            user: row.user,
            stamp: row.stamp,
            chunk,
        }
    }

    #[inline(always)]
    pub fn unpack_with_id(self, id: u64, p: &EventData) -> EventRow {
        const NO_TIME: u32 = (1 << 21) - 1;
        const PAYLOAD_REF: u32 = NO_TIME - 65535;
        let (stamp, ingest) = if self.1 >> 63 != 0 {
            let stamp = ((self.1 >> 20) & 0x7fff_ffff) as u32;
            let delta = ((self.1 >> 51) & 4095) as i64 - 32;
            (stamp, (i64::from(stamp / 1000) + delta) as u32)
        } else {
            let time = ((self.1 >> 20) & ((1 << 21) - 1)) as u32;
            let stamp = if time == NO_TIME {
                INVALID
            } else if time >= PAYLOAD_REF {
                super::SIDE + (time - PAYLOAD_REF)
            } else {
                time * 1000
            };
            (stamp, (self.1 >> 41) as u32)
        };
        EventRow {
            id,
            user: from_code(
                (self.0 >> 42) | ((self.1 & ((1 << 20) - 1)) << 22),
                42,
                &p.users,
            ),
            stamp,
            ingest,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ShortClick(u64, u64);

impl ShortClick {
    // A canonical wide click keeps its full 128 bits in ClickIds. The containing
    // ClickBlock supplies the chunk, so only its slot goes into this record.
    pub const WIDE_SLOT: u64 = 1 << 40;

    #[inline]
    pub fn id_code(id: u64) -> Option<u64> {
        use crate::records::{HEX_CLICK, NUMERIC_CLICK};
        if id >= HEX_CLICK {
            return Some(Self::WIDE_SLOT | u64::from(id as u32));
        }
        let rank = id ^ NUMERIC_CLICK;
        (rank < Self::WIDE_SLOT).then_some(rank)
    }

    pub fn pack(user: u64, click: u64, time: i64, base: i64, campaign: u32) -> Option<Self> {
        let owner = code(user, 41)?;
        let delta = time.checked_sub(base)? as u64;
        if click >= 1 << 41 || delta >= 1 << 31 || campaign >= 1 << 15 {
            return None;
        }
        Some(Self(
            owner | (click << 41),
            (click >> 23) | (delta << 18) | (u64::from(campaign) << 49),
        ))
    }

    pub fn unpack(self, chunk: u32, base: i64, wide: &[[u64; 2]]) -> RawClick {
        let owner = from_code(self.0 & ((1 << 41) - 1), 41, wide);
        let click = (self.0 >> 41) | ((self.1 & ((1 << 18) - 1)) << 23);
        let id = if click >= Self::WIDE_SLOT {
            crate::records::HEX_CLICK | (u64::from(chunk) << 32) | u64::from(click as u32)
        } else {
            crate::records::NUMERIC_CLICK | click
        };
        RawClick {
            user: owner,
            time: base + ((self.1 >> 18) & ((1 << 31) - 1)) as i64,
            id,
            campaign: (self.1 >> 49) as u32,
            chunk,
        }
    }
}

const _: () = assert!(size_of::<ShortEvent>() == 16 && size_of::<ShortClick>() == 16);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paired_clicks_preserve_owner_and_lexical_ties_across_all_key_forms() {
        use crate::input::Names;
        use crate::records::{ClickIds, ClickKeyChunk, HEX_CLICK, NUMERIC_CLICK};
        let spellings: &[&[u8]] = &[
            b"k",
            b"k0",
            b"k00",
            b"k01a0d50",
            b"kz99ry6k4",
            b"k00000000",
            b"k00000000000000000000000000000000",
            b"k00000000000000000000000000000001",
            b"k10000000000000000000000000000001",
            b"k0000000000000000000000000000000A",
            b"kA",
            b"K01a0d50",
            b"kabcdefghi",
        ];
        let owners: &[&[u8]] = &[
            b"u003bc9a",
            b"u09ffg89z",
            b"u00000000",
            b"U001",
            b"u00000000000000000000000000000001",
        ];
        let users = Names::default();
        let fallback = Names::default();
        let (mut a, mut b) = (ClickKeyChunk::new(0), ClickKeyChunk::new(1));
        let (mut wa, mut wb) = (Vec::new(), Vec::new());
        let mut samples = Vec::new();
        for &spelling in spellings {
            for &user in owners {
                let (id, owner) = click_owner(spelling, user, &mut wa, &users, &mut a, &fallback);
                assert_eq!(
                    owner,
                    super::super::encode(user, b'u', &mut wb, &users, true)
                );
                let packed = if id >= HEX_CLICK {
                    Some(ShortClick::WIDE_SLOT | u64::from(id as u32))
                } else {
                    spelling
                        .strip_prefix(b"k")
                        .and_then(number)
                        .filter(|&x| x < ShortClick::WIDE_SLOT)
                };
                assert_eq!(ShortClick::id_code(id), packed);
                if let Some(n) = spelling.strip_prefix(b"k").and_then(number) {
                    assert_eq!(id, NUMERIC_CLICK | n);
                }
                samples.push((spelling, id, b.encode(spelling, &fallback)));
            }
        }
        assert_eq!(wa, wb);
        let ids = ClickIds::new(fallback, vec![a, b]);
        for &(raw, left, _) in &samples {
            for &(other, _, right) in &samples {
                assert_eq!(ids.compare(left, right), raw.cmp(other));
            }
        }
    }

    #[test]
    fn indexed_wide_keys_keep_the_hash_and_slot_without_loading_a_sidecar() {
        for bits in 0..=13 {
            let bucket_mask = (1u32 << bits) - 1;
            let limit = 1u32 << (11 + bits);
            for h in [0, 1, 0x1234_5678, super::super::HASH_MASK as u32] {
                for slot in [0, limit - 1, limit] {
                    let id = WIDE | (u64::from(h) << 32) | u64::from(slot);
                    let e = EventRow {
                        id,
                        user: 19,
                        stamp: 1001,
                        ingest: 3,
                    };
                    let v = ShortEvent::pack_indexed(e, h, bits);
                    if slot == limit {
                        assert!(v.is_none());
                        continue;
                    }
                    let v = v.unwrap();
                    assert_eq!(v.indexed_key(bits, (h & bucket_mask) as u16), id);
                    let usable = (1usize << (30 - bits)) - 1;
                    assert_eq!(v.bitmap_fingerprint(bits) & usable, (h >> bits) as usize);
                }
            }
            for rank in [0, 1, MARK - 2, MARK - 1, MARK, MARK + 1, OTHER] {
                let id = distributed_key(rank);
                let h = fingerprint(id);
                let e = EventRow {
                    id,
                    user: 19,
                    stamp: 1001,
                    ingest: 3,
                };
                let v = ShortEvent::pack_indexed(e, h, bits);
                if rank >= MARK {
                    assert_eq!(id, rank);
                    assert!(v.is_none());
                } else {
                    let v = v.unwrap();
                    assert!(id < MARK);
                    assert_eq!(v.indexed_key(bits, (h & bucket_mask) as u16), id);
                    assert_eq!(v.bitmap_fingerprint(bits) & 63, (h >> bits) as usize & 63);
                }
            }
        }
    }

    #[test]
    fn wide_base36_decoder_matches_every_byte_in_every_lane() {
        for sample in [b"0za3f90".as_slice(), b"z0ga107z", b"00000000", b"zzzzzzzz"] {
            let mut input = sample.to_vec();
            for pos in 0..sample.len() {
                for value in 0..=255 {
                    input[pos] = value;
                    let expected = input.iter().try_fold(0u64, |n, &b| {
                        DIGITS
                            .iter()
                            .position(|&symbol| symbol == b)
                            .map(|v| n * 36 + 1 + v as u64)
                    });
                    assert_eq!(number(&input), expected, "{input:?}");
                }
                input[pos] = sample[pos];
            }
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
    #[test]
    fn paired_key_decoder_keeps_both_ranks_and_rejects_mixed_spelling() {
        for mut event in [b"e01ad7z0".to_vec(), b"ez013ma01".to_vec()] {
            for mut user in [b"u4190a0b".to_vec(), b"u003wp3ji".to_vec()] {
                for field in 0..2 {
                    let len = if field == 0 { event.len() } else { user.len() };
                    for i in 0..len {
                        let old = if field == 0 { event[i] } else { user[i] };
                        for val in 0..=255 {
                            if field == 0 {
                                event[i] = val;
                            } else {
                                user[i] = val;
                            }
                            let expected = if event[0] == b'e' && user[0] == b'u' {
                                number(&event[1..]).zip(number(&user[1..]))
                            } else {
                                None
                            };
                            assert_eq!(event_and_user(&event, &user), expected);
                        }
                        if field == 0 {
                            event[i] = old;
                        } else {
                            user[i] = old;
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn fractional_candidate_preserves_exact_milliseconds_and_shifted_ingest() {
        let keys = EventKeys::default();
        let data = EventData::default();
        for stamp in [1, 31_999, 32_001, 86_400_001, super::super::SIDE - 1] {
            let sec = stamp / 1000;
            for difference in [-33, -32, -1, 0, 1, 4063, 4064] {
                let Ok(ingest) = u32::try_from(i64::from(sec) + difference) else {
                    continue;
                };
                let row = EventRow {
                    id: 10,
                    user: 23,
                    stamp,
                    ingest,
                };
                let encoded = ShortEvent::pack_precise(row);
                if (-32..=4063).contains(&difference) {
                    let decoded = encoded.unwrap().unpack(&keys, &data);
                    assert_eq!(
                        (decoded.id, decoded.user, decoded.stamp, decoded.ingest),
                        (10, 23, stamp, ingest)
                    );
                } else {
                    assert!(encoded.is_none());
                }
            }
        }
    }

    #[test]
    fn preserves_spelling_width_and_numeric_boundaries() {
        for s in [
            b"".as_slice(),
            b"0",
            b"00",
            b"0000abcd",
            b"abc",
            b"zzzzzzzz",
        ] {
            let n = number(s).unwrap();
            let mut b = [0; 8];
            let len = write_number(n, &mut b);
            assert_eq!(&b[..len], s);
        }
        assert!(number(b"aB").is_none());
        assert!(number(b"000000000").is_none());
        let k = EventKeys {
            wide: vec![[0, 1]],
            ..EventKeys::default()
        };
        let p = EventData {
            users: vec![[0, 0]],
            ..EventData::default()
        };
        let eid = from_code(1 << 41, 42, &k.wide);
        let user = from_code(1 << 41, 42, &p.users);
        for id in [0, (1 << 41) - 1, eid] {
            for stamp in [
                0,
                1_800_000,
                2_031_614_000,
                super::super::SIDE,
                super::super::SIDE + 65534,
                INVALID,
            ] {
                let e = EventRow {
                    id,
                    user,
                    stamp,
                    ingest: (1 << 21) - 1,
                };
                let r = ShortEvent::pack(e).unwrap().unpack(&k, &p);
                assert_eq!(
                    (r.id, r.user, r.stamp, r.ingest),
                    (id, user, stamp, e.ingest)
                );
            }
        }
        let t = ShortClick::pack(
            user,
            number(b"00ab").unwrap(),
            (1 << 31) - 1,
            0,
            (1 << 15) - 1,
        )
        .unwrap()
        .unpack(0, 0, &p.users);
        assert_eq!(t.user, user);
        assert_eq!(
            t.id,
            crate::records::NUMERIC_CLICK | number(b"00ab").unwrap()
        );
        assert_eq!(t.campaign, (1 << 15) - 1);
        assert_eq!(t.time, (1 << 31) - 1);
        let wide = ShortClick::pack(user, ShortClick::WIDE_SLOT | 0x9876, 800, 0, 1)
            .unwrap()
            .unpack(13, 0, &p.users);
        assert_eq!(wide.id, crate::records::HEX_CLICK | (13u64 << 32) | 0x9876);
    }
}
