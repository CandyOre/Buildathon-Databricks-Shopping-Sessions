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

pub(super) fn write_number(mut n: u64, output: &mut [u8; 8]) -> usize {
    let mut start = 8;
    while n != 0 {
        n -= 1;
        start -= 1;
        output[start] = DIGITS[(n % 36) as usize];
        n /= 36;
    }
    output.copy_within(start.., 0);
    let length = 8 - start;
    output[length..].fill(0);
    length
}

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

impl ShortEvent {
    #[inline]
    pub fn key(self, k: &EventKeys) -> u64 {
        from_code(self.0 & ((1 << 42) - 1), 42, &k.wide)
    }
    pub fn pack(e: EventRow) -> Option<Self> {
        Self::encode::<false>(e)
    }

    pub fn pack_precise(e: EventRow) -> Option<Self> {
        Self::encode::<true>(e)
    }

    #[inline]
    fn encode<const FRACTIONAL: bool>(e: EventRow) -> Option<Self> {
        const NO_TIME: u32 = (1 << 21) - 1;
        const PAYLOAD_REF: u32 = NO_TIME - 65535;
        let id = code(e.id, 42)?;
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
            id: self.key(k),
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
            let mut tail = [0; 8];
            write_number(click, &mut tail);
            u64::from_be_bytes(tail)
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
    fn wide_base36_decoder_matches_every_byte_in_every_lane() {
        for sample in [b"0za3f90".as_slice(), b"z0ga107z", b"00000000", b"zzzzzzzz"] {
            let mut input = sample.to_vec();
            for pos in 0..sample.len() {
                for value in 0..=255 {
                    input[pos] = value;
                    let expected = input.iter().try_fold(0u64, |n, &b| {
                        DIGITS.iter().position(|&symbol| symbol == b)
                            .map(|v| n * 36 + 1 + v as u64)
                    });
                    assert_eq!(number(&input), expected, "{input:?}");
                }
                input[pos] = sample[pos];
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
                let Ok(ingest) = u32::try_from(i64::from(sec) + difference) else { continue };
                let row = EventRow { id: 10, user: 23, stamp, ingest };
                let encoded = ShortEvent::pack_precise(row);
                if (-32..=4063).contains(&difference) {
                    let decoded = encoded.unwrap().unpack(&keys, &data);
                    assert_eq!((decoded.id,decoded.user,decoded.stamp,decoded.ingest), (10,23,stamp,ingest));
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
        assert_eq!(t.id, u64::from_be_bytes(*b"00ab\0\0\0\0"));
        assert_eq!(t.campaign, (1 << 15) - 1);
        assert_eq!(t.time, (1 << 31) - 1);
        let wide = ShortClick::pack(user, ShortClick::WIDE_SLOT | 0x9876, 800, 0, 1)
            .unwrap()
            .unpack(13, 0, &p.users);
        assert_eq!(wide.id, crate::records::HEX_CLICK | (13u64 << 32) | 0x9876);
    }
}
