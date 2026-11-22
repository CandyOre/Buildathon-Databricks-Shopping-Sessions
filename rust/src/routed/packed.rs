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
    if tail.len() > 8 {
        return None;
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

#[derive(Clone, Copy)]
pub(super) struct ShortEvent(u64, u64);

impl ShortEvent {
    pub fn pack(e: EventRow) -> Option<Self> {
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
        let stamp = ((self.1 >> 20) & ((1 << 21) - 1)) as u32;
        EventRow {
            id: from_code(self.0 & ((1 << 42) - 1), 42, &k.wide),
            user: from_code(
                (self.0 >> 42) | ((self.1 & ((1 << 20) - 1)) << 22),
                42,
                &p.users,
            ),
            stamp: if stamp == NO_TIME {
                INVALID
            } else if stamp >= PAYLOAD_REF {
                super::SIDE + (stamp - PAYLOAD_REF)
            } else {
                stamp * 1000
            },
            ingest: (self.1 >> 41) as u32,
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
