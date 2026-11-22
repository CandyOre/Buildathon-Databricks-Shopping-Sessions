//! A dedup producer owns one allocation. Each raw-person destination writes
//! eight-byte records from the front of its stripe and sixteen-byte records
//! from the back. A skewed destination can grow without relocating any stripe.
use super::{RawWinnerBlock, ShortWinner, WinnerRow};
use std::mem::MaybeUninit;

#[derive(Clone, Copy, Default)]
struct Cursor {
    front: u32,
    back: u32,
    spill: u32, // one-based index in spills
}

pub(super) struct WinnerStore {
    words: Vec<MaybeUninit<u64>>,
    stride: usize,
    cursors: Vec<Cursor>,
    spills: Vec<RawWinnerBlock>,
    compact: bool,
}

impl WinnerStore {
    pub fn new(candidates: usize, parts: usize, compact: bool, striped: bool) -> Self {
        let avg = candidates.div_ceil(parts);
        let stride = if striped && avg != 0 {
            // Space for mixed widths and scatter variation. Capacity affects
            // only the proportion in spills; neither bound is a row limit.
            let words = if compact {
                avg + avg.div_ceil(3)
            } else {
                avg * 2
            };
            (words + 4)
                .next_multiple_of(8)
                .min(u32::MAX as usize / parts)
        } else {
            0
        };
        let count = stride.checked_mul(parts).expect("winner stripe capacity");
        let mut words = Vec::with_capacity(count);
        words.resize_with(count, MaybeUninit::uninit);
        // Absolute word positions remove a stride multiplication from each
        // winner write. A capped reservation still spills all excess rows.
        let cursors = (0..parts)
            .map(|p| Cursor {
                front: (p * stride) as u32,
                back: ((p + 1) * stride) as u32,
                spill: 0,
            })
            .collect();
        Self {
            words,
            stride,
            cursors,
            spills: Vec::new(),
            compact,
        }
    }

    #[inline]
    pub fn push(&mut self, part: usize, row: WinnerRow) {
        let short = self.compact.then(|| ShortWinner::encode(row)).flatten();
        let c = &mut self.cursors[part];
        if let Some(s) = short {
            if c.front < c.back {
                self.words[c.front as usize].write(s.0);
                c.front += 1;
                return;
            }
        } else if c.back - c.front >= 2 {
            // WinnerRow is repr(C), two 8-byte words with 8-byte alignment.
            // The two cursors leave this range exclusively owned by this row.
            c.back -= 2;
            unsafe {
                self.words
                    .as_mut_ptr()
                    .add(c.back as usize)
                    .cast::<WinnerRow>()
                    .write(row);
            }
            return;
        }
        if c.spill == 0 {
            self.spills.push(RawWinnerBlock::default());
            c.spill = self.spills.len() as u32;
        }
        let spill = &mut self.spills[c.spill as usize - 1];
        if let Some(s) = short {
            spill.short.push(s);
        } else {
            spill.full.push(row);
        }
    }

    #[inline]
    fn spill(&self, c: Cursor) -> Option<&RawWinnerBlock> {
        self.spills.get(c.spill.wrapping_sub(1) as usize)
    }

    pub fn len(&self, part: usize) -> usize {
        let c = self.cursors[part];
        let start = part * self.stride;
        c.front as usize - start
            + (start + self.stride - c.back as usize) / 2
            + self.spill(c).map_or(0, RawWinnerBlock::len)
    }

    #[inline]
    pub fn rows(&self, part: usize) -> impl Iterator<Item = WinnerRow> + '_ {
        let c = self.cursors[part];
        let start = self.stride * part;
        let front = &self.words[start..c.front as usize];
        let back = c.back as usize;
        // Producer mutation ended before any target view is published.
        // Only committed, initialized records lie in each returned slice.
        let full = unsafe {
            std::slice::from_raw_parts(
                self.words.as_ptr().add(back).cast::<WinnerRow>(),
                (start + self.stride - back) / 2,
            )
        };
        let spill = self.spill(c);
        front
            .iter()
            .map(|s| ShortWinner(unsafe { s.assume_init() }).decode())
            .chain(
                spill
                    .into_iter()
                    .flat_map(|s| s.short.iter().copied().map(ShortWinner::decode)),
            )
            .chain(full.iter().rev().copied())
            .chain(spill.into_iter().flat_map(|s| s.full.iter().copied()))
    }

    pub fn metrics(&self) -> (usize, usize, usize) {
        let mut short: usize = self
            .cursors
            .iter()
            .enumerate()
            .map(|(part, c)| c.front as usize - part * self.stride)
            .sum();
        let mut capacity = self.words.capacity() * size_of::<u64>();
        let mut overflow = 0;
        for spill in &self.spills {
            short += spill.short.len();
            capacity += spill.short.capacity() * size_of::<ShortWinner>()
                + spill.full.capacity() * size_of::<WinnerRow>();
            overflow += spill.len();
        }
        (short, capacity, overflow)
    }
}

const _: () = assert!(size_of::<WinnerRow>() == 16 && align_of::<WinnerRow>() == align_of::<u64>());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routed::{SIDE, WIDE};

    #[test]
    fn skewed_stripes_preserve_typed_lanes_and_source_references() {
        for predicted in [0, 4, 511] {
            for compact in [false, true] {
                let mut store = WinnerStore::new(predicted, 32, compact, true);
                let mut reference: Vec<_> = (0..32).map(|_| RawWinnerBlock::default()).collect();
                for i in 0..750u32 {
                    let part = if i % 4 == 0 { i as usize / 4 % 31 } else { 0 };
                    let (user, stamp) = match i % 6 {
                        0 => (WIDE | u64::from(i), 101),
                        1 => (u64::from(i), SIDE + i),
                        2 => ((1 << 38) + u64::from(i), 202),
                        3 => ((1 << 38) + u64::from(i), i * 1000),
                        _ => (u64::from(i), i + 1),
                    };
                    let row = WinnerRow {
                        user,
                        chunk: 311 + i,
                        stamp,
                    };
                    store.push(part, row);
                    reference[part].push(row, compact);
                }
                let (short, _, over) = store.metrics();
                assert_eq!(
                    short,
                    reference.iter().map(|p| p.short.len()).sum::<usize>()
                );
                assert!(over > 0);
                for (part, want) in reference.into_iter().enumerate() {
                    assert_eq!(store.len(part), want.len());
                    let tuple = |r: WinnerRow| (r.user, r.chunk, r.stamp);
                    assert!(store.rows(part).map(tuple).eq(want.rows().map(tuple)));
                }
            }
        }
    }
}
