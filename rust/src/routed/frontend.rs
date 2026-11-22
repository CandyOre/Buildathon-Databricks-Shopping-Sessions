//! Reader-owned event buckets for the two architecture candidates.
use super::*;

type Decoded = (
    Pieces<EventRow>,
    Pieces<packed::ShortEvent>,
    Vec<NativeBlock<EventRow, packed::ShortEvent>>,
    EventKeys,
    EventData,
    usize,
);

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Mode {
    Legacy,
    Batch,
    Direct,
}

pub(super) fn mode(plan: Plan) -> Mode {
    if !plan.person_kernel || !plan.packed {
        return Mode::Legacy;
    }
    match std::env::var("PERF_ROUTE_FRONTEND").as_deref() {
        Ok("legacy") => Mode::Legacy,
        Ok("direct") => Mode::Direct,
        _ => Mode::Batch,
    }
}

const LANES: usize = 4;

#[derive(Clone, Copy)]
enum Payload {
    Invalid,
    Whole(u32),
    Millis { seconds: u32, offset: u32 },
    Exact(i64, Option<Purchase>),
}

impl Payload {
    #[inline(always)]
    fn reference(p: Option<(i64, Option<Purchase>)>) -> Self {
        p.map_or(Self::Invalid, |(time, sale)| Self::Exact(time, sale))
    }
}

struct Limits {
    window: Window,
    base: i64,
    width: u64,
    first_second: i64,
    seconds: u64,
    aligned: bool,
    aligned_end: bool,
}

impl Limits {
    fn new(window: Window) -> Self {
        let ceil = |t: i64| t.div_euclid(1000) + i64::from(t.rem_euclid(1000) != 0);
        let first_second = ceil(window.start);
        Self {
            base: window.start.div_euclid(1000),
            width: if window.end > window.start {
                window.end.wrapping_sub(window.start) as u64
            } else {
                0
            },
            first_second,
            seconds: (ceil(window.end) - first_second).max(0) as u64,
            aligned: window.start.rem_euclid(1000) == 0,
            aligned_end: window.end.rem_euclid(1000) == 0,
            window,
        }
    }

    #[inline(always)]
    fn ordinary(&self, clock: input::EventClock, ingest: i64) -> Payload {
        use input::EventClock;
        if let EventClock::Decimal(t) = clock {
            if self.aligned && self.aligned_end {
                let seconds = (t >> 10) as i64;
                let delta = (seconds - self.base) as u64;
                if delta >= self.seconds || seconds < ingest - 86_400 {
                    return Payload::Invalid;
                }
                let millis = (t & 1023) as u32;
                if delta <= u64::from((SIDE - 1) / 1000) {
                    let seconds = delta as u32;
                    let offset = seconds * 1000 + millis;
                    if offset < SIDE {
                        return if millis == 0 {
                            Payload::Whole(seconds)
                        } else {
                            Payload::Millis { seconds, offset }
                        };
                    }
                }
                return Payload::Exact(seconds * 1000 + i64::from(millis), None);
            }
        }
        let time = match clock {
            EventClock::Whole(s) => {
                let delta = (s - self.first_second) as u64;
                if delta >= self.seconds || s < ingest - 86_400 {
                    return Payload::Invalid;
                }
                if self.aligned && delta <= u64::from((SIDE - 1) / 1000) {
                    return Payload::Whole(delta as u32);
                }
                s * 1000
            }
            EventClock::Millis(_) | EventClock::Decimal(_) => {
                let t = clock.millis().unwrap();
                if t.wrapping_sub(self.window.start) as u64 >= self.width
                    || t < ingest * 1000 - input::DAY
                {
                    return Payload::Invalid;
                }
                t
            }
            EventClock::Invalid => return Payload::Invalid,
        };
        let delta = time.wrapping_sub(self.window.start) as u64;
        if delta < u64::from(SIDE) {
            let offset = delta as u32;
            Payload::Millis {
                seconds: offset / 1000,
                offset,
            }
        } else {
            Payload::Exact(time, None)
        }
    }

    #[inline(always)]
    fn payload(
        &self,
        f: &[&[u8]; 7],
        clock: input::EventClock,
        ingest: i64,
        dims: &Dimensions,
    ) -> Payload {
        if matches!(f[4], b"view" | b"cart") {
            if f[1].is_empty() || f[5].is_empty() {
                return Payload::Invalid;
            }
            return self.ordinary(clock, ingest);
        }
        Payload::reference(crate::csv::event_payload(
            f,
            clock.millis(),
            ingest * 1000,
            self.window,
            dims,
        ))
    }
}

struct Buckets {
    result: Pieces<packed::ShortEvent>,
    pending: Vec<[packed::ShortEvent; LANES]>,
    used: Vec<u8>,
    initial: usize,
}

impl Buckets {
    fn new<const BATCH: bool>(parts: usize, bytes: usize) -> Self {
        let buffered = if BATCH { parts } else { 0 };
        Self {
            result: empty_buckets(parts),
            pending: vec![[packed::ShortEvent::default(); LANES]; buffered],
            used: vec![0; buffered],
            // A capacity hint only. Skewed or extremely long input rows may
            // grow a populated bucket or leave other buckets unallocated.
            initial: (bytes / parts / 80).clamp(LANES, 8192),
        }
    }

    #[inline]
    fn push<const BATCH: bool>(&mut self, bucket: usize, row: packed::ShortEvent) {
        if !BATCH {
            self.result[bucket].push(row);
            return;
        }
        let at = usize::from(self.used[bucket]);
        self.pending[bucket][at] = row;
        if at + 1 == LANES {
            let out = &mut self.result[bucket];
            if out.capacity() == 0 {
                out.reserve_exact(self.initial);
            }
            out.extend_from_slice(&self.pending[bucket]);
            self.used[bucket] = 0;
        } else {
            self.used[bucket] = (at + 1) as u8;
        }
    }

    fn finish(mut self) -> Pieces<packed::ShortEvent> {
        for ((out, pending), n) in self.result.iter_mut().zip(self.pending).zip(self.used) {
            out.extend_from_slice(&pending[..usize::from(n)]);
        }
        self.result
    }
}

pub(super) fn decode<const BATCH: bool>(
    sources: &[Chunk],
    plan: Plan,
    calendar: &Calendar,
    window: Window,
    dims: &Dimensions,
    other_ids: &Names,
    raw_users: &Names,
) -> Result<Decoded> {
    let mut keys = EventKeys::default();
    let mut data = EventData::default();
    let mut valid_count = 0;
    let mut full = empty_buckets(plan.event_parts);
    let bytes = sources.iter().map(Chunk::byte_len).sum();
    let mut short = Buckets::new::<BATCH>(plan.event_parts, bytes);
    let mask = plan.event_parts - 1;
    let bucket_bits = plan.event_parts.trailing_zeros();
    let limits = Limits::new(window);
    let mut emit = |id: &[u8], user: &[u8], seconds: i64, payload: Payload| {
        let ingest = keys.ingest_seconds(limits.base, seconds);
        let (id, user, stamp) = match payload {
            Payload::Invalid => (
                packed::distributed_key(encode(id, b'e', &mut keys.wide, other_ids, true)),
                0,
                INVALID,
            ),
            _ => {
                valid_count += 1;
                let (id, user) = packed::event_and_user(id, user).unwrap_or_else(|| {
                    (
                        encode(id, b'e', &mut keys.wide, other_ids, true),
                        encode(user, b'u', &mut data.users, raw_users, true),
                    )
                });
                let id = packed::distributed_key(id);
                let stamp = match payload {
                    Payload::Whole(s) => {
                        if let Some(row) =
                            packed::ShortEvent::ordinary(id, user, s, s * 1000, true, ingest)
                        {
                            short.push::<BATCH>(id as usize & mask, row);
                            return;
                        }
                        s * 1000
                    }
                    Payload::Millis {
                        seconds: s,
                        offset: ms,
                    } => {
                        if let Some(row) =
                            packed::ShortEvent::ordinary(id, user, s, ms, s * 1000 == ms, ingest)
                        {
                            short.push::<BATCH>(id as usize & mask, row);
                            return;
                        }
                        ms
                    }
                    Payload::Exact(time, sale) => data.stamp(window.start, time, sale),
                    Payload::Invalid => unreachable!(),
                };
                (id, user, stamp)
            }
        };
        let fingerprint = packed::fingerprint(id);
        let part = fingerprint as usize & mask;
        let row = EventRow {
            id,
            user,
            stamp,
            ingest,
        };
        if let Some(record) = packed::ShortEvent::pack_indexed(row, fingerprint, bucket_bits) {
            short.push::<BATCH>(part, record);
        } else {
            full[part].push(row);
        }
    };
    let specialized = std::env::var("PERF_ROUTE_TIMES").as_deref() != Ok("reference");
    for source in sources {
        if specialized {
            source.scan_parsed(|f: [&[u8]; 7]| {
                if f[0].is_empty() {
                    return;
                }
                let (time, ingest) = calendar.event_clock(f[2], f[3]);
                let payload = limits.payload(&f, time, ingest, dims);
                emit(f[0], f[1], ingest, payload);
            })?;
        } else {
            source.scan_events(calendar, window, dims, |id, user, ingest, payload| {
                emit(
                    id,
                    user,
                    ingest.div_euclid(1000),
                    Payload::reference(payload),
                )
            })?;
        }
    }
    Ok((full, short.finish(), Vec::new(), keys, data, valid_count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_csv_records_match_reference_payloads_and_packed_winners() {
        let start = input::timestamp(b"2024-01-01T00:00:00Z").unwrap();
        let dims = Dimensions {
            products: [("p1".to_owned(), 3)].into_iter().collect(),
            categories: vec![],
            unknown_category: 2,
            campaigns: vec![],
            campaign_ids: std::iter::empty().collect(),
        };
        let cases: &[&[u8]] = &[
            br#""e01ad7z0",u149aabc,"2024-01-01T00:00:00Z",2024-01-01T00:04:00Z,view,p1,noise"#,
            b"e01ad7z0,u149aabc,1704067200000,2024-01-01T00:00:00Z,cart,p1,",
            b"e01ad7z0,u149aabc,1704067200001,2024-01-01T00:00:00Z,view,p1,",
            b"e01ad7z0,u149aabc,1704067200999,2024-01-02T00:00:00Z,view,p1,",
            b"e01ad7z0,u149aabc,1704067200999,2024-01-02T00:00:01Z,view,p1,",
            b"e01ad7z0,u149aabc,1704153600000,2024-01-02T00:00:00Z,view,p1,",
            b"e01ad7z0,u149aabc,2024-01-01T00:60:00Z,2024-01-01T00:00:00Z,cart,p1,",
            b"e01ad7z0,u149aabc,2024-01-01T03:00:42+03:00,2024-01-01T00:00:42Z,cart,p1,",
            b"e01ad7z0,u149aabc,1704067201002,2024-01-01T00:00:00Z,purchase,p1,13",
            b"e01ad7z0,u149aabc,1704067201002,2024-01-01T00:00:00Z,purchase,p1,0",
            b"e01ad7z0,u149aabc,1704067201002,2024-01-01T00:00:00Z,cart,,",
            b"e01ad7z0,,1704067201002,2024-01-01T00:00:00Z,view,p1,",
            b"e01ad7z0,u149aabc,17040672a1002,2024-01-01T00:00:00Z,view,p1,",
            b"e01ad7z0,u149aabc,01704067201002,2024-01-01T00:00:00Z,view,p1,",
            b"e01ad7z0,u149aabc,1704067201002,2024-01-01T00:00:00Z,unknown,p1,",
        ];
        for (a, b) in [
            (start, start + input::DAY),
            (start + 1, start + 1000),
            (start - 1, start + input::DAY + 1),
            (start - 40 * input::DAY, start + input::DAY),
            (start + input::DAY, start),
        ] {
            let window = Window { start: a, end: b };
            let limits = Limits::new(window);
            let cal = Calendar::new(window);
            for line in cases {
                let f = crate::csv::fields::<7>(line).unwrap();
                let (clock, ingest) = cal.event_clock(f[2], f[3]);
                let reference = crate::csv::event_payload(
                    &f,
                    input::timestamp(f[2]),
                    input::ingest_timestamp(f[3]),
                    window,
                    &dims,
                );
                let payload = limits.payload(&f, clock, ingest, &dims);
                let p = match payload {
                    Payload::Invalid => None,
                    Payload::Exact(t, p) => Some((t, p)),
                    Payload::Whole(s) => Some((a + i64::from(s) * 1000, None)),
                    Payload::Millis { offset, .. } => Some((a + i64::from(offset), None)),
                };
                let tuple = |(t, p): (i64, Option<Purchase>)| (t, p.map(|s| (s.category, s.cents)));
                assert_eq!(p.map(tuple), reference.map(tuple), "{line:?}");
                let (s, ms, whole) = match payload {
                    Payload::Whole(s) => (s, s * 1000, true),
                    Payload::Millis {
                        seconds: s,
                        offset: o,
                    } => (s, o, s * 1000 == o),
                    _ => continue,
                };
                let id = packed::distributed_key(packed::number(&f[0][1..]).unwrap());
                let user = packed::number(&f[1][1..]).unwrap();
                let mut keys = EventKeys::default();
                let z = keys.ingest_seconds(limits.base, ingest);
                let direct = packed::ShortEvent::ordinary(id, user, s, ms, whole, z);
                let h = packed::fingerprint(id);
                let cmp = packed::ShortEvent::pack_indexed(
                    EventRow {
                        id,
                        user,
                        stamp: ms,
                        ingest: z,
                    },
                    h,
                    8,
                );
                let unpack = |r: packed::ShortEvent| {
                    let v =
                        r.unpack_with_id(r.indexed_key(8, h as u8 as u16), &EventData::default());
                    (v.id, v.user, v.stamp, v.ingest)
                };
                assert_eq!(direct.map(unpack), cmp.map(unpack));
            }
        }
    }

    #[test]
    fn four_lane_writers_preserve_bucket_order_and_partial_tails() {
        let keys = EventKeys::default();
        let data = EventData::default();
        let mut buckets = Buckets::new::<true>(8, 800);
        let mut expected: Vec<Vec<u64>> = empty_buckets(8);
        for i in 0..41u64 {
            let bucket = ((i * i / 4) % 7) as usize;
            expected[bucket].push(i);
            buckets.push::<true>(
                bucket,
                packed::ShortEvent::pack_precise(EventRow {
                    id: i,
                    user: i + 1,
                    stamp: 1001,
                    ingest: 2,
                })
                .unwrap(),
            );
        }
        for (bucket, values) in buckets.finish().into_iter().zip(expected) {
            let actual: Vec<_> = bucket
                .into_iter()
                .map(|v| v.unpack(&keys, &data).id)
                .collect();
            assert_eq!(actual, values);
        }
    }
}
