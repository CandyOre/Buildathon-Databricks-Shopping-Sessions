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
    let base = window.start.div_euclid(1000);
    let mut emit =
        |id: &[u8], user: &[u8], ingest: i64, payload: Option<(i64, Option<Purchase>)>| {
            let (id, user, stamp) = if let Some((time, sale)) = payload {
                valid_count += 1;
                let (id, user) = packed::event_and_user(id, user).unwrap_or_else(|| {
                    (
                        encode(id, b'e', &mut keys.wide, other_ids, true),
                        encode(user, b'u', &mut data.users, raw_users, true),
                    )
                });
                (id, user, data.stamp(window.start, time, sale))
            } else {
                (
                    encode(id, b'e', &mut keys.wide, other_ids, true),
                    0,
                    INVALID,
                )
            };
            let id = packed::distributed_key(id);
            let fingerprint = packed::fingerprint(id);
            let part = fingerprint as usize & mask;
            let row = EventRow {
                id,
                user,
                stamp,
                ingest: keys.ingest_seconds(base, ingest),
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
                let (time, ingest) = calendar.routed_event_times(f[2], f[3]);
                let payload = crate::csv::event_payload(&f, time, ingest * 1000, window, dims);
                emit(f[0], f[1], ingest, payload);
            })?;
        } else {
            source.scan_events(calendar, window, dims, |id, user, ingest, payload| {
                emit(id, user, ingest.div_euclid(1000), payload)
            })?;
        }
    }
    Ok((full, short.finish(), Vec::new(), keys, data, valid_count))
}

#[cfg(test)]
mod tests {
    use super::*;

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
