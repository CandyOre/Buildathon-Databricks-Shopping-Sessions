use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use ahash::AHashMap;
use anyhow::Result;
use rayon::prelude::*;

use crate::csv::chunks;
use crate::input::{
    self, Aliases, DAY, Dimensions, LOOKBACK, Names, PersonCache, SHARDS, Window, text, unsigned,
};

const EXTERNAL_ID: u64 = 1 << 63;
const INVALID_PERSON: u32 = u32::MAX;

// Encode the common prefix plus up to eight following bytes without hashing
// or base conversion. The high bit is reserved for interned names. Right zero
// padding preserves byte order; names ending in NUL use the exact fallback.
fn id_for(id: &[u8], prefix: u8, fallback: &Names) -> u64 {
    if let [first, tail @ ..] = id {
        if *first == prefix && tail.len() <= 8 && tail.last() != Some(&0) {
            let word = match tail {
                full @ [_, _, _, _, _, _, _, _] => u64::from_be_bytes(full.try_into().unwrap()),
                _ => {
                    let mut bytes = [0; 8];
                    bytes[..tail.len()].copy_from_slice(tail);
                    u64::from_be_bytes(bytes)
                }
            };
            if word < EXTERNAL_ID {
                return word;
            }
        }
    }
    EXTERNAL_ID | u64::from(fallback.intern(text(id)))
}

pub struct ClickIds(Vec<Vec<Arc<str>>>);

impl ClickIds {
    pub fn compare(&self, left: u64, right: u64) -> Ordering {
        if left < EXTERNAL_ID && right < EXTERNAL_ID {
            return left.cmp(&right);
        }
        let (mut left_buffer, mut right_buffer) = ([0; 9], [0; 9]);
        self.bytes(left, &mut left_buffer)
            .cmp(self.bytes(right, &mut right_buffer))
    }

    fn bytes<'a>(&'a self, id: u64, buffer: &'a mut [u8; 9]) -> &'a [u8] {
        if id >= EXTERNAL_ID {
            return input::name_for(&self.0, id as u32).as_bytes();
        }
        buffer[0] = b'k';
        buffer[1..].copy_from_slice(&id.to_be_bytes());
        let end = 9 - (id.trailing_zeros() / 8) as usize;
        &buffer[..end]
    }
}

#[derive(Clone, Copy)]
pub struct Purchase {
    pub category: u32,
    pub cents: u32,
}

#[derive(Clone, Copy)]
pub struct Event {
    pub time: i64,
    pub person: u32,
    pub purchase: u32,
}

const EXTENDED: u32 = 1 << 31;

struct EntryId {
    id: u64,
    ingested: u32,
    position: u32,
}

#[derive(Default)]
struct IdRun {
    rows: Vec<EntryId>,
    far_ingests: Vec<i64>,
}

impl IdRun {
    fn push(&mut self, id: u64, ingested: i64, position: u32, origin_seconds: i64) {
        let offset = (ingested.div_euclid(1000) - origin_seconds) as u64;
        let ingested = if offset < u64::from(EXTENDED) {
            offset as u32
        } else {
            let index = self.far_ingests.len() as u32;
            self.far_ingests.push(ingested);
            EXTENDED | index
        };
        self.rows.push(EntryId {
            id,
            ingested,
            position,
        });
    }

    fn ingest_time(&self, value: u32, origin_seconds: i64) -> i64 {
        if value < EXTENDED {
            (origin_seconds + i64::from(value)) * 1000
        } else {
            self.far_ingests[(value ^ EXTENDED) as usize]
        }
    }
}

struct PersonEvent {
    person: u32,
    time: u32,
}

#[derive(Default)]
struct PersonRun {
    rows: Vec<PersonEvent>,
    extra: Vec<(Event, Option<Purchase>)>,
}

impl PersonRun {
    fn push(&mut self, person: u32, time: i64, purchase: Option<Purchase>, start: i64) -> u32 {
        // Each run belongs to one 4 MiB input chunk; a position plus a shard
        // index fits in 32 bits even when every byte ends a row.
        let at = self.rows.len() as u32;
        let offset = time.wrapping_sub(start) as u64;
        let row = if person < EXTENDED && offset <= u64::from(u32::MAX) && purchase.is_none() {
            PersonEvent {
                person,
                time: offset as u32,
            }
        } else {
            let index = self.extra.len() as u32;
            self.extra.push((
                Event {
                    time,
                    person,
                    purchase: 0,
                },
                purchase,
            ));
            PersonEvent {
                person: EXTENDED,
                time: index,
            }
        };
        self.rows.push(row);
        at * SHARDS as u32 + person % SHARDS as u32
    }
}

pub struct Candidates {
    compact_times: bool,
    by_id: Vec<Vec<IdRun>>,
    by_person: Vec<Vec<PersonRun>>,
    start: i64,
    origin_seconds: i64,
}

pub struct Batch<T> {
    pub rows: Vec<T>,
    pub purchases: Vec<Purchase>,
}

impl<T> Default for Batch<T> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            purchases: Vec::new(),
        }
    }
}

impl<T> Batch<T> {
    fn add_purchase(&mut self, purchase: Option<Purchase>) -> u32 {
        purchase.map_or(0, |value| {
            self.purchases.push(value);
            u32::try_from(self.purchases.len()).expect("too many purchases in one shard")
        })
    }
}

fn shards<T: Default>() -> Vec<T> {
    (0..SHARDS).map(|_| T::default()).collect()
}

pub fn read_events(
    data: &Path,
    window: Window,
    dimensions: &Dimensions,
    users: &Names,
    aliases: &Aliases,
) -> Result<Candidates> {
    let ids = Names::default();
    let calendar = input::Calendar::new(window);
    let origin_seconds = window.start.div_euclid(1000) - 86_400;
    let parsed: Vec<_> = chunks(&data.join("events"))?
        .into_par_iter()
        .map_init(PersonCache::default, |people, chunk| {
            let mut id_runs = shards::<IdRun>();
            let mut person_runs = shards::<PersonRun>();
            chunk.scan_parsed(|[id, user, ts, ingested, kind, product, price]| {
                if id.is_empty() {
                    return;
                }
                let proposal = people.prepare(users, aliases, user);
                let ingested = calendar.ingest(ingested);
                let id = id_for(id, b'e', &ids);
                // All three operations are bijective, including the multiplication
                // by an odd number. Using the mixed value as the ID stays exact.
                let mixed = (id ^ (id >> 32)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                let id = mixed ^ (mixed >> 31);
                let shard = id as usize % SHARDS;
                let mut valid = || {
                    if user.is_empty() || product.is_empty() {
                        return None;
                    }
                    let time = calendar.parse(ts)?;
                    if time < window.start || time >= window.end || time < ingested - DAY {
                        return None;
                    }
                    let purchase = match kind {
                        b"view" | b"cart" => None,
                        b"purchase" => {
                            let cents = unsigned(price)?;
                            if !(1..=10_000_000).contains(&cents) {
                                return None;
                            }
                            let category = dimensions
                                .products
                                .get(text(product))
                                .copied()
                                .unwrap_or(dimensions.unknown_category);
                            Some(Purchase {
                                category,
                                cents: cents as u32,
                            })
                        }
                        _ => return None,
                    };
                    Some((
                        time,
                        people.prepared(proposal, users, aliases, user),
                        purchase,
                    ))
                };
                // Invalid candidates still compete in deduplication. The earliest copy wins.
                let position = valid().map_or(u32::MAX, |(time, person, purchase)| {
                    person_runs[person as usize % SHARDS].push(person, time, purchase, window.start)
                });
                id_runs[shard].push(id, ingested, position, origin_seconds);
            })?;
            Ok::<_, anyhow::Error>((id_runs, person_runs))
        })
        .collect::<Result<_>>()?;

    let mut by_id = shards::<Vec<IdRun>>();
    let mut by_person = shards::<Vec<PersonRun>>();
    for (ids, people) in parsed {
        for (to, run) in by_id.iter_mut().zip(ids) {
            to.push(run);
        }
        for (to, run) in by_person.iter_mut().zip(people) {
            to.push(run);
        }
    }
    Ok(Candidates {
        compact_times: window
            .end
            .checked_sub(window.start)
            .and_then(|v| u32::try_from(v).ok())
            .is_some(),
        by_id,
        by_person,
        start: window.start,
        origin_seconds,
    })
}

// A bucket hit proves an ID is unique if no other row hit that bucket. Only
// repeated buckets need exact equality checks and earliest-ingest selection.
fn losing_positions(fragments: Vec<IdRun>, origin_seconds: i64) -> Vec<(usize, u32)> {
    let rows: usize = fragments.iter().map(|batch| batch.rows.len()).sum();
    let buckets = (8 * rows).next_power_of_two().max(64);
    let mut occupied = vec![0_u64; buckets / 64];
    let mut repeated = vec![0_u64; buckets / 64];
    let bucket = |id: u64| (id as usize / SHARDS) & (buckets - 1);
    for batch in &fragments {
        for row in &batch.rows {
            let slot = bucket(row.id);
            let bit = 1_u64 << (slot % 64);
            let seen = &mut occupied[slot / 64];
            repeated[slot / 64] |= *seen & bit;
            *seen |= bit;
        }
    }
    drop(occupied);

    let mut losers = Vec::new();
    let mut winners = AHashMap::<u64, (i64, usize, u32)>::new();
    for (chunk, batch) in fragments.iter().enumerate() {
        for row in &batch.rows {
            let slot = bucket(row.id);
            if repeated[slot / 64] & (1_u64 << (slot % 64)) != 0 {
                let current = (
                    batch.ingest_time(row.ingested, origin_seconds),
                    chunk,
                    row.position,
                );
                let (_, old_chunk, old_row) = match winners.entry(row.id) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(current);
                        continue;
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if current.0 < entry.get().0 {
                            entry.insert(current)
                        } else {
                            current
                        }
                    }
                };
                if old_row != u32::MAX {
                    losers.push((old_chunk, old_row));
                }
            }
        }
    }
    losers
}

pub fn dedup_and_sessionize(
    candidates: Candidates,
    width: usize,
) -> (Vec<crate::aggregate::SessionBatch>, usize) {
    let losses: Vec<_> = candidates
        .by_id
        .into_par_iter()
        .map(|shard| losing_positions(shard, candidates.origin_seconds))
        .collect();
    let mut by_person = shards::<Vec<(usize, usize)>>();
    for loss in losses {
        for (chunk, position) in loss {
            by_person[position as usize % SHARDS].push((chunk, position as usize / SHARDS));
        }
    }
    let events: Vec<_> = candidates
        .by_person
        .into_par_iter()
        .zip(by_person)
        .enumerate()
        .map(|(shard, (mut fragments, losers))| {
            let size: usize = fragments.iter().map(|run| run.rows.len()).sum();
            let length = size - losers.len();
            for (chunk, position) in losers {
                fragments[chunk].rows[position].person = INVALID_PERSON;
            }
            if candidates.compact_times && use_counting_sort(length, width) {
                // Count each user's offsets and purchases independently. Once converted to
                // prefix sums, these counters also serve as the next free positions.
                let mut ends = vec![(0u32, 0u32); width];
                for run in &fragments {
                    for row in &run.rows {
                        let (person, purchase) = match row.person {
                            INVALID_PERSON => continue,
                            EXTENDED => {
                                let (ev, p) = run.extra[row.time as usize];
                                (ev.person, p.is_some())
                            }
                            person => (person, false),
                        };
                        let counts = &mut ends[person as usize / SHARDS];
                        counts.0 += 1;
                        counts.1 += u32::from(purchase);
                    }
                }
                let mut totals = (0, 0);
                for counts in &mut ends {
                    let local = *counts;
                    *counts = totals;
                    totals.0 += local.0;
                    totals.1 += local.1;
                }
                let mut times = Vec::<u32>::with_capacity(totals.0 as usize);
                let mut sales = Vec::<(u32, Purchase)>::with_capacity(totals.1 as usize);
                let ts_dst = times.spare_capacity_mut();
                let sale_dst = sales.spare_capacity_mut();
                for run in fragments {
                    for row in run.rows {
                        if row.person == INVALID_PERSON {
                            continue;
                        }
                        let (person, time, p) = if row.person == EXTENDED {
                            let (e, p) = run.extra[row.time as usize];
                            (e.person, (e.time - candidates.start) as u32, p)
                        } else {
                            (row.person, row.time, None)
                        };
                        let heads = &mut ends[person as usize / SHARDS];
                        ts_dst[heads.0 as usize].write(time);
                        heads.0 += 1;
                        if let Some(p) = p {
                            sale_dst[heads.1 as usize].write((time, p));
                            heads.1 += 1;
                        }
                    }
                }
                // Both ranges are based on the same surviving records we just scattered.
                unsafe {
                    times.set_len(totals.0 as usize);
                    sales.set_len(totals.1 as usize);
                }
                return (
                    crate::aggregate::compact_sessions(
                        shard,
                        candidates.start,
                        &mut times,
                        &mut sales,
                        ends,
                    ),
                    length,
                );
            }
            let mut output = Batch {
                rows: Vec::with_capacity(length),
                purchases: Vec::new(),
            };
            for run in fragments {
                for row in run.rows {
                    let event = match row.person {
                        INVALID_PERSON => continue,
                        EXTENDED => {
                            let (mut e, purchase) = run.extra[row.time as usize];
                            e.purchase = output.add_purchase(purchase);
                            e
                        }
                        person => Event {
                            time: candidates.start + i64::from(row.time),
                            person,
                            purchase: 0,
                        },
                    };
                    output.rows.push(event);
                }
            }
            output.rows.par_sort_unstable_by_key(|e| (e.person, e.time));
            (crate::aggregate::sessionize(output), length)
        })
        .collect();
    let event_count = events.iter().map(|(_, count)| *count).sum();
    (
        events.into_iter().map(|(batch, _)| batch).collect(),
        event_count,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Click {
    pub time: i64,
    pub id: u64,
    pub person: u32,
    pub campaign: u32,
}

// Dense person IDs give a disjoint output range per person. For small or sparse
// partitions, comparison sorting avoids initializing a larger offsets table.
fn use_counting_sort(length: usize, width: usize) -> bool {
    (4096..=u32::MAX as usize).contains(&length) && width <= length.saturating_mul(2)
}

fn prefixes(offsets: &mut [u32]) {
    let mut total = 0;
    for offset in offsets {
        let size = *offset;
        *offset = total;
        total += size;
    }
}

fn sort_times<T: Send, F: Fn(&T) -> i64 + Sync>(rows: &mut [T], ends: Vec<u32>, time: F) {
    let mut start = 0;
    for end in ends {
        let end = end as usize;
        if end - start > 32768 {
            rows[start..end].par_sort_unstable_by_key(&time);
        } else if end - start > 1 {
            rows[start..end].sort_unstable_by_key(&time);
        }
        start = end;
    }
}

fn order_clicks(pieces: Vec<Vec<Click>>, width: usize) -> Vec<Click> {
    let length = pieces.iter().map(Vec::len).sum();
    let mut clicks = Vec::with_capacity(length);
    if !use_counting_sort(length, width) {
        for piece in pieces {
            clicks.extend(piece);
        }
        clicks.par_sort_unstable_by_key(|c| (c.person, c.time));
        return clicks;
    }
    let mut ends = vec![0u32; width];
    for c in pieces.iter().flatten() {
        ends[c.person as usize / SHARDS] += 1;
    }
    prefixes(&mut ends);
    let storage = clicks.spare_capacity_mut();
    for c in pieces.into_iter().flatten() {
        let head = &mut ends[c.person as usize / SHARDS];
        storage[*head as usize].write(c);
        *head += 1;
    }
    // Every click fills one distinct slot in its person's range.
    unsafe {
        clicks.set_len(length);
    }
    sort_times(&mut clicks, ends, |c| c.time);
    clicks
}

pub fn read_clicks(
    data: &Path,
    window: Window,
    dimensions: &Dimensions,
    users: &Names,
    aliases: &Aliases,
) -> Result<(Vec<Vec<Click>>, ClickIds)> {
    let ids = Names::default();
    let calendar = input::Calendar::new(window);
    let pieces = chunks(&data.join("clicks"))?
        .into_par_iter()
        .map_init(PersonCache::default, |people, chunk| {
            let mut local = shards::<Vec<Click>>();
            chunk.scan_parsed(|[id, user, campaign, ts]| {
                if id.is_empty() || user.is_empty() {
                    return;
                }
                let proposal = people.prepare(users, aliases, user);
                let Some(&campaign) = dimensions.campaign_ids.get(text(campaign)) else {
                    return;
                };
                let Some(time) = calendar.parse(ts) else {
                    return;
                };
                if time < window.start - LOOKBACK || time >= window.end {
                    return;
                }
                let person = people.prepared(proposal, users, aliases, user);
                let id = id_for(id, b'k', &ids);
                local[person as usize % SHARDS].push(Click {
                    time,
                    id,
                    person,
                    campaign,
                });
            })?;
            Ok::<_, anyhow::Error>(local)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut partitions = shards::<Vec<Vec<Click>>>();
    for chunk in pieces {
        for (partition, rows) in partitions.iter_mut().zip(chunk) {
            if !rows.is_empty() {
                partition.push(rows);
            }
        }
    }
    let width = users.person_slots_per_shard();
    let clicks = partitions
        .into_par_iter()
        .map(|p| order_clicks(p, width))
        .collect();
    Ok((clicks, ClickIds(ids.into_names())))
}

#[cfg(test)]
mod id_tests {
    use super::*;

    #[test]
    fn click_ids_distinguish_padding_and_preserve_mixed_byte_order() {
        let cases: &[&[u8]] = &[
            b"k",
            b"k\0",
            b"k\0a",
            b"ka",
            b"ka\0",
            b"ka\0b",
            b"k12345678",
            b"k123456789",
            b"K12",
            b"kx\xc3\xa9",
            "k用户".as_bytes(),
        ];
        let fallback = Names::default();
        let codes: Vec<_> = cases
            .iter()
            .map(|value| id_for(value, b'k', &fallback))
            .collect();
        let ids = ClickIds(fallback.into_names());
        for (i, a) in cases.iter().enumerate() {
            for (j, b) in cases.iter().enumerate() {
                assert_eq!(ids.compare(codes[i], codes[j]), a.cmp(b));
                assert_eq!(codes[i] == codes[j], a == b);
            }
        }
    }
}

#[cfg(test)]
mod grouping_tests {
    use super::*;

    #[test]
    fn large_and_sparse_partitions_keep_all_clicks_in_time_order() {
        for (count, width) in [(17, 29), (12000, 29), (12000, 40000)] {
            let mut pieces = vec![Vec::new(); 7];
            let mut expected = Vec::new();
            for i in 0..count {
                let person = ((i * 13 % 29) * SHARDS + 7) as u32;
                let time = ((i * 23 % 41) as i64) - 23;
                let id = i as u64;
                let campaign = (i % 7) as u32;
                pieces[i % 7].push(Click {
                    person,
                    time,
                    id,
                    campaign,
                });
                expected.push((person, time, id, campaign));
            }
            let sorted = order_clicks(pieces, width);
            assert!(
                sorted
                    .windows(2)
                    .all(|v| (v[0].person, v[0].time) <= (v[1].person, v[1].time))
            );
            let mut actual: Vec<_> = sorted
                .iter()
                .map(|c| (c.person, c.time, c.id, c.campaign))
                .collect();
            expected.sort_unstable();
            actual.sort_unstable();
            assert_eq!(actual, expected);
        }
    }
}
