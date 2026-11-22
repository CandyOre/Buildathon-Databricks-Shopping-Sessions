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
const INVALID_PERSON: u16 = u16::MAX;

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

const EXTENDED: u16 = u16::MAX - 1;

// The shard owns seven low ID bits. The remaining 57-bit quotient and a
// 39-bit location/ingestion code fit in three naturally aligned u32 words.
const ID_SHARD_BITS: u32 = SHARDS.trailing_zeros();
const QUOTIENT_HIGH_BITS: u32 = 32 - ID_SHARD_BITS;
const QUOTIENT_HIGH_MASK: u32 = (1 << QUOTIENT_HIGH_BITS) - 1;
const POSITION_BITS: u32 = 18;
const POSITION_MASK: u32 = (1 << POSITION_BITS) - 1;
const EXCEPTIONAL: u64 = (1 << (32 + ID_SHARD_BITS)) - (1 << 22);
const _: () = assert!(SHARDS == 128);

#[repr(C)]
#[derive(Clone, Copy)]
struct EntryId {
    low: u32,
    high_code: u32,
    code: u32,
}

impl EntryId {
    #[inline]
    fn quotient(&self) -> u64 {
        (u64::from(self.high_code & QUOTIENT_HIGH_MASK) << 32) | u64::from(self.low)
    }

    #[inline]
    fn code(&self) -> u64 {
        (u64::from(self.high_code >> QUOTIENT_HIGH_BITS) << 32) | u64::from(self.code)
    }
}
const _: () = assert!(std::mem::size_of::<EntryId>() == 12);

// A chunk owns one backing allocation for the common prefixes of all shards.
// Only a shard that outgrows its prefix allocates a separate continuation.
// T is Copy, so uninitialized padding never contains values requiring Drop.
struct ShardSlab<T: Copy> {
    slots: Vec<std::mem::MaybeUninit<T>>,
    stride: usize,
    used: [usize; SHARDS],
    overflow: [Vec<T>; SHARDS],
}

impl<T: Copy> ShardSlab<T> {
    fn new(stride: usize) -> Self {
        // A zero capacity hint is valid for an empty input. chunks_exact_mut
        // still needs a positive stride to produce one slice per shard.
        let stride = stride.max(1);
        let capacity = stride
            .checked_mul(SHARDS)
            .expect("shard slab capacity overflow");
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, std::mem::MaybeUninit::uninit);
        Self {
            slots,
            stride,
            used: [0; SHARDS],
            overflow: std::array::from_fn(|_| Vec::new()),
        }
    }

    #[inline]
    fn push(&mut self, shard: usize, row: T) -> u32 {
        let index = self.used[shard];
        if index < self.stride {
            self.slots[shard * self.stride + index].write(row);
        } else {
            self.overflow[shard].push(row);
        }
        self.used[shard] += 1;
        index as u32
    }

    fn fragment(&self, shard: usize) -> (&[T], &[T]) {
        let start = shard * self.stride;
        let count = self.used[shard].min(self.stride);
        // push writes each lane before advancing used. Padding is never read.
        let raw = &self.slots[start..start + count];
        let prefix = unsafe { std::slice::from_raw_parts(raw.as_ptr().cast::<T>(), raw.len()) };
        (prefix, &self.overflow[shard])
    }

    fn fragments_mut(&mut self) -> impl Iterator<Item = (&mut [T], &mut [T])> {
        let stride = self.stride;
        self.slots
            .chunks_exact_mut(stride)
            .zip(self.overflow.iter_mut())
            .zip(&self.used)
            .map(move |((slots, overflow), &count)| {
                let count = count.min(stride);
                // Every yielded slice belongs to a different shard. Its prefix
                // is initialized by push; its continuation is an ordinary Vec.
                let prefix = unsafe {
                    std::slice::from_raw_parts_mut(slots.as_mut_ptr().cast::<T>(), count)
                };
                (prefix, overflow.as_mut_slice())
            })
    }
}

struct ChunkIds {
    rows: ShardSlab<EntryId>,
    exceptional: Vec<(i64, u32)>,
}

impl ChunkIds {
    fn push(&mut self, id: u64, ingested: i64, position: u32, origin_seconds: i64) {
        let seconds = (ingested.div_euclid(1000) - origin_seconds) as u64;
        let code = if seconds < (EXCEPTIONAL >> POSITION_BITS)
            && (position < POSITION_MASK || position == u32::MAX)
        {
            (seconds << POSITION_BITS) | u64::from(position & POSITION_MASK)
        } else {
            // A chunk owns only lines starting within its 4 MiB byte range.
            // This leaves room for every row to require a full-width entry.
            let index = self.exceptional.len();
            assert!(index < 1 << 22);
            self.exceptional.push((ingested, position));
            EXCEPTIONAL + index as u64
        };
        let quotient = id >> ID_SHARD_BITS;
        let high_code = (quotient >> 32) as u32 | ((code >> 32) as u32) << QUOTIENT_HIGH_BITS;
        self.rows.push(
            id as usize % SHARDS,
            EntryId {
                low: quotient as u32,
                high_code,
                code: code as u32,
            },
        );
    }

    fn run(&self, shard: usize) -> IdRun<'_> {
        let (prefix, overflow) = self.rows.fragment(shard);
        IdRun {
            prefix,
            overflow,
            exceptional: &self.exceptional,
        }
    }
}

struct IdRun<'a> {
    prefix: &'a [EntryId],
    overflow: &'a [EntryId],
    exceptional: &'a [(i64, u32)],
}

impl IdRun<'_> {
    fn rows(&self) -> impl Iterator<Item = &EntryId> {
        self.prefix.iter().chain(self.overflow)
    }

    #[inline]
    fn winner(&self, row: &EntryId, origin_seconds: i64, chunk: usize) -> (i64, usize, u32) {
        let code = row.code();
        let (ingest, position) = if code < EXCEPTIONAL {
            let seconds = (code >> POSITION_BITS) as i64;
            let short = code as u32 & POSITION_MASK;
            (
                (origin_seconds + seconds) * 1000,
                if short == POSITION_MASK {
                    u32::MAX
                } else {
                    short
                },
            )
        } else {
            self.exceptional[(code - EXCEPTIONAL) as usize]
        };
        (ingest, chunk, position)
    }
}

// The slab's shard supplies the low person-ID bits. The two reserved local
// values represent a removed row or an index into the full-width side table.
#[repr(C)]
#[derive(Clone, Copy)]
struct PersonEvent {
    low: u16,
    high: u16,
    local: u16,
}

impl PersonEvent {
    fn new(local: u16, value: u32) -> Self {
        Self {
            low: value as u16,
            high: (value >> 16) as u16,
            local,
        }
    }

    #[inline]
    fn value(&self) -> u32 {
        u32::from(self.low) | (u32::from(self.high) << 16)
    }
}
const _: () = assert!(std::mem::size_of::<PersonEvent>() == 6);

struct ChunkPeople {
    rows: ShardSlab<PersonEvent>,
    extra: Vec<(Event, Option<Purchase>)>,
}

impl ChunkPeople {
    fn push(&mut self, person: u32, time: i64, purchase: Option<Purchase>, start: i64) -> u32 {
        let offset = time.wrapping_sub(start) as u64;
        let local = person / SHARDS as u32;
        let row =
            if local < u32::from(EXTENDED) && offset <= u64::from(u32::MAX) && purchase.is_none() {
                PersonEvent::new(local as u16, offset as u32)
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
                PersonEvent::new(EXTENDED, index)
            };
        let shard = person as usize % SHARDS;
        // Chunk::scan owns only records starting in its 4 MiB byte range.
        // Even if a final record crosses many chunks, this index is below
        // 2^22, so the encoded position is below 2^29 for SHARDS = 128.
        self.rows.push(shard, row) * SHARDS as u32 + shard as u32
    }
}

struct PersonRun<'a> {
    prefix: &'a mut [PersonEvent],
    overflow: &'a mut [PersonEvent],
    extra: &'a [(Event, Option<Purchase>)],
}

impl PersonRun<'_> {
    fn rows(&self) -> impl Iterator<Item = &PersonEvent> {
        self.prefix.iter().chain(self.overflow.iter())
    }

    fn at(&mut self, index: usize) -> &mut PersonEvent {
        if index < self.prefix.len() {
            &mut self.prefix[index]
        } else {
            &mut self.overflow[index - self.prefix.len()]
        }
    }
}

pub struct Candidates {
    compact_times: bool,
    blocks: Vec<(ChunkIds, ChunkPeople)>,
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

#[cfg(feature = "large-input")]
const PENDING_LOOKUPS: usize = 16;

#[cfg(feature = "large-input")]
struct WaitingEvent {
    query: input::Prepared,
    event_id: u64,
    ingest: i64,
    time: i64,
    purchase: Option<Purchase>,
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
            // Approximate only the backing capacity. Arbitrary CSV lengths and
            // shard skew are handled by the exact per-shard continuation.
            let stride = chunk.byte_len().div_ceil(70 * SHARDS) + 8;
            let mut id_rows = ChunkIds {
                rows: ShardSlab::new(stride),
                exceptional: Vec::new(),
            };
            let mut people_rows = ChunkPeople {
                rows: ShardSlab::new(stride),
                extra: Vec::new(),
            };
            #[cfg(feature = "large-input")]
            let mut next = 0;
            #[cfg(feature = "large-input")]
            let mut waiting = Vec::<WaitingEvent>::with_capacity(PENDING_LOOKUPS);
            #[cfg(feature = "large-input")]
            let commit = |job: WaitingEvent,
                          people: &mut PersonCache,
                          people_rows: &mut ChunkPeople,
                          id_rows: &mut ChunkIds| {
                let person = people.prepared(job.query, users, aliases, &[]);
                let position = people_rows.push(person, job.time, job.purchase, window.start);
                id_rows.push(job.event_id, job.ingest, position, origin_seconds);
            };
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
                #[allow(unused_mut)]
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
                                .lookup(product)
                                .unwrap_or(dimensions.unknown_category);
                            Some(Purchase {
                                category,
                                cents: cents as u32,
                            })
                        }
                        _ => return None,
                    };
                    #[cfg(not(feature = "large-input"))]
                    {
                        Some((
                            time,
                            people.prepared(proposal, users, aliases, user),
                            purchase,
                        ))
                    }
                    #[cfg(feature = "large-input")]
                    {
                        Some((time, purchase))
                    }
                };
                // Invalid candidates still compete in deduplication. The earliest copy wins.
                #[cfg(not(feature = "large-input"))]
                let position = valid().map_or(u32::MAX, |(time, person, purchase)| {
                    people_rows.push(person, time, purchase, window.start)
                });
                #[cfg(feature = "large-input")]
                let position = if let Some((time, purchase)) = valid() {
                    if proposal.needs_lookup() {
                        let job = WaitingEvent {
                            query: proposal,
                            event_id: id,
                            ingest: ingested,
                            time,
                            purchase,
                        };
                        if waiting.len() == PENDING_LOOKUPS {
                            let ready = std::mem::replace(&mut waiting[next], job);
                            next = (next + 1) % PENDING_LOOKUPS;
                            commit(ready, people, &mut people_rows, &mut id_rows);
                        } else {
                            waiting.push(job)
                        }
                        return;
                    }
                    people_rows.push(
                        people.prepared(proposal, users, aliases, user),
                        time,
                        purchase,
                        window.start,
                    )
                } else {
                    u32::MAX
                };
                id_rows.push(id, ingested, position, origin_seconds);
            })?;
            #[cfg(feature = "large-input")]
            for job in waiting {
                commit(job, people, &mut people_rows, &mut id_rows)
            }
            Ok::<_, anyhow::Error>((id_rows, people_rows))
        })
        .collect::<Result<_>>()?;

    Ok(Candidates {
        compact_times: window
            .end
            .checked_sub(window.start)
            .and_then(|v| u32::try_from(v).ok())
            .is_some(),
        blocks: parsed,
        start: window.start,
        origin_seconds,
    })
}

// A bucket hit proves an ID is unique if no other row hit that bucket. Only
// repeated buckets need exact equality checks and earliest-ingest selection.
fn losing_positions(fragments: Vec<IdRun<'_>>, origin_seconds: i64) -> Vec<(usize, u32)> {
    let rows: usize = fragments
        .iter()
        .map(|batch| batch.prefix.len() + batch.overflow.len())
        .sum();
    let buckets = (8 * rows).next_power_of_two().max(64);
    let mut occupied = vec![0_u64; buckets / 64];
    let mut repeated = vec![0_u64; buckets / 64];
    let bucket = |row: &EntryId| row.low as usize & (buckets - 1);
    for batch in &fragments {
        for row in batch.rows() {
            let slot = bucket(row);
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
        for row in batch.rows() {
            let slot = bucket(row);
            if repeated[slot / 64] & (1_u64 << (slot % 64)) != 0 {
                let current = batch.winner(row, origin_seconds, chunk);
                let (_, old_chunk, old_row) = match winners.entry(row.quotient()) {
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
    let losses: Vec<_> = (0..SHARDS)
        .into_par_iter()
        .map(|shard| {
            let runs = candidates
                .blocks
                .iter()
                .map(|(ids, _)| ids.run(shard))
                .collect();
            losing_positions(runs, candidates.origin_seconds)
        })
        .collect();
    let mut people: Vec<_> = candidates
        .blocks
        .into_iter()
        .map(|(_, rows)| rows)
        .collect();
    let mut person_runs = shards::<Vec<PersonRun<'_>>>();
    for block in &mut people {
        let extra = block.extra.as_slice();
        for (to, (prefix, overflow)) in person_runs.iter_mut().zip(block.rows.fragments_mut()) {
            to.push(PersonRun {
                prefix,
                overflow,
                extra,
            });
        }
    }
    let mut by_person = shards::<Vec<(usize, usize)>>();
    for loss in losses {
        for (chunk, position) in loss {
            by_person[position as usize % SHARDS].push((chunk, position as usize / SHARDS));
        }
    }
    let events: Vec<_> = person_runs
        .into_par_iter()
        .zip(by_person)
        .enumerate()
        .map(|(shard, (mut fragments, losers))| {
            let size: usize = fragments
                .iter()
                .map(|run| run.prefix.len() + run.overflow.len())
                .sum();
            let length = size - losers.len();
            for (chunk, position) in losers {
                fragments[chunk].at(position).local = INVALID_PERSON;
            }
            if candidates.compact_times && use_counting_sort(length, width) {
                // Count each user's offsets and purchases independently. Once converted to
                // prefix sums, these counters also serve as the next free positions.
                let mut ends = vec![(0u32, 0u32); width];
                for run in &fragments {
                    for row in run.rows() {
                        let (local, purchase) = match row.local {
                            INVALID_PERSON => continue,
                            EXTENDED => {
                                let (ev, p) = run.extra[row.value() as usize];
                                (ev.person as usize / SHARDS, p.is_some())
                            }
                            local => (usize::from(local), false),
                        };
                        let counts = &mut ends[local];
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
                    for row in run.rows() {
                        if row.local == INVALID_PERSON {
                            continue;
                        }
                        let (local, time, p) = if row.local == EXTENDED {
                            let (e, p) = run.extra[row.value() as usize];
                            (
                                e.person as usize / SHARDS,
                                (e.time - candidates.start) as u32,
                                p,
                            )
                        } else {
                            (usize::from(row.local), row.value(), None)
                        };
                        let heads = &mut ends[local];
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
                for row in run.rows() {
                    let event = match row.local {
                        INVALID_PERSON => continue,
                        EXTENDED => {
                            let (mut e, purchase) = run.extra[row.value() as usize];
                            e.purchase = output.add_purchase(purchase);
                            e
                        }
                        local => Event {
                            time: candidates.start + i64::from(row.value()),
                            person: u32::from(local) * SHARDS as u32 + shard as u32,
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
                let Some(campaign) = dimensions.campaign_ids.lookup(campaign) else {
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
#[cfg(test)]
mod shard_slab_tests {
    use super::*;

    #[test]
    fn six_byte_rows_reserve_both_markers_and_preserve_full_width_exceptions() {
        let start = -1701;
        let shard = SHARDS as u32 - 1;
        let direct = (u32::from(EXTENDED) - 1) * SHARDS as u32 + shard;
        let mut people = ChunkPeople {
            rows: ShardSlab::new(1),
            extra: Vec::new(),
        };
        let max_time = start + i64::from(u32::MAX);
        let pos = people.push(direct, max_time, None, start);
        assert_eq!(pos, shard);
        let row = people.rows.fragment(shard as usize).0[0];
        assert_eq!(row.local, EXTENDED - 1);
        assert_eq!(row.value(), u32::MAX);
        for (person, time, sale) in [
            (direct + SHARDS as u32, start + 65535, None),
            (direct + 2 * SHARDS as u32, start + 65536, None),
            (u32::MAX, max_time + 1001, None),
            (direct, start - 1, None),
            (
                direct,
                start,
                Some(Purchase {
                    category: 9,
                    cents: 27,
                }),
            ),
        ] {
            let pos = people.push(person, time, sale, start);
            let (_, spill) = people.rows.fragment(shard as usize);
            let row = spill[(pos / SHARDS as u32 - 1) as usize];
            assert_eq!(row.local, EXTENDED);
            let (ev, stored) = people.extra[row.value() as usize];
            assert_eq!((ev.person, ev.time), (person, time));
            assert_eq!(
                stored.map(|p| (p.category, p.cents)),
                sale.map(|p| (p.category, p.cents))
            );
        }
        for value in [0, 1, 65535, 65536, 65537, u32::MAX] {
            assert_eq!(PersonEvent::new(EXTENDED, value).value(), value);
        }
    }

    #[test]
    fn dense_and_full_sort_recover_nonzero_shard_at_u16_person_boundary() {
        let shard = 91;
        let start = -1000;
        let locals = [15, 65533, 65534, 65535, 65536];
        let run = |compact_times| {
            let mut ids = ChunkIds {
                rows: ShardSlab::new(1),
                exceptional: Vec::new(),
            };
            let mut people = ChunkPeople {
                rows: ShardSlab::new(1),
                extra: Vec::new(),
            };
            let times = [0, 65535, 65536, 16_777_216, u32::MAX - 1];
            for i in 0..33_000 {
                let person = locals[i % locals.len()] * SHARDS as u32 + shard as u32;
                let time = start + i64::from(times[(i / locals.len()) % times.len()]);
                let sale = (i % 29 == 0).then_some(Purchase {
                    category: (i % 3) as u32,
                    cents: 23,
                });
                let pos = people.push(person, time, sale, start);
                ids.push(i as u64, 2000, pos, 0);
            }
            // Remove one ordinary and one extended row through the dedup path.
            for id in [1, 2] {
                ids.push(id, 1000, u32::MAX, 0);
            }
            let batch = Candidates {
                compact_times,
                blocks: vec![(ids, people)],
                start,
                origin_seconds: 0,
            };
            dedup_and_sessionize(batch, 65537)
        };
        let (dense, n) = run(true);
        let (sorted, m) = run(false);
        assert_eq!((n, m), (32998, 32998));
        let (a, b) = (&dense[shard].daily, &sorted[shard].daily);
        let mut seen = a.person.clone();
        seen.dedup();
        assert_eq!(seen, locals.map(|p| p * SHARDS as u32 + shard as u32));
        assert_eq!(a.person, b.person);
        assert_eq!(a.day, b.day);
        assert_eq!(a.events, b.events);
        assert_eq!(a.sessions, b.sessions);
        assert_eq!(a.revenue, b.revenue);
        assert_eq!(a.category, b.category);
    }

    #[test]
    fn an_empty_slab_with_zero_hint_exposes_every_disjoint_shard() {
        let mut slab = ShardSlab::<u32>::new(0);
        assert_eq!(
            slab.fragments_mut()
                .filter(|(a, b)| a.is_empty() && b.is_empty())
                .count(),
            SHARDS
        );
        for i in 0..2048 {
            slab.push(SHARDS - 1, i);
        }
        for (shard, (prefix, overflow)) in slab.fragments_mut().enumerate() {
            if shard + 1 < SHARDS {
                assert!(prefix.is_empty() && overflow.is_empty());
            } else {
                assert_eq!(prefix.len(), 1);
                assert_eq!(prefix[0], 0);
                assert!(overflow.iter().copied().eq(1..2048));
            }
        }
    }

    #[test]
    fn a_skewed_chunk_keeps_offsets_across_both_sides_of_the_spill() {
        let mut slab = ShardSlab::new(3);
        let mut expected = vec![Vec::new(); SHARDS];
        for i in 0..1000usize {
            let shard = if i % 5 == 0 { i % SHARDS } else { 7 };
            assert_eq!(slab.push(shard, i), expected[shard].len() as u32);
            expected[shard].push(i);
        }
        for (shard, (prefix, overflow)) in slab.fragments_mut().enumerate() {
            assert_eq!(prefix.len(), expected[shard].len().min(3));
            for (value, &original) in prefix.iter_mut().chain(overflow).zip(&expected[shard]) {
                assert_eq!(*value, original);
                *value += 1;
            }
        }
        for (shard, values) in expected.iter().enumerate() {
            let (prefix, overflow) = slab.fragment(shard);
            assert_eq!(
                prefix.iter().chain(overflow).copied().collect::<Vec<_>>(),
                values.iter().map(|value| value + 1).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn earliest_invalid_can_remove_rows_in_a_different_chunks_overflow() {
        let mut blocks: Vec<_> = (0..2)
            .map(|_| ChunkIds {
                rows: ShardSlab::new(1),
                exceptional: Vec::new(),
            })
            .collect();
        let shard = 7;
        let a = SHARDS as u64 + shard as u64;
        let b = 2 * SHARDS as u64 + shard as u64;
        blocks[0].push(a, 90_000, 21, 0);
        blocks[0].push(b, 91_000, 22, 0);
        blocks[0].push(a, 89_000, 23, 0);
        blocks[1].push(b, -10_000, u32::MAX, 0);
        blocks[1].push(a, 88_000, 24, 0);
        let mut losers = losing_positions(blocks.iter().map(|c| c.run(shard)).collect(), 0);
        losers.sort_unstable();
        assert_eq!(losers, vec![(0, 21), (0, 22), (0, 23)]);
    }
}

#[cfg(test)]
mod packed_id_tests {
    use super::*;

    #[test]
    fn quotient_and_location_round_trip_across_all_encoding_boundaries() {
        let origin = -86_400i64;
        let inline_limit = (EXCEPTIONAL >> POSITION_BITS) as i64;
        let keys = [0, 1, 127, 128, (1u64 << 39) - 1, 1 << 39, 1 << 63, u64::MAX];
        for id in keys {
            let mut block = ChunkIds {
                rows: ShardSlab::new(1),
                exceptional: Vec::new(),
            };
            let mut expected = Vec::new();
            for offset in [
                -1,
                0,
                1,
                1 << 14,
                inline_limit - 1,
                inline_limit,
                3_000_000_000,
            ] {
                for pos in [
                    0,
                    POSITION_MASK - 1,
                    POSITION_MASK,
                    POSITION_MASK + 1,
                    u32::MAX - 1,
                    u32::MAX,
                ] {
                    let time = (origin + offset) * 1000;
                    block.push(id, time, pos, origin);
                    expected.push((time, 19, pos));
                }
            }
            let shard = id as usize % SHARDS;
            let run = block.run(shard);
            for (packed, &original) in run.rows().zip(&expected) {
                assert_eq!((packed.quotient() << ID_SHARD_BITS) | shard as u64, id);
                assert_eq!(run.winner(packed, origin, 19), original);
            }
            assert_eq!(run.rows().count(), expected.len());
        }
    }

    #[test]
    fn equal_low_words_distinct_quotients_ties_and_invalid_winners_match_full_ids() {
        let shard = 101usize;
        let mut blocks: Vec<_> = (0..5)
            .map(|_| ChunkIds {
                rows: ShardSlab::new(2),
                exceptional: Vec::new(),
            })
            .collect();
        let mut flat: Vec<Vec<(u64, i64, u32)>> = (0..blocks.len()).map(|_| Vec::new()).collect();
        for i in 0..1900usize {
            let chunk = i % blocks.len();
            let high = (i * 193 % 103) as u64;
            let quotient = (high << 32) | ((i / 3 % 4) as u64);
            let id = (quotient << ID_SHARD_BITS) | shard as u64;
            let time = match i % 13 {
                0 => -9_000,
                1 => 3_000_000_000_000,
                _ => ((i * 11 % 3) * 1000) as i64,
            };
            let position = match i % 17 {
                0 => u32::MAX,
                1 => POSITION_MASK + i as u32,
                _ => i as u32,
            };
            blocks[chunk].push(id, time, position, 0);
            flat[chunk].push((id, time, position));
        }
        let mut exact = AHashMap::<u64, (i64, usize, u32)>::default();
        let mut expected = Vec::new();
        for (chunk, rows) in flat.iter().enumerate() {
            for &(id, time, pos) in rows {
                let row = (time, chunk, pos);
                if let Some(winner) = exact.get_mut(&id) {
                    let lost = if time < winner.0 {
                        std::mem::replace(winner, row)
                    } else {
                        row
                    };
                    if lost.2 != u32::MAX {
                        expected.push((lost.1, lost.2));
                    }
                } else {
                    exact.insert(id, row);
                }
            }
        }
        let actual = losing_positions(blocks.iter().map(|b| b.run(shard)).collect(), 0);
        assert_eq!(actual, expected);
    }
}
