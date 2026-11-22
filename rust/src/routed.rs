//! Resolve identity once per raw spelling, after event-ID winner selection.
//! A connected component can have aliases in several raw partitions, so every
//! member of such a component (including its representative) is forwarded.
//!
//! `PERF_ROUTE_WRITES=direct|buffered|stream` selects the full-record Vec
//! writer. To time that choice across all event candidates, use
//! `PERF_ROUTE_PACK=0 PERF_ROUTE_SLAB=0`. A slab writes its own fixed
//! prefixes directly and sends full-record overflow to the selected writer.
//! Packed records use a separate 16-byte lane and also write directly.
//! `PERF_ROUTE_ARENA` controls later page-backed scans and is independent
//! of the reader's writes.
mod packed;
mod storage;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::AHashMap;
use anyhow::{Result, ensure};
use rayon::prelude::*;

use crate::aggregate::{self, SessionBatch};
use crate::csv::{Chunk, chunks};
use crate::input::{self, Aliases, Calendar, Dimensions, Names, PersonCache, PersonNames, Window};
use crate::records::{Batch, Click, ClickIds, ClickKeyChunk, Event, Purchase};

const WIDE: u64 = 1 << 63;
const OTHER: u64 = 3 << 62;
const SIDE: u32 = 1 << 31;
const INVALID: u32 = u32::MAX;
const HASH_MASK: u64 = (1 << 30) - 1;

fn mix(word: u64) -> u32 {
    let v = (word ^ (word >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    ((v ^ (v >> 32)) & HASH_MASK) as u32
}

#[inline]
fn hash(handle: u64) -> u32 {
    if (WIDE..OTHER).contains(&handle) {
        ((handle >> 32) & HASH_MASK) as u32
    } else {
        mix(handle)
    }
}

// Compact handles contain the actual suffix. A wide handle addresses its
// chunk-local, full 128-bit key; the upper 30 hash bits are only a routing hint.
#[inline]
fn encode(
    bytes: &[u8],
    prefix: u8,
    wide: &mut Vec<[u64; 2]>,
    fallback: &Names,
    numeric: bool,
) -> u64 {
    if bytes.first() == Some(&prefix) {
        let tail = &bytes[1..];
        if numeric && tail.len() <= 8 {
            if let Some(n) = packed::number(tail) {
                return n;
            }
        } else if tail.len() <= 8
            && tail.first().copied().unwrap_or(0) < 128
            && tail.last() != Some(&0)
        {
            let word = if let Ok(full) = <&[u8; 8]>::try_from(tail) {
                u64::from_be_bytes(*full)
            } else {
                let mut padded = [0u8; 8];
                padded[..tail.len()].copy_from_slice(tail);
                u64::from_be_bytes(padded)
            };
            return word;
        }
        if let Some(key) = input::hex_id(bytes, prefix) {
            let slot = u32::try_from(wide.len()).expect("too many keys in one input chunk");
            wide.push(key);
            let fingerprint = mix(key[0] ^ key[1].rotate_left(23));
            return WIDE | (u64::from(fingerprint) << 32) | u64::from(slot);
        }
    }
    OTHER | u64::from(fallback.intern(input::text(bytes)))
}

fn user_bytes<'a>(
    handle: u64,
    wide: &[[u64; 2]],
    other: &'a [Vec<Arc<str>>],
    buffer: &'a mut [u8; 33],
    numeric: bool,
) -> &'a [u8] {
    if handle >= OTHER {
        return input::name_for(other, handle as u32).as_bytes();
    }
    buffer[0] = b'u';
    if handle < WIDE {
        if numeric {
            let len = packed::write_number(handle, (&mut buffer[1..9]).try_into().unwrap());
            return &buffer[..len + 1];
        }
        buffer[1..9].copy_from_slice(&handle.to_be_bytes());
        return &buffer[..9 - (handle.trailing_zeros() / 8) as usize];
    }
    let [hi, lo] = wide[handle as u32 as usize];
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (dst, byte) in buffer[1..]
        .chunks_exact_mut(2)
        .zip(hi.to_be_bytes().into_iter().chain(lo.to_be_bytes()))
    {
        dst[0] = HEX[(byte >> 4) as usize];
        dst[1] = HEX[(byte & 15) as usize];
    }
    buffer
}

#[derive(Clone, Copy)]
struct EventRow {
    id: u64,
    user: u64,
    stamp: u32,
    ingest: u32,
}
const _: () = assert!(std::mem::size_of::<EventRow>() == 24);

#[derive(Default)]
struct EventKeys {
    wide: Vec<[u64; 2]>,
    ingests: Vec<i64>,
    stripe: Option<EventStripe>,
}

enum EventStripe {
    Full(storage::Stripe<EventRow>),
    Short(storage::Stripe<packed::ShortEvent>),
}

impl EventKeys {
    fn prefixes(&self,part:usize)->(&[EventRow],&[packed::ShortEvent]) {
        match &self.stripe {
            None => (&[],&[]),
            Some(EventStripe::Full(s)) => (s.prefix(part),&[]),
            Some(EventStripe::Short(s)) => (&[],s.prefix(part)),
        }
    }
}

#[derive(Default)]
struct EventData {
    users: Vec<[u64; 2]>,
    extra: Vec<(i64, Option<Purchase>)>,
}

impl EventData {
    fn stamp(&mut self, start: i64, time: i64, sale: Option<Purchase>) -> u32 {
        let offset = (time - start) as u64;
        if sale.is_none() && offset < u64::from(SIDE) {
            return offset as u32;
        }
        let slot = self.extra.len();
        assert!(slot < (SIDE - 1) as usize);
        self.extra.push((time, sale));
        SIDE | slot as u32
    }

    #[inline]
    fn decode(&self, start: i64, stamp: u32) -> (i64, Option<Purchase>) {
        if stamp < SIDE {
            (start + i64::from(stamp), None)
        } else {
            self.extra[(stamp ^ SIDE) as usize]
        }
    }
}

impl EventKeys {
    fn ingest(&mut self, base: i64, time: i64) -> u32 {
        let seconds = (time.div_euclid(1000) - base) as u64;
        if seconds < u64::from(SIDE) {
            return seconds as u32;
        }
        let slot = self.ingests.len();
        assert!(slot < SIDE as usize);
        self.ingests.push(time);
        SIDE | slot as u32
    }

    fn millis(&self, base: i64, ingest: u32) -> i64 {
        if ingest < SIDE {
            (base + i64::from(ingest)) * 1000
        } else {
            self.ingests[(ingest ^ SIDE) as usize]
        }
    }
}

fn empty_buckets<T>(n: usize) -> Vec<Vec<T>> {
    (0..n).map(|_| Vec::new()).collect()
}

#[derive(Clone, Copy, PartialEq)]
enum Writes {
    Direct,
    Buffered,
    Stream,
}

struct BucketWriter {
    output: Vec<Vec<EventRow>>,
    staging: Vec<[std::mem::MaybeUninit<EventRow>; 8]>,
    used: Vec<u8>,
    mode: Writes,
}

impl BucketWriter {
    fn new(n: usize, mode: Writes) -> Self {
        let staging = if mode == Writes::Direct { 0 } else { n };
        Self {
            output: empty_buckets(n),
            staging: vec![[std::mem::MaybeUninit::uninit(); 8]; staging],
            used: vec![0; staging],
            mode,
        }
    }

    #[inline]
    fn push(&mut self, part: usize, row: EventRow) {
        if self.mode == Writes::Direct {
            self.output[part].push(row);
            return;
        }
        let n = self.used[part] as usize;
        self.staging[part][n].write(row);
        self.used[part] = (n + 1) as u8;
        if n == 7 {
            self.flush(part);
        }
    }

    fn flush(&mut self, part: usize) {
        let n = std::mem::take(&mut self.used[part]) as usize;
        if n == 0 {
            return;
        }
        let out = &mut self.output[part];
        let source = self.staging[part].as_ptr().cast::<EventRow>();
        out.reserve(n);
        let len = out.len();
        // Only the initialized staging prefix is copied. reserve supplies the
        // entire destination and no other worker accesses this private vector.
        unsafe {
            let dest = out.as_mut_ptr().add(len);
            #[cfg(target_arch = "x86_64")]
            if self.mode == Writes::Stream && n == 8 && dest as usize % 16 == 0 {
                use std::arch::x86_64::*;
                for lane in 0..(8 * size_of::<EventRow>() / 16) {
                    _mm_stream_si128(
                        dest.cast::<__m128i>().add(lane),
                        _mm_loadu_si128(source.cast::<__m128i>().add(lane)),
                    );
                }
                // Vec may reallocate (and read the old range) on the next flush.
                _mm_sfence();
                out.set_len(len + n);
                return;
            }
            std::ptr::copy_nonoverlapping(source, dest, n);
            out.set_len(len + n);
        }
    }

    fn finish(mut self) -> Vec<Vec<EventRow>> {
        for part in 0..self.used.len() {
            self.flush(part);
        }
        self.output
    }
}

#[derive(Clone, Copy)]
struct WinnerRow {
    user: u64,
    chunk: u32,
    stamp: u32,
}
#[derive(Clone, Copy)]
struct Candidate {
    row: EventRow,
    chunk: u32,
}
#[derive(Clone, Copy)]
struct RawClick {
    user: u64,
    time: i64,
    id: u64,
    campaign: u32,
    chunk: u32,
}

type Pieces<T> = Vec<Vec<T>>;

#[derive(Default)]
struct Work {
    parsed: usize,
    valid: usize,
    side_rows: usize,
    wide_events: usize,
    wide_users: usize,
    ingest_side: usize,
    nonempty_event_buckets: usize,
    max_event_bucket: usize,
    median_event_bucket: usize,
    event_capacity: usize,
    repeated_rows: usize,
    real_duplicates: usize,
    linear_probes: usize,
    packed_events: usize,
    scan_time: Duration,
    exchange_time: Duration,
    winner_time: Duration,
    scan_tasks: usize,
    stripe_rows:usize,
}

struct Winners {
    partitions: Vec<Pieces<WinnerRow>>,
    data: Vec<EventData>,
    count: usize,
    work: Work,
}

fn config_size(name: &str, default: usize) -> Result<usize> {
    let size = match std::env::var(name) {
        Ok(s) => s.parse::<usize>()?,
        Err(_) => default,
    };
    ensure!(
        size.is_power_of_two() && size <= 8192,
        "{name} must be a power of two up to 8192"
    );
    Ok(size)
}

fn switch(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[derive(Clone, Copy)]
struct Plan {
    event_parts: usize,
    user_parts: usize,
    writes: Writes,
    bitmap: bool,
    overlap: bool,
    p1: bool,
    early_clicks: bool,
    profile: bool,
    linear: bool,
    factor: usize,
    arena: bool,
    reuse: bool,
    packed: bool,
    flat: bool,
    chunk_group: usize,
    slab:bool,
}

impl Plan {
    fn chunks_per_task(self, n:usize) -> usize {
        self.chunk_group.min((n/(rayon::current_num_threads()*2)).max(1))
    }
    fn new(bytes: usize) -> Result<Self> {
        let event_parts = bytes.div_ceil(3 << 20).clamp(8, 2048).next_power_of_two();
        let user_parts = bytes.div_ceil(10 << 20).clamp(8, 512).next_power_of_two();
        let writes = match switch("PERF_ROUTE_WRITES", "buffered").as_str() {
            "direct" => Writes::Direct,
            "buffered" => Writes::Buffered,
            "stream" => Writes::Stream,
            _ => anyhow::bail!("unknown PERF_ROUTE_WRITES"),
        };
        let chunk_group = switch("PERF_ROUTE_CHUNKS", "1").parse::<usize>()?;
        ensure!((1..=128).contains(&chunk_group), "PERF_ROUTE_CHUNKS must be 1..128");
        Ok(Self {
            event_parts: config_size("PERF_ROUTE_EVENT_BUCKETS", event_parts)?,
            user_parts: config_size("PERF_ROUTE_USER_BUCKETS", user_parts)?,
            writes,
            bitmap: switch("PERF_ROUTE_WINNER", "bitmap") != "map",
            linear: switch("PERF_ROUTE_EXACT", "hash") == "linear",
            factor: config_size("PERF_ROUTE_BITMAP_FACTOR", 8)?,
            arena: switch("PERF_ROUTE_ARENA", "0") == "1",
            reuse: switch("PERF_ROUTE_REUSE", "1") != "0",
            packed: switch("PERF_ROUTE_PACK", "0") == "1",
            flat: switch("PERF_ROUTE_GROUPS", "map") == "flat",
            chunk_group,
            slab:switch("PERF_ROUTE_SLAB","0")=="1",
            overlap: switch("PERF_ROUTE_LINKS", "overlap") != "serial",
            p1: switch("PERF_ROUTE_P1", "1") != "0",
            early_clicks: switch("PERF_ROUTE_CLICKS", "late") == "early",
            profile: switch("PROFILE", "0") == "1",
        })
    }
}

// Full spelling equality is independent of the bucket fingerprint. In
// particular an all-zero wide key is distinct from the compact value zero.
fn equal_key(a: Candidate, b: Candidate, keys: &[EventKeys]) -> bool {
    if (WIDE..OTHER).contains(&a.row.id) {
        (WIDE..OTHER).contains(&b.row.id)
            && keys[a.chunk as usize].wide[a.row.id as u32 as usize]
                == keys[b.chunk as usize].wide[b.row.id as u32 as usize]
    } else {
        a.row.id == b.row.id
    }
}

fn exact_hash(c: Candidate, keys: &[EventKeys]) -> usize {
    let value = if (WIDE..OTHER).contains(&c.row.id) {
        let [hi, lo] = keys[c.chunk as usize].wide[c.row.id as u32 as usize];
        hi.wrapping_mul(0xd6e8_feb8_6659_fd93) ^ lo.rotate_right(19)
    } else {
        c.row.id
    };
    mix(value) as usize
}

#[derive(Default)]
struct ExactTable {
    slots: Vec<u32>,
    entries: Vec<Candidate>,
    probes: usize,
}

impl ExactTable {
    fn clear(&mut self) {
        self.slots.fill(0);
        self.entries.clear();
        self.probes = 0;
    }
    fn slot(&mut self, c: Candidate, keys: &[EventKeys]) -> usize {
        let mask = self.slots.len() - 1;
        let mut idx = exact_hash(c, keys) & mask;
        loop {
            let n = self.slots[idx];
            self.probes += 1;
            if n == 0 || equal_key(c, self.entries[n as usize - 1], keys) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }
    fn insert(&mut self, c: Candidate, keys: &[EventKeys]) -> &mut Candidate {
        if (self.entries.len() + 1) * 4 > self.slots.len() * 3 {
            let capacity = (self.slots.len() * 2).max(64);
            self.slots.clear();
            self.slots.resize(capacity, 0);
            for i in 0..self.entries.len() {
                let idx = self.slot(self.entries[i], keys);
                self.slots[idx] = (i + 1) as u32;
            }
        }
        let idx = self.slot(c, keys);
        if self.slots[idx] == 0 {
            self.entries.push(c);
            self.slots[idx] = u32::try_from(self.entries.len()).expect("too many winners");
        }
        &mut self.entries[self.slots[idx] as usize - 1]
    }
}

#[derive(Default)]
struct Scratch {
    seen: Vec<u64>,
    repeated: Vec<u64>,
    linear: ExactTable,
    compact: AHashMap<u64, Candidate>,
    wide: AHashMap<[u64; 2], Candidate>,
}

struct Fragment {
    chunk: u32,
    bucket: u16,
    prefix_len: u32,
    full: Vec<EventRow>,
    short: Vec<packed::ShortEvent>,
}

impl Fragment {
    fn len(&self) -> usize {
        self.full.len() + self.short.len()+self.prefix_len as usize
    }
    fn rows<'a>(
        &'a self,
        keys: &'a [EventKeys],
        payloads: &'a [EventData],
    ) -> impl Iterator<Item = Candidate> + 'a {
        let chunk = self.chunk;
        let k = &keys[chunk as usize];
        let p = &payloads[chunk as usize];
        let (full,small)=k.prefixes(self.bucket as usize);
        full.iter().chain(&self.full)
            .copied()
            .chain(small.iter().chain(&self.short).map(move |&s| s.unpack(k, p)))
            .map(move |row| Candidate { row, chunk })
    }
}

enum Rows {
    Fragments(Vec<Fragment>),
    Pages(storage::Pages<Candidate>),
}

impl Rows {
    fn visit_ids(&self,keys:&[EventKeys], mut emit:impl FnMut(u64)) {
        match self {
            Self::Fragments(pieces)=>for p in pieces {
                let keys=&keys[p.chunk as usize];
                let (full,short)=keys.prefixes(p.bucket as usize);
                for row in full.iter().chain(&p.full) {emit(row.id);}
                for row in short.iter().chain(&p.short) {emit(row.key(keys));}
            },
            Self::Pages(p)=>p.iter().for_each(|c| emit(c.row.id)),
        }
    }

    fn visit(&self, keys: &[EventKeys], payloads: &[EventData], mut emit: impl FnMut(Candidate)) {
        match self {
            Self::Fragments(p) => {
                for part in p {
                    part.rows(keys, payloads).for_each(&mut emit);
                }
            }
            Self::Pages(p) => p.iter().for_each(emit),
        }
    }
}

fn event_winners(
    plans: Vec<Chunk>,
    plan: Plan,
    window: Window,
    dimensions: &Dimensions,
    raw_users: &Names,
    arena: &storage::Arena,
) -> Result<Winners> {
    let other_ids = Names::default();
    let calendar = Calendar::new(window);
    let base = window.start.div_euclid(1000);
    let scan_start=Instant::now();
    let bucket_header=size_of::<Vec<EventRow>>() * if plan.packed {2} else {1};
    let stripe_padding=if plan.slab {8*if plan.packed {size_of::<packed::ShortEvent>()} else {size_of::<EventRow>()} + size_of::<u32>()} else {0};
    let groups=crate::csv::work_groups(&plans,plan.chunks_per_task(plans.len()),plan.event_parts*(bucket_header+stripe_padding));
    let chunks = groups.into_par_iter()
        .map(|sources| {
            let mut sink = BucketWriter::new(plan.event_parts, plan.writes);
            let mut narrow = if plan.packed {
                empty_buckets(plan.event_parts)
            } else {
                Vec::new()
            };
            let (mut keys, mut data) = (EventKeys::default(), EventData::default());
            let mut stripe=plan.slab.then(||{
                // This reserve uses the same conservative byte density as
                // the original scorer. Actual keys still determine all rows.
                let bytes=sources.iter().map(Chunk::byte_len).sum::<usize>();
                let stride=bytes.div_ceil(70*plan.event_parts)+8;
                if plan.packed {EventStripe::Short(storage::Stripe::new(plan.event_parts,stride))}
                else {EventStripe::Full(storage::Stripe::new(plan.event_parts,stride))}
            });
            let mut valid_count = 0;
            for chunk in sources {
                chunk.scan_events(&calendar, window, dimensions, |id, user, ingest, valid| {
                let key = encode(id, b'e', &mut keys.wide, &other_ids, plan.packed);
                let (user, stamp) = match valid {
                    Some((time, sale)) => {
                        valid_count += 1;
                        (
                            encode(user, b'u', &mut data.users, raw_users, plan.packed),
                            data.stamp(window.start, time, sale),
                        )
                    }
                    None => (0, INVALID),
                };
                let row = EventRow {
                    id: key,
                    user,
                    stamp,
                    ingest: keys.ingest(base, ingest),
                };
                let bucket = hash(key) as usize & (plan.event_parts - 1);
                if plan.packed {
                    if let Some(s) = packed::ShortEvent::pack(row) {
                        if let Some(EventStripe::Short(slab))=&mut stripe {
                            if slab.push(bucket,s) {return;}
                        }
                        narrow[bucket].push(s);
                        return;
                    }
                }
                if let Some(EventStripe::Full(slab))=&mut stripe {
                    if slab.push(bucket,row) {return;}
                }
                sink.push(bucket, row);
                })?;
            }
            keys.stripe=stripe;
            Ok::<_, anyhow::Error>((sink.finish(), narrow, keys, data, valid_count))
        })
        .collect::<Result<Vec<_>>>()?;
    drop(other_ids);
    let exchange_start=Instant::now();

    let mut pieces: Vec<Vec<Fragment>> = empty_buckets(plan.event_parts);
    let mut keys = Vec::with_capacity(chunks.len());
    let mut payloads = Vec::with_capacity(chunks.len());
    let mut work = Work::default();
    work.scan_time=exchange_start.duration_since(scan_start);
    work.scan_tasks=chunks.len();
    for (i, (buckets, narrow, k, p, valid)) in chunks.into_iter().enumerate() {
        let chunk = u32::try_from(i)?;
        work.valid += valid;
        work.wide_events += k.wide.len();
        work.wide_users += p.users.len();
        work.ingest_side += k.ingests.len();
        work.side_rows += p.extra.len();
        if let Some(s)=&k.stripe {
            work.event_capacity+=match s {EventStripe::Full(v)=>v.capacity_bytes(),EventStripe::Short(v)=>v.capacity_bytes()};
        }
        payloads.push(p);
        let mut narrow = narrow.into_iter();
        for (part,(dest, full)) in pieces.iter_mut().zip(buckets).enumerate() {
            let short = narrow.next().unwrap_or_default();
            let (full_prefix,short_prefix)=k.prefixes(part);
            let prefix_len=u32::try_from(full_prefix.len()+short_prefix.len())?;
            work.stripe_rows+=prefix_len as usize;
            work.parsed += full.len() + short.len()+prefix_len as usize;
            work.packed_events += short.len()+short_prefix.len();
            work.event_capacity += full.capacity() * size_of::<EventRow>()
                + short.capacity() * size_of::<packed::ShortEvent>();
            if prefix_len!=0 || !(full.is_empty() && short.is_empty()) {
                dest.push(Fragment { chunk, bucket:part as u16, prefix_len, full, short });
            }
        }
        keys.push(k);
    }
    if plan.profile {
        let mut sizes: Vec<usize> = pieces
            .iter()
            .map(|p| p.iter().map(Fragment::len).sum())
            .collect();
        sizes.sort_unstable();
        work.nonempty_event_buckets = sizes.iter().filter(|&&n| n > 0).count();
        work.max_event_bucket = *sizes.last().unwrap_or(&0);
        work.median_event_bucket = sizes[sizes.len() / 2];
    }
    let winner_start=Instant::now();
    work.exchange_time=winner_start.duration_since(exchange_start);
    let results = pieces
        .into_par_iter()
        .with_min_len(4)
        .map_init(Scratch::default, |scratch, pieces| {
            if !plan.reuse {
                *scratch = Scratch::default();
            }
            let n: usize = pieces.iter().map(Fragment::len).sum();
            // Hash bits already consumed by partitioning cannot discriminate
            // bitmap slots. Avoid allocating unreachable addresses.
            let reachable=1usize<<(30-plan.event_parts.trailing_zeros());
            let slots=n.saturating_mul(plan.factor).clamp(64,reachable).next_power_of_two();
            let rows = if plan.arena {
                let all = pieces.iter().flat_map(|p| p.rows(&keys, &payloads));
                Rows::Pages(arena.pack(all))
            } else {
                Rows::Fragments(pieces)
            };
            let Scratch {
                seen,
                repeated,
                compact,
                wide,
                linear,
            } = scratch;
            linear.clear();
            let slot = |id| (hash(id) as usize >> plan.event_parts.trailing_zeros()) & (slots - 1);
            if plan.bitmap {
                seen.clear();
                seen.resize(slots / 64, 0u64);
                repeated.clear();
                repeated.resize(slots / 64, 0u64);
                crate::runtime::huge_pages(seen);
                crate::runtime::huge_pages(repeated);
                rows.visit_ids(&keys, |id| {
                    let i = slot(id);
                    let bit = 1u64 << (i % 64);
                    repeated[i / 64] |= seen[i / 64] & bit;
                    seen[i / 64] |= bit;
                });
            }
            let mut output: Pieces<WinnerRow> = empty_buckets(plan.user_parts);
            let mut count = 0;
            let mut disputed = 0;
            let mut route = |c: Candidate| {
                if c.row.stamp != INVALID {
                    let p = hash(c.row.user) as usize & (plan.user_parts - 1);
                    output[p].push(WinnerRow {
                        user: c.row.user,
                        chunk: c.chunk,
                        stamp: c.row.stamp,
                    });
                    count += 1;
                }
            };
            rows.visit(&keys, &payloads, |c| {
                let row = c.row;
                let meta = &keys[c.chunk as usize];
                let i = slot(row.id);
                if plan.bitmap && repeated[i / 64] & (1u64 << (i % 64)) == 0 {
                    route(c);
                    return;
                }
                disputed += 1;
                let winner = if plan.linear {
                    linear.insert(c, &keys)
                } else if (WIDE..OTHER).contains(&row.id) {
                    wide.entry(meta.wide[row.id as u32 as usize]).or_insert(c)
                } else {
                    compact.entry(row.id).or_insert(c)
                };
                let earlier = if (row.ingest | winner.row.ingest) < SIDE {
                    row.ingest < winner.row.ingest
                } else {
                    meta.millis(base, row.ingest)
                        < keys[winner.chunk as usize].millis(base, winner.row.ingest)
                };
                if earlier {
                    *winner = c;
                }
            });
            let duplicate = disputed - compact.len() - wide.len() - linear.entries.len();
            for c in compact
                .drain()
                .map(|(_, v)| v)
                .chain(wide.drain().map(|(_, v)| v))
                .chain(linear.entries.iter().copied())
            {
                route(c);
            }
            (output, count, disputed, duplicate, linear.probes)
        })
        .collect::<Vec<_>>();
    drop(keys);
    let mut partitions: Vec<Pieces<WinnerRow>> = empty_buckets(plan.user_parts);
    let mut count = 0;
    for (out, n, disputed, duplicates, probes) in results {
        count += n;
        work.repeated_rows += disputed;
        work.real_duplicates += duplicates;
        work.linear_probes += probes;
        for (dest, bucket) in partitions.iter_mut().zip(out) {
            if !bucket.is_empty() {
                dest.push(bucket);
            }
        }
    }
    work.winner_time=winner_start.elapsed();
    Ok(Winners {
        partitions,
        data: payloads,
        count,
        work,
    })
}

struct ClickBlock {
    chunk: u32,
    full: Vec<RawClick>,
    short: Vec<packed::ShortClick>,
}

struct ClickInput {
    partitions: Vec<Vec<ClickBlock>>,
    users: Vec<Vec<[u64; 2]>>,
    ids: ClickIds,
    elapsed: Duration,
}

fn raw_clicks(
    data: &Path,
    plan: Plan,
    window: Window,
    dimensions: &Dimensions,
    raw_users: &Names,
) -> Result<ClickInput> {
    let fallback = Names::default();
    let calendar = Calendar::new(window);
    let started=Instant::now();
    let sources = chunks(&data.join("clicks"))?;
    let metadata=plan.user_parts*size_of::<Vec<RawClick>>() * if plan.packed {2} else {1};
    let groups=crate::csv::work_groups(&sources,plan.chunks_per_task(sources.len()),metadata);
    let pieces = groups.into_par_iter()
        .enumerate()
        .map(|(i, sources)| {
            let mut output: Pieces<RawClick> = empty_buckets(plan.user_parts);
            let mut narrow = if plan.packed {
                empty_buckets(plan.user_parts)
            } else {
                Vec::new()
            };
            let mut users = Vec::new();
            let mut ids = ClickKeyChunk::new(i);
            let chunk_id = u32::try_from(i)?;
            for chunk in sources {
                chunk.scan_clicks(&calendar, window, dimensions, |id, user, time, campaign| {
                let user = encode(user, b'u', &mut users, raw_users, plan.packed);
                let part = hash(user) as usize & (plan.user_parts - 1);
                let mut encoded = None;
                if plan.packed {
                    let code = if id.len() == 33 {
                        let handle = ids.encode(id, &fallback);
                        encoded = Some(handle);
                        (handle >= crate::records::HEX_CLICK)
                            .then_some(packed::ShortClick::WIDE_SLOT | u64::from(handle as u32))
                    } else {
                        id.strip_prefix(b"k")
                            .and_then(packed::number)
                            .filter(|n| *n < packed::ShortClick::WIDE_SLOT)
                    };
                    let compact = code.and_then(|click| {
                        packed::ShortClick::pack(
                            user,
                            click,
                            time,
                            window.start.saturating_sub(input::LOOKBACK),
                            campaign,
                        )
                    });
                    if let Some(c) = compact {
                        narrow[part].push(c);
                        return;
                    }
                }
                let id = encoded.unwrap_or_else(|| ids.encode(id, &fallback));
                output[part].push(RawClick {
                    user,
                    time,
                    id,
                    campaign,
                    chunk: chunk_id,
                });
                })?;
            }
            Ok::<_, anyhow::Error>((output, narrow, users, ids))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut partitions: Vec<Vec<ClickBlock>> = empty_buckets(plan.user_parts);
    let mut users = Vec::with_capacity(pieces.len());
    let mut ids = Vec::with_capacity(pieces.len());
    for (i, (out, narrow, u, k)) in pieces.into_iter().enumerate() {
        users.push(u);
        ids.push(k);
        let mut narrow = narrow.into_iter();
        for (dest, full) in partitions.iter_mut().zip(out) {
            let short = narrow.next().unwrap_or_default();
            if !(full.is_empty() && short.is_empty()) {
                dest.push(ClickBlock {
                    chunk: i as u32,
                    full,
                    short,
                });
            }
        }
    }
    Ok(ClickInput {
        partitions,
        users,
        ids: ClickIds::new(fallback, ids),
        elapsed: started.elapsed(),
    })
}

#[derive(Clone, Copy)]
struct PersonEvent {
    person: u32,
    chunk: u32,
    stamp: u32,
}

#[derive(Default)]
struct Forward {
    events: Vec<PersonEvent>,
    clicks: Vec<Click>,
}

enum Destinations {
    Sparse(AHashMap<usize, Forward>),
    Dense(Vec<Forward>),
}

impl Destinations {
    fn new(plan:Plan)->Self {
        if plan.flat {
            Self::Dense((0..plan.user_parts).map(|_|Forward::default()).collect())
        } else {
            Self::Sparse(AHashMap::default())
        }
    }
    #[inline]
    fn get(&mut self,bucket:usize)->&mut Forward {
        match self {
            Self::Sparse(map)=>map.entry(bucket).or_default(),
            Self::Dense(rows)=>&mut rows[bucket],
        }
    }
    fn into_blocks(self)->Vec<(usize,Forward)> {
        match self {
            Self::Sparse(map)=>map.into_iter().collect(),
            Self::Dense(rows)=>rows.into_iter().enumerate().filter(|(_,r)|!(r.events.is_empty()&&r.clicks.is_empty())).collect(),
        }
    }
}

#[derive(Default, Clone, Copy)]
struct Counts {
    events: usize,
    sales: usize,
    clicks: usize,
}

impl Counts {
    fn add(&mut self, other: Self) {
        self.events += other.events;
        self.sales += other.sales;
        self.clicks += other.clicks;
    }
}

#[derive(Default)]
struct People {
    groups: Vec<(u32, Counts)>,
    events: Vec<PersonEvent>,
    clicks: Vec<Click>,
}

impl People {
    fn group(&mut self, person: u32) -> u32 {
        let slot = u32::try_from(self.groups.len()).expect("too many people");
        self.groups.push((person, Counts::default()));
        slot
    }

    fn event(&mut self, e: PersonEvent, data: &[EventData]) {
        let counts = &mut self.groups[e.person as usize].1;
        counts.events += 1;
        if e.stamp >= SIDE
            && data[e.chunk as usize].extra[(e.stamp ^ SIDE) as usize]
                .1
                .is_some()
        {
            counts.sales += 1;
        }
        self.events.push(e);
    }

    fn click(&mut self, c: Click) {
        self.groups[c.person as usize].1.clicks += 1;
        self.clicks.push(c);
    }

    fn finish(self, window: Window, data: &[EventData]) -> (SessionBatch, Vec<Click>) {
        let Self {
            groups,
            events,
            clicks,
        } = self;
        let n=events.len();
        Self::finish_groups(groups,events.into_iter(),clicks.into_iter(),n,false,window,data)
    }

    fn finish_groups(
        groups:Vec<(u32,Counts)>,
        events:impl Iterator<Item=PersonEvent>,
        clicks:impl Iterator<Item=Click>,
        event_count:usize,
        ordered_ids:bool,
        window: Window,
        data: &[EventData],
    ) -> (SessionBatch, Vec<Click>) {
        let mut indexes: Vec<_> = (0..groups.len())
            .filter(|&i| groups[i].1.events > 0)
            .collect();
        if !ordered_ids {
            indexes.sort_unstable_by_key(|&i| groups[i].0);
        }
        let mut totals = Counts::default();
        let mut heads = vec![Counts::default(); groups.len()];
        let mut person_ids = Vec::with_capacity(indexes.len());
        let mut boundaries = Vec::with_capacity(indexes.len());
        let mut click_ends = Vec::with_capacity(indexes.len());
        let compact = window
            .end
            .checked_sub(window.start)
            .and_then(|v| u32::try_from(v).ok())
            .is_some()
            && event_count <= u32::MAX as usize;
        for i in indexes {
            let (person, n) = groups[i];
            heads[i] = totals;
            totals.add(n);
            person_ids.push(person);
            click_ends.push(totals.clicks);
            if compact {
                boundaries.push((totals.events as u32, totals.sales as u32));
            }
        }
        let batch = if compact {
            let mut times = Vec::<u32>::with_capacity(totals.events);
            let mut sales = Vec::<(u32, Purchase)>::with_capacity(totals.sales);
            let dst_times = times.spare_capacity_mut();
            let dst_sales = sales.spare_capacity_mut();
            for e in events {
                let pos = &mut heads[e.person as usize];
                let stamp = if e.stamp < SIDE {
                    e.stamp
                } else {
                    let (time, p) = data[e.chunk as usize].decode(window.start, e.stamp);
                    let offset = (time - window.start) as u32;
                    if let Some(p) = p {
                        dst_sales[pos.sales].write((offset, p));
                        pos.sales += 1;
                    }
                    offset
                };
                dst_times[pos.events].write(stamp);
                pos.events += 1;
            }
            // Each group's exclusive counts were derived from these exact
            // rows. The ranges are disjoint, and every slot has been written.
            unsafe {
                times.set_len(totals.events);
                sales.set_len(totals.sales);
            }
            aggregate::compact_named_sessions(
                window.start,
                &mut times,
                &mut sales,
                boundaries,
                &person_ids,
            )
        } else {
            let mut batch = Batch::<Event>::default();
            batch.rows.reserve(totals.events);
            batch.purchases.reserve(totals.sales);
            for e in events {
                let (time, p) = data[e.chunk as usize].decode(window.start, e.stamp);
                let purchase = if let Some(p) = p {
                    batch.purchases.push(p);
                    u32::try_from(batch.purchases.len())
                        .expect("too many purchases in one partition")
                } else {
                    0
                };
                batch.rows.push(Event {
                    time,
                    person: groups[e.person as usize].0,
                    purchase,
                });
            }
            batch.rows.par_sort_unstable_by_key(|e| (e.person, e.time));
            aggregate::sessionize(batch)
        };
        let mut ordered = Vec::<Click>::with_capacity(totals.clicks);
        let dest = ordered.spare_capacity_mut();
        for mut c in clicks {
            let slot = c.person as usize;
            // Click-only people cannot credit or assist any session.
            if groups[slot].1.events == 0 {
                continue;
            }
            c.person = groups[slot].0;
            dest[heads[slot].clicks].write(c);
            heads[slot].clicks += 1;
        }
        // Counts and scatter exclude exactly the same click-only groups.
        unsafe {
            ordered.set_len(totals.clicks);
        }
        let mut begin = 0;
        for end in click_ends {
            let section = &mut ordered[begin..end];
            if section.len() > 32768 {
                section.par_sort_unstable_by_key(|c| c.time);
            } else if section.len() > 1 && !aggregate::skip_click_sort(section.len()) {
                section.sort_unstable_by_key(|c| c.time);
            }
            begin = end;
        }
        (batch, ordered)
    }
}

#[derive(Clone, Copy)]
enum Target {
    Here(u32),
    Forward(u32),
}

struct Resolver<'a> {
    compact: AHashMap<u64, Target>,
    wide: AHashMap<[u64; 2], Target>,
    names: &'a [Vec<Arc<str>>],
    people: &'a Names,
    aliases: &'a Aliases,
    cache: PersonCache,
    p1: bool,
    numeric: bool,
    hot: Vec<(u64,Target)>,
    bucket_bits: u32,
}

impl Resolver<'_> {
    #[inline]
    fn target(&mut self, user: u64, wide: &[[u64; 2]], local: &mut People) -> Target {
        macro_rules! resolve {
            ($map:expr, $key:expr) => {
                *$map.entry($key).or_insert_with(|| {
                    let mut buf = [0u8; 33];
                    let s = user_bytes(user, wide, self.names, &mut buf, self.numeric);
                    let q = self.cache.prepare(self.people, self.aliases, s);
                    let person = self.cache.prepared(q, self.people, self.aliases, s);
                    if self.p1 && !self.aliases.is_linked_person(person) {
                        Target::Here(local.group(person))
                    } else {
                        Target::Forward(person)
                    }
                })
            };
        }
        if (WIDE..OTHER).contains(&user) {
            resolve!(self.wide, wide[user as u32 as usize])
        } else if !self.hot.is_empty() {
            // All spellings reaching this task have the same low partition
            // bits. Use independent fingerprint bits for the private cache.
            let i=(hash(user) as usize>>self.bucket_bits)&(self.hot.len()-1);
            let (old,value)=self.hot[i];
            if old==user {return value;}
            let target=resolve!(self.compact,user);
            self.hot[i]=(user,target);
            target
        } else {
            resolve!(self.compact, user)
        }
    }
}

struct P1 {
    local: (SessionBatch, Vec<Click>),
    forwarded: Vec<(usize, Forward)>,
    spells: usize,
    direct: usize,
    elapsed: Duration,
    group_time: Duration,
    finish_time: Duration,
}

#[allow(clippy::too_many_arguments)]
fn first_pass(
    events: Pieces<WinnerRow>,
    clicks: Vec<ClickBlock>,
    data: &[EventData],
    click_users: &[Vec<[u64; 2]>],
    other: &[Vec<Arc<str>>],
    users: &Names,
    aliases: &Aliases,
    window: Window,
    plan: Plan,
    arena: &storage::Arena,
) -> P1 {
    let start = Instant::now();
    let cap=if plan.flat {
        let rows=events.iter().map(Vec::len).sum::<usize>()+clicks.iter().map(|c|c.full.len()+c.short.len()).sum::<usize>();
        (rows/8).clamp(128,32768).next_power_of_two()
    } else {0};
    let mut ids = Resolver {
        compact: AHashMap::default(),
        wide: AHashMap::default(),
        names: other,
        people: users,
        aliases,
        cache: PersonCache::default(),
        p1: plan.p1,
        numeric: plan.packed,
        hot: vec![(u64::MAX,Target::Here(0)); cap],
        bucket_bits:plan.user_parts.trailing_zeros(),
    };
    let mut local = People::default();
    let mut forwarded = Destinations::new(plan);
    for r in events.into_iter().flatten() {
        let dest = ids.target(r.user, &data[r.chunk as usize].users, &mut local);
        let person = match dest {
            Target::Here(p) | Target::Forward(p) => p,
        };
        let row = PersonEvent {
            person,
            chunk: r.chunk,
            stamp: r.stamp,
        };
        match dest {
            Target::Here(_) => local.event(row, data),
            Target::Forward(_) => forwarded
                .get(person as usize & (plan.user_parts - 1))
                .events
                .push(row),
        }
    }
    let mut accept_click = |c: RawClick| {
        let dest = ids.target(c.user, &click_users[c.chunk as usize], &mut local);
        let person = match dest {
            Target::Here(p) | Target::Forward(p) => p,
        };
        let row = Click {
            person,
            time: c.time,
            id: c.id,
            campaign: c.campaign,
        };
        match dest {
            Target::Here(_) => local.click(row),
            Target::Forward(_) => forwarded
                .get(person as usize & (plan.user_parts - 1))
                .clicks
                .push(row),
        }
    };
    let base = window.start.saturating_sub(input::LOOKBACK);
    let decoded = clicks.into_iter().flat_map(|block| {
        let chunk = block.chunk;
        let wide = &click_users[chunk as usize];
        block.full.into_iter().chain(
            block
                .short
                .into_iter()
                .map(move |s| s.unpack(chunk, base, wide)),
        )
    });
    if plan.arena {
        arena.pack(decoded).iter().for_each(accept_click);
    } else {
        decoded.for_each(&mut accept_click);
    }
    let spells = ids.compact.len() + ids.wide.len();
    let direct = local.groups.len();
    drop(ids);
    let group_time = start.elapsed();
    let finish_start=Instant::now();
    let local=local.finish(window,data);
    let finish_time=finish_start.elapsed();
    P1 {
        local,
        forwarded:forwarded.into_blocks(),
        spells,
        direct,
        elapsed: start.elapsed(),
        group_time,
        finish_time,
    }
}

fn second_pass(
    pieces: Vec<Forward>,
    data: &[EventData],
    window: Window,
    plan:Plan,
) -> (SessionBatch, Vec<Click>) {
    if plan.flat {
        return second_flat(pieces,data,window,plan.user_parts.trailing_zeros());
    }
    let mut groups = AHashMap::<u32, u32>::default();
    let mut local = People::default();
    for part in pieces {
        for mut row in part.events {
            row.person = *groups.entry(row.person).or_insert_with(||local.group(row.person));
            local.event(row, data);
        }
        for mut row in part.clicks {
            row.person = *groups.entry(row.person).or_insert_with(||local.group(row.person));
            local.click(row);
        }
    }
    drop(groups);
    local.finish(window, data)
}

fn second_flat(pieces:Vec<Forward>, data:&[EventData],window:Window,bits:u32)->(SessionBatch,Vec<Click>) {
    // Names assigns monotonic IDs inside interleaved shards. A destination
    // owns all people sharing its low bits, so the quotient is an exact index.
    // First count rows without moving them; the scatter then reads the original
    // blocks directly, avoiding one whole write and read of forwarded rows.
    let mut groups=Vec::<(u32,Counts)>::new();
    fn counts(groups:&mut Vec<(u32,Counts)>,id:u32,bits:u32)->&mut Counts {
        let slot=(id>>bits) as usize;
        if slot>=groups.len() {groups.resize(slot+1,(0,Counts::default()));}
        groups[slot].0=id;
        &mut groups[slot].1
    }
    let mut event_count=0;
    for part in &pieces {
        event_count+=part.events.len();
        for row in &part.events {
            let g=counts(&mut groups,row.person,bits);
            g.events+=1;
            if row.stamp>=SIDE && data[row.chunk as usize].extra[(row.stamp^SIDE) as usize].1.is_some() {
                g.sales+=1;
            }
        }
        for c in &part.clicks {
            counts(&mut groups,c.person,bits).clicks+=1;
        }
    }
    let events=pieces.iter().flat_map(|p|p.events.iter().copied()).map(|mut e| {e.person>>=bits;e});
    let clicks=pieces.iter().flat_map(|p|p.clicks.iter().copied()).map(|mut c| {c.person>>=bits;c});
    People::finish_groups(groups,events,clicks,event_count,true,window,data)
}

pub enum Campaigns {
    Pending(Vec<Vec<Click>>,ClickIds),
    Ready(aggregate::CampaignDays),
}

pub type Routed = (Vec<SessionBatch>,Campaigns,PersonNames,usize);

pub fn run(data: &Path, window: Window, dimensions: &Dimensions) -> Result<Routed> {
    let tasks = chunks(&data.join("events"))?;
    let bytes = tasks.iter().map(Chunk::byte_len).sum();
    let plan = Plan::new(bytes)?;
    let fused=crate::runtime::flag("PERF_ROUTE_FUSED").then(||aggregate::FusedCampaigns::new(dimensions.campaigns.len(),window));
    let arena = storage::Arena::new(plan.reuse);
    let started = Instant::now();
    let read_links = || -> Result<_> {
        let mut users = Names::default();
        let aliases = input::read_aliases(data, &mut users)?;
        Ok((users, aliases, Instant::now()))
    };
    let read_data = || -> Result<_> {
        let users = Names::default();
        let events = || event_winners(tasks, plan, window, dimensions, &users, &arena);
        let clicks = || raw_clicks(data, plan, window, dimensions, &users);
        let (winners, clicks) = if plan.early_clicks {
            let (ev, cl) = rayon::join(events, clicks);
            (ev?, cl?)
        } else {
            (events()?, clicks()?)
        };
        Ok((winners, clicks, users.into_names(), Instant::now()))
    };
    let ((users, aliases, link_end), (winners, clicks, other, ingest_end)) = if plan.overlap {
        let (links, rows) = rayon::join(read_links, read_data);
        (links?, rows?)
    } else {
        (read_links()?, read_data()?)
    };
    let Winners {
        partitions: events,
        data: payloads,
        count,
        work,
    } = winners;
    let ClickInput {
        partitions: clicks,
        users: click_users,
        ids,
        elapsed:click_time,
    } = clicks;
    let p1_start = Instant::now();
    let (click_count, packed_clicks) = if plan.profile {
        clicks
            .iter()
            .flatten()
            .fold((0usize, 0usize), |(total, small), c| {
                (total + c.full.len() + c.short.len(), small + c.short.len())
            })
    } else {
        (0, 0)
    };
    let click_wide: usize = if plan.profile {
        click_users.iter().map(Vec::len).sum()
    } else {
        0
    };
    let results = events
        .into_par_iter()
        .zip(clicks)
        .map(|(e, c)| {
            let mut result=first_pass(
                e,
                c,
                &payloads,
                &click_users,
                &other,
                &users,
                &aliases,
                window,
                plan,
                &arena,
            );
            if let Some(acc)=&fused {acc.consume(&mut result.local.0,&mut result.local.1,&ids);}
            result
        })
        .collect::<Vec<_>>();
    drop(other);
    drop(aliases);
    drop(click_users);
    let mut sessions = Vec::with_capacity(2 * plan.user_parts);
    let mut click_lists = Vec::with_capacity(2 * plan.user_parts);
    let mut later: Vec<Vec<Forward>> = empty_buckets(plan.user_parts);
    let (mut spells, mut direct, mut forwarded_events, mut forwarded_clicks) = (0, 0, 0, 0);
    let mut longest_p1 = Duration::ZERO;
    let (mut p1_group_tasks,mut p1_finish_tasks)=(Duration::ZERO,Duration::ZERO);
    for r in results {
        spells += r.spells;
        direct += r.direct;
        longest_p1 = longest_p1.max(r.elapsed);
        p1_group_tasks+=r.group_time;
        p1_finish_tasks+=r.finish_time;
        sessions.push(r.local.0);
        click_lists.push(r.local.1);
        for (part, rows) in r.forwarded {
            forwarded_events += rows.events.len();
            forwarded_clicks += rows.clicks.len();
            later[part].push(rows);
        }
    }
    let p2_start = Instant::now();
    let rest = later
        .into_par_iter()
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (mut batch,mut clicks)=second_pass(p,&payloads,window,plan);
            if let Some(acc)=&fused {acc.consume(&mut batch,&mut clicks,&ids);}
            (batch,clicks)
        })
        .collect::<Vec<_>>();
    for (s, c) in rest {
        sessions.push(s);
        click_lists.push(c);
    }
    if plan.profile {
        use std::sync::atomic::Ordering::Relaxed;
        let storage = serde_json::json!({
            "stage":"route_storage","exact_duplicate_rows":work.real_duplicates,"linear_probes":work.linear_probes,
            "arena_2mib_regions":arena.stats.regions.load(Relaxed),"arena_pages_claimed":arena.stats.pages.load(Relaxed),
            "arena_pages_reused":arena.stats.reused.load(Relaxed),"bitmap_slots_per_row":plan.factor,
            "packed_events":work.packed_events,"packed_clicks":packed_clicks,
            "event_scan_s":work.scan_time.as_secs_f64(),"event_exchange_s":work.exchange_time.as_secs_f64(),
            "event_winner_route_s":work.winner_time.as_secs_f64(),"click_scan_route_s":click_time.as_secs_f64(),
            "event_scan_tasks":work.scan_tasks,"stripe_rows":work.stripe_rows,"p1_group_task_sum_s":p1_group_tasks.as_secs_f64(),
            "p1_local_aggregate_task_sum_s":p1_finish_tasks.as_secs_f64()
        });
        eprintln!("profile {storage}");
        let v = serde_json::json!({
            "stage":"raw_user_route", "elapsed_s":started.elapsed().as_secs_f64(),
            "link_elapsed_s":link_end.duration_since(started).as_secs_f64(),
            "link_barrier_s":link_end.saturating_duration_since(ingest_end).as_secs_f64(),
            "p1_s":p2_start.duration_since(p1_start).as_secs_f64(),
            "p2_s":p2_start.elapsed().as_secs_f64(),"longest_p1_task_s":longest_p1.as_secs_f64(),
            "input_event_bytes":bytes,"event_partitions":plan.event_parts,"person_partitions":plan.user_parts,
            "event_buckets_nonempty":work.nonempty_event_buckets,"max_event_bucket":work.max_event_bucket,
            "median_event_bucket":work.median_event_bucket,"event_candidates":work.parsed,"valid_before_dedup":work.valid,
            "repeated_bucket_rows":work.repeated_rows,"valid_winners":count,"valid_clicks":click_count,
            "identity_queries":spells,"p1_complete_raw_groups":direct,"p2_raw_groups":spells-direct,
            "candidate_bytes":(work.parsed-work.packed_events)*size_of::<EventRow>()+work.packed_events*16,"candidate_capacity_bytes":work.event_capacity,
            "wide_event_key_bytes":work.wide_events*16,"wide_raw_user_bytes":(work.wide_users+click_wide)*16,
            "event_payload_side_bytes":work.side_rows*size_of::<(i64,Option<Purchase>)>(),
            "ingest_side_bytes":work.ingest_side*8,"winner_route_bytes":count*size_of::<WinnerRow>(),
            "click_route_bytes":(click_count-packed_clicks)*size_of::<RawClick>()+packed_clicks*16,"forwarded_events":forwarded_events,
            "forwarded_clicks":forwarded_clicks,"forwarded_bytes":forwarded_events*size_of::<PersonEvent>()+forwarded_clicks*size_of::<Click>(),
        });
        eprintln!("profile {v}");
    }
    let campaigns=if let Some(acc)=fused {Campaigns::Ready(acc.finish(&sessions))}
        else {Campaigns::Pending(click_lists,ids)};
    Ok((sessions,campaigns,users.into_people(),count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colliding_wide_keys_and_the_zero_short_handle_keep_separate_winners() {
        let mut keys = vec![EventKeys::default(), EventKeys::default()];
        let mut table = ExactTable::default();
        let dict = Names::default();
        let mut mk = |chunk: usize, raw: &[u8], stamp: u32| Candidate {
            row: EventRow {
                id: encode(raw, b'e', &mut keys[chunk].wide, &dict, true),
                user: 0,
                ingest: 0,
                stamp,
            },
            chunk: chunk as u32,
        };
        let short = mk(0, b"e", 1);
        let a = mk(0, b"e00000000000000000000000000000000", 2);
        let b = mk(0, b"e00000000008000000000000000000001", 3);
        let again = mk(1, b"e00000000000000000000000000000000", 4);
        assert_eq!(hash(short.row.id), hash(a.row.id));
        assert_eq!(hash(a.row.id), hash(b.row.id));
        for c in [short, a, b, again] {
            table.insert(c, &keys);
        }
        assert_eq!(table.entries.len(), 3);
        assert_eq!(table.insert(again, &keys).row.stamp, 2);
    }

    #[test]
    fn raw_user_keys_expand_to_the_exact_quoted_field_contents() {
        let spellings = [
            b"u".as_slice(),
            b"u0",
            b"u00",
            b"uAB",
            b"u0000000a",
            b"u123456789",
            b"u00000000000000000000000000000000",
            b"u0000000000000000000000000000000a",
            b"u0000000000000000000000000000000A",
            b"U0",
            b"uA\0B",
            b"uB\0",
        ];
        for numeric in [false, true] {
            let dict = Names::default();
            let mut wide = Vec::new();
            let handles = spellings.map(|s| encode(s, b'u', &mut wide, &dict, numeric));
            let names = dict.into_names();
            for (raw, handle) in spellings.into_iter().zip(handles) {
                assert_eq!(
                    user_bytes(handle, &wide, &names, &mut [0; 33], numeric),
                    raw
                );
            }
        }
    }
}
