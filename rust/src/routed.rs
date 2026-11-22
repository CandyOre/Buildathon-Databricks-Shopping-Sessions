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
//! With `PERF_ROUTE_INPUT_PAGES=1`, both full and packed lanes write
//! directly into 4 KiB pages. WRITES selects ordinary, batched, or streaming
//! stores for these lanes. Native pages take priority over a slab.
mod direct;
mod forward;
mod frontend;
mod packed;
mod person;
mod storage;
mod targets;
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

#[inline]
fn mixed_bits(word: u64) -> u64 {
    let v = (word ^ (word >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    v ^ (v >> 32)
}

fn mix(word: u64) -> u32 {
    (mixed_bits(word) & HASH_MASK) as u32
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
        let wide_key = if numeric {
            input::routed_hex(bytes, prefix)
        } else {
            input::hex_id(bytes, prefix)
        };
        if let Some(key) = wide_key {
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
    fn prefixes(&self, part: usize) -> (&[EventRow], &[packed::ShortEvent]) {
        match &self.stripe {
            None => (&[], &[]),
            Some(EventStripe::Full(s)) => (s.prefix(part), &[]),
            Some(EventStripe::Short(s)) => (&[], s.prefix(part)),
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
        self.ingest_side(time)
    }

    #[inline]
    fn ingest_seconds(&mut self, base: i64, seconds: i64) -> u32 {
        let relative = (seconds - base) as u64;
        if relative < u64::from(SIDE) {
            return relative as u32;
        }
        self.ingest_side(seconds * 1000)
    }

    #[cold]
    fn ingest_side(&mut self, time: i64) -> u32 {
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
// An immediate timestamp needs no source chunk. Small users leave room for
// all 31 timestamp bits. The other short base36 spellings use 42 bits, with
// the remaining 21 bits holding exact whole seconds.
#[derive(Clone, Copy)]
struct ShortWinner(u64);

impl ShortWinner {
    #[inline]
    fn encode(row: WinnerRow) -> Option<Self> {
        if row.stamp >= SIDE || row.user >= (1 << 42) {
            return None;
        }
        if row.user <= u64::from(u32::MAX) {
            return Some(Self((1 << 63) | (u64::from(row.stamp) << 32) | row.user));
        }
        let seconds = row.stamp / 1000;
        (row.stamp % 1000 == 0 && seconds < (1 << 21))
            .then(|| Self((u64::from(seconds) << 42) | row.user))
    }
    #[inline]
    fn decode(self) -> WinnerRow {
        let (user, stamp) = if self.0 >> 63 != 0 {
            (u64::from(self.0 as u32), (self.0 >> 32) as u32 & (SIDE - 1))
        } else {
            (self.0 & ((1 << 42) - 1), (self.0 >> 42) as u32 * 1000)
        };
        WinnerRow {
            user,
            stamp,
            chunk: 0,
        }
    }
}

#[derive(Default)]
struct RawWinnerBlock {
    short: Vec<ShortWinner>,
    // Precise millisecond, purchase, and wide-user exceptions retain the
    // original chunk. The matching EventData lives through both user passes.
    full: Vec<WinnerRow>,
}

impl RawWinnerBlock {
    #[inline]
    fn push(&mut self, row: WinnerRow, compact: bool) {
        if compact {
            if let Some(short) = ShortWinner::encode(row) {
                self.short.push(short);
                return;
            }
        }
        self.full.push(row);
    }
    fn len(&self) -> usize {
        self.short.len() + self.full.len()
    }
    fn rows(self) -> impl Iterator<Item = WinnerRow> {
        self.short
            .into_iter()
            .map(ShortWinner::decode)
            .chain(self.full)
    }
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

// Each of these records consists entirely of initialized integers. Its size
// equals the sum of its field widths, so byte-wise SIMD copies see no padding.
const _: () =
    assert!(size_of::<RawClick>() == 8 * 3 + 4 * 2 && size_of::<EventRow>() == 8 * 2 + 4 * 2);
unsafe impl storage::PageRecord for EventRow {}
unsafe impl storage::PageRecord for RawClick {}
unsafe impl storage::PageRecord for packed::ShortEvent {}
unsafe impl storage::PageRecord for packed::ShortClick {}

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
    stripe_rows: usize,
    returned_bucket_page_bytes: usize,
    returned_bucket_vec_bytes: usize,
    short_winners: usize,
    winner_capacity: usize,
}

struct Winners {
    partitions: Vec<Vec<RawWinnerBlock>>,
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
    person_kernel: bool,
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
    forward_blocks: bool,
    chunk_group: usize,
    slab: bool,
    input_pages: bool,
}

impl Plan {
    fn chunks_per_task(self, n: usize) -> usize {
        let target = if self.input_pages {
            self.chunk_group.max(16)
        } else {
            self.chunk_group
        };
        target.min((n / (rayon::current_num_threads() * 2)).max(1))
    }
    fn new(bytes: usize, architecture: bool) -> Result<Self> {
        // The compact front end batches four records per event destination.
        // Bound its active cache lines and the producer × consumer matrix;
        // the event-ID bitmap and exact maps remain private to each bucket.
        let (event_bytes, event_limit, person_bytes, person_limit) = if architecture {
            (16 << 20, 256, 32 << 20, 128)
        } else {
            (3 << 20, 2048, 10 << 20, 512)
        };
        let event_parts = bytes
            .div_ceil(event_bytes)
            .clamp(8, event_limit)
            .next_power_of_two();
        let user_parts = bytes
            .div_ceil(person_bytes)
            .clamp(8, person_limit)
            .next_power_of_two();
        let writes = match switch("PERF_ROUTE_WRITES", "buffered").as_str() {
            "direct" => Writes::Direct,
            "buffered" => Writes::Buffered,
            "stream" => Writes::Stream,
            _ => anyhow::bail!("unknown PERF_ROUTE_WRITES"),
        };
        let chunk_group =
            switch("PERF_ROUTE_CHUNKS", if architecture { "12" } else { "1" }).parse::<usize>()?;
        ensure!(
            (1..=128).contains(&chunk_group),
            "PERF_ROUTE_CHUNKS must be 1..128"
        );
        Ok(Self {
            person_kernel: false,
            event_parts: config_size("PERF_ROUTE_EVENT_BUCKETS", event_parts)?,
            user_parts: config_size("PERF_ROUTE_USER_BUCKETS", user_parts)?,
            writes,
            bitmap: switch("PERF_ROUTE_WINNER", "bitmap") != "map",
            linear: switch("PERF_ROUTE_EXACT", "hash") == "linear",
            factor: config_size("PERF_ROUTE_BITMAP_FACTOR", 8)?,
            arena: switch("PERF_ROUTE_ARENA", "0") == "1",
            reuse: switch("PERF_ROUTE_REUSE", "1") != "0",
            packed: switch("PERF_ROUTE_PACK", "0") == "1",
            flat: switch(
                "PERF_ROUTE_GROUPS",
                if architecture { "flat" } else { "map" },
            ) == "flat",
            forward_blocks: switch("PERF_ROUTE_FORWARD", "rows") == "blocks",
            chunk_group,
            slab: switch("PERF_ROUTE_SLAB", "0") == "1",
            input_pages: switch("PERF_ROUTE_INPUT_PAGES", "0") == "1",
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
    wide_by_fingerprint: AHashMap<u32, Candidate>,
    wide: AHashMap<[u64; 2], Candidate>,
}

struct Fragment {
    chunk: u32,
    bucket: u16,
    prefix_len: u32,
    full: Vec<EventRow>,
    short: Vec<packed::ShortEvent>,
    pages: NativeBlock<EventRow, packed::ShortEvent>,
}

struct NativeBlock<T: Copy, S: Copy> {
    // Ordinary Vec/slab fragments need just one null pointer per fragment.
    lanes: Option<Box<(Option<storage::Pages<T>>, Option<storage::Pages<S>>)>>,
}

impl<T: Copy, S: Copy> Default for NativeBlock<T, S> {
    fn default() -> Self {
        Self { lanes: None }
    }
}

impl<T: Copy, S: Copy> NativeBlock<T, S> {
    fn new(full: Option<storage::Pages<T>>, short: Option<storage::Pages<S>>) -> Self {
        Self {
            lanes: if full.is_some() || short.is_some() {
                Some(Box::new((full, short)))
            } else {
                None
            },
        }
    }
    fn full(&self) -> Option<&storage::Pages<T>> {
        self.lanes.as_deref()?.0.as_ref()
    }
    fn short(&self) -> Option<&storage::Pages<S>> {
        self.lanes.as_deref()?.1.as_ref()
    }
    fn len(&self) -> usize {
        self.full().map_or(0, |p| p.len()) + self.narrow_len()
    }
    fn narrow_len(&self) -> usize {
        self.short().map_or(0, |p| p.len())
    }
    fn capacity_bytes(&self) -> usize {
        self.full().map_or(0, |p| p.capacity_bytes())
            + self.short().map_or(0, |p| p.capacity_bytes())
    }
}

struct NativeWriters<T: storage::PageRecord, S: storage::PageRecord> {
    full: storage::InputWriter<T>,
    short: Option<storage::InputWriter<S>>,
}

impl<T: storage::PageRecord, S: storage::PageRecord> NativeWriters<T, S> {
    fn new(arena: &storage::Arena, parts: usize, plan: Plan, kind: storage::InputKind) -> Self {
        Self {
            full: storage::InputWriter::new(arena, parts, plan.writes, kind),
            short: plan
                .packed
                .then(|| storage::InputWriter::new(arena, parts, plan.writes, kind)),
        }
    }
    fn finish(self) -> Vec<NativeBlock<T, S>> {
        let mut short = self
            .short
            .map(storage::InputWriter::finish)
            .unwrap_or_default()
            .into_iter();
        self.full
            .finish()
            .into_iter()
            .map(|full| NativeBlock::new(full, short.next().flatten()))
            .collect()
    }
}

impl Fragment {
    fn len(&self) -> usize {
        self.full.len() + self.short.len() + self.prefix_len as usize + self.pages.len()
    }
    fn rows<'a, const INDEXED: bool>(
        &'a self,
        keys: &'a [EventKeys],
        payloads: &'a [EventData],
        bucket_bits: u32,
    ) -> impl Iterator<Item = Candidate> + 'a {
        let chunk = self.chunk;
        let k = &keys[chunk as usize];
        let p = &payloads[chunk as usize];
        let (full, small) = k.prefixes(self.bucket as usize);
        full.iter()
            .chain(&self.full)
            .copied()
            .chain(self.pages.full().into_iter().flat_map(|p| p.iter()))
            .chain(
                small
                    .iter()
                    .chain(&self.short)
                    .copied()
                    .chain(self.pages.short().into_iter().flat_map(|p| p.iter()))
                    .map(move |s| {
                        if INDEXED {
                            s.unpack_with_id(s.indexed_key(bucket_bits, self.bucket), p)
                        } else {
                            s.unpack(k, p)
                        }
                    }),
            )
            .map(move |row| Candidate { row, chunk })
    }
}

enum Rows {
    Fragments(Vec<Fragment>),
    Pages(storage::Pages<Candidate>),
}

impl Rows {
    fn visit_fingerprints<const INDEXED: bool>(
        &self,
        keys: &[EventKeys],
        bits: u32,
        mut emit: impl FnMut(usize),
    ) {
        let full = |id| bitmap_fingerprint::<INDEXED>(id, bits);
        match self {
            Self::Fragments(pieces) => {
                for p in pieces {
                    let keys = &keys[p.chunk as usize];
                    let compact = |row: packed::ShortEvent| {
                        if INDEXED {
                            row.bitmap_fingerprint(bits)
                        } else {
                            full(row.key(keys))
                        }
                    };
                    let (prefix, short) = keys.prefixes(p.bucket as usize);
                    for row in prefix.iter().chain(&p.full) {
                        emit(full(row.id));
                    }
                    for row in short.iter().chain(&p.short) {
                        emit(compact(*row));
                    }
                    if let Some(page) = p.pages.full() {
                        for row in page.iter() {
                            emit(full(row.id));
                        }
                    }
                    if let Some(page) = p.pages.short() {
                        for row in page.iter() {
                            emit(compact(row));
                        }
                    }
                }
            }
            Self::Pages(p) => p.iter().for_each(|c| emit(full(c.row.id))),
        }
    }

    fn visit<const INDEXED: bool>(
        &self,
        keys: &[EventKeys],
        payloads: &[EventData],
        bits: u32,
        mut emit: impl FnMut(Candidate),
    ) {
        match self {
            Self::Fragments(p) => {
                for part in p {
                    part.rows::<INDEXED>(keys, payloads, bits)
                        .for_each(&mut emit);
                }
            }
            Self::Pages(p) => p.iter().for_each(emit),
        }
    }
}

struct Scanned {
    indexed: bool,
    pieces: Vec<Vec<Fragment>>,
    keys: Vec<EventKeys>,
    payloads: Vec<EventData>,
    work: Work,
}

struct Selected<T> {
    streams: Vec<T>,
    payloads: Vec<EventData>,
    count: usize,
    work: Work,
}

fn scan_events(
    plans: Vec<Chunk>,
    plan: Plan,
    window: Window,
    dimensions: &Dimensions,
    raw_users: &Names,
    arena: &storage::Arena,
) -> Result<Scanned> {
    let other_ids = Names::default();
    let calendar = Calendar::new(window);
    let base = window.start.div_euclid(1000);
    let scan_start = Instant::now();
    let bucket_header = size_of::<Vec<EventRow>>() * if plan.packed { 2 } else { 1 };
    let stripe_padding = if plan.slab {
        8 * if plan.packed {
            size_of::<packed::ShortEvent>()
        } else {
            size_of::<EventRow>()
        } + size_of::<u32>()
    } else {
        0
    };
    let groups = crate::csv::work_groups(
        &plans,
        plan.chunks_per_task(plans.len()),
        plan.event_parts * (bucket_header + stripe_padding),
    );
    let frontend = frontend::mode(plan);
    let chunks = crate::runtime::stage_map("route_decode_events", groups, |sources| {
        match frontend {
            frontend::Mode::Batch => {
                return frontend::decode::<true>(
                    sources, plan, &calendar, window, dimensions, &other_ids, raw_users,
                );
            }
            frontend::Mode::Direct => {
                return frontend::decode::<false>(
                    sources, plan, &calendar, window, dimensions, &other_ids, raw_users,
                );
            }
            frontend::Mode::Legacy => {}
        }
        let mut pages = plan
            .input_pages
            .then(|| NativeWriters::new(arena, plan.event_parts, plan, storage::InputKind::Event));
        let mut sink = BucketWriter::new(
            if plan.input_pages {
                0
            } else {
                plan.event_parts
            },
            plan.writes,
        );
        let mut narrow = if plan.packed && !plan.input_pages {
            empty_buckets(plan.event_parts)
        } else {
            Vec::new()
        };
        let (mut keys, mut data) = (EventKeys::default(), EventData::default());
        let mut stripe = (plan.slab && !plan.input_pages).then(|| {
            // This reserve uses the same conservative byte density as
            // the original scorer. Actual keys still determine all rows.
            let bytes = sources.iter().map(Chunk::byte_len).sum::<usize>();
            let stride = bytes.div_ceil(70 * plan.event_parts) + 8;
            if plan.packed {
                EventStripe::Short(storage::Stripe::new(plan.event_parts, stride))
            } else {
                EventStripe::Full(storage::Stripe::new(plan.event_parts, stride))
            }
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
                        if let Some(w) = &mut pages {
                            w.short.as_mut().unwrap().push(bucket, s);
                            return;
                        }
                        if let Some(EventStripe::Short(slab)) = &mut stripe {
                            if slab.push(bucket, s) {
                                return;
                            }
                        }
                        narrow[bucket].push(s);
                        return;
                    }
                }
                if let Some(w) = &mut pages {
                    w.full.push(bucket, row);
                    return;
                }
                if let Some(EventStripe::Full(slab)) = &mut stripe {
                    if slab.push(bucket, row) {
                        return;
                    }
                }
                sink.push(bucket, row);
            })?;
        }
        keys.stripe = stripe;
        Ok::<_, anyhow::Error>((
            sink.finish(),
            narrow,
            pages.map(NativeWriters::finish).unwrap_or_default(),
            keys,
            data,
            valid_count,
        ))
    })
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    drop(other_ids);
    let exchange_start = Instant::now();

    let mut pieces: Vec<Vec<Fragment>> = empty_buckets(plan.event_parts);
    let mut keys = Vec::with_capacity(chunks.len());
    let mut payloads = Vec::with_capacity(chunks.len());
    let mut work = Work::default();
    work.scan_time = exchange_start.duration_since(scan_start);
    work.scan_tasks = chunks.len();
    for (i, (buckets, narrow, pages, k, p, valid)) in chunks.into_iter().enumerate() {
        let chunk = u32::try_from(i)?;
        work.valid += valid;
        work.wide_events += k.wide.len();
        work.wide_users += p.users.len();
        work.ingest_side += k.ingests.len();
        work.side_rows += p.extra.len();
        if let Some(s) = &k.stripe {
            work.event_capacity += match s {
                EventStripe::Full(v) => v.capacity_bytes(),
                EventStripe::Short(v) => v.capacity_bytes(),
            };
        }
        payloads.push(p);
        let mut narrow = narrow.into_iter();
        let mut full = buckets.into_iter();
        let mut pages = pages.into_iter();
        for (part, dest) in pieces.iter_mut().enumerate() {
            let full = full.next().unwrap_or_default();
            let page = pages.next().unwrap_or_default();
            let short = narrow.next().unwrap_or_default();
            let (full_prefix, short_prefix) = k.prefixes(part);
            let prefix_len = u32::try_from(full_prefix.len() + short_prefix.len())?;
            work.stripe_rows += prefix_len as usize;
            work.parsed += full.len() + short.len() + prefix_len as usize + page.len();
            work.packed_events += short.len() + short_prefix.len() + page.narrow_len();
            work.event_capacity += full.capacity() * size_of::<EventRow>()
                + short.capacity() * size_of::<packed::ShortEvent>()
                + page.capacity_bytes();
            if page.len() != 0 || prefix_len != 0 || !(full.is_empty() && short.is_empty()) {
                dest.push(Fragment {
                    chunk,
                    bucket: part as u16,
                    prefix_len,
                    full,
                    short,
                    pages: page,
                });
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
    work.exchange_time = exchange_start.elapsed();
    Ok(Scanned {
        indexed: frontend != frontend::Mode::Legacy,
        pieces,
        keys,
        payloads,
        work,
    })
}

#[inline]
fn bitmap_fingerprint<const INDEXED: bool>(id: u64, bucket_bits: u32) -> usize {
    let fingerprint = if INDEXED {
        packed::fingerprint(id)
    } else {
        hash(id)
    };
    (fingerprint >> bucket_bits) as usize
}

fn dedup_events<T: Send, U: Send>(
    scanned: Scanned,
    plan: Plan,
    window: Window,
    arena: &storage::Arena,
    init: impl Fn() -> T + Sync,
    emit: impl Fn(&mut T, Candidate, &[EventData]) + Sync,
    finish: impl Fn(T, &[EventData]) -> U + Sync,
) -> Selected<U> {
    if scanned.indexed {
        dedup_layout::<true, _, _>(scanned, plan, window, arena, init, emit, finish)
    } else {
        dedup_layout::<false, _, _>(scanned, plan, window, arena, init, emit, finish)
    }
}

fn dedup_layout<const INDEXED: bool, T: Send, U: Send>(
    scanned: Scanned,
    plan: Plan,
    window: Window,
    arena: &storage::Arena,
    init: impl Fn() -> T + Sync,
    emit: impl Fn(&mut T, Candidate, &[EventData]) + Sync,
    finish: impl Fn(T, &[EventData]) -> U + Sync,
) -> Selected<U> {
    let Scanned {
        pieces,
        keys,
        payloads,
        mut work,
        ..
    } = scanned;
    let bits = plan.event_parts.trailing_zeros();
    let base = window.start.div_euclid(1000);
    let winner_start = Instant::now();
    let results = crate::runtime::stage_map_init(
        "route_event_dedup",
        pieces,
        4,
        Scratch::default,
        |scratch, pieces| {
            if !plan.reuse {
                *scratch = Scratch::default();
            }
            let n: usize = pieces.iter().map(Fragment::len).sum();
            // Hash bits already consumed by partitioning cannot discriminate
            // bitmap slots. Avoid allocating unreachable addresses.
            let reachable = 1usize << (30 - plan.event_parts.trailing_zeros());
            let slots = n
                .saturating_mul(plan.factor)
                .clamp(64, reachable)
                .next_power_of_two();
            // Count the actual Vec allocations once, including those copied
            // into arena pages and released before the winner table is built.
            let returned_vec = if plan.profile {
                pieces
                    .iter()
                    .map(|p| {
                        p.full.capacity() * size_of::<EventRow>()
                            + p.short.capacity() * size_of::<packed::ShortEvent>()
                    })
                    .sum::<usize>()
            } else {
                0
            };
            let rows = if plan.arena && !plan.input_pages {
                let all = pieces
                    .iter()
                    .flat_map(|p| p.rows::<INDEXED>(&keys, &payloads, bits));
                let pages = arena.pack(all);
                drop(pieces);
                Rows::Pages(pages)
            } else {
                Rows::Fragments(pieces)
            };
            let Scratch {
                seen,
                repeated,
                compact,
                wide_by_fingerprint,
                wide,
                linear,
            } = scratch;
            linear.clear();
            if plan.bitmap {
                seen.clear();
                seen.resize(slots / 64, 0u64);
                repeated.clear();
                repeated.resize(slots / 64, 0u64);
                crate::runtime::huge_pages(seen);
                crate::runtime::huge_pages(repeated);
                rows.visit_fingerprints::<INDEXED>(&keys, bits, |h| {
                    let i = h & (slots - 1);
                    let bit = 1u64 << (i % 64);
                    repeated[i / 64] |= seen[i / 64] & bit;
                    seen[i / 64] |= bit;
                });
            }
            let mut output = init();
            let mut count = 0;
            let mut disputed = 0;
            let mut route = |c: Candidate| {
                if c.row.stamp != INVALID {
                    emit(&mut output, c, &payloads);
                    count += 1;
                }
            };
            rows.visit::<INDEXED>(&keys, &payloads, bits, |c| {
                let row = c.row;
                let meta = &keys[c.chunk as usize];
                let i = bitmap_fingerprint::<INDEXED>(row.id, bits) & (slots - 1);
                if plan.bitmap && repeated[i / 64] & (1u64 << (i % 64)) == 0 {
                    route(c);
                    return;
                }
                disputed += 1;
                let winner = if plan.linear {
                    linear.insert(c, &keys)
                } else if (WIDE..OTHER).contains(&row.id) {
                    // A contested bitmap bit typically covers several *distinct*
                    // fingerprints. Access a full key only when the entire
                    // thirty-bit fingerprint is shared by two records.
                    let first = wide_by_fingerprint.entry(hash(row.id)).or_insert(c);
                    if (first.chunk == c.chunk && first.row.id == row.id)
                        || equal_key(*first, c, &keys)
                    {
                        first
                    } else {
                        wide.entry(meta.wide[row.id as u32 as usize]).or_insert(c)
                    }
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
            // Candidates in the exact table own their small row; only key and
            // value sidecars survive. Recycle this bucket's source pages now.
            let returned = if plan.profile {
                match &rows {
                    Rows::Pages(p) => p.capacity_bytes(),
                    Rows::Fragments(parts) => parts.iter().map(|p| p.pages.capacity_bytes()).sum(),
                }
            } else {
                0
            };
            drop(rows);
            let duplicate = disputed
                - compact.len()
                - wide_by_fingerprint.len()
                - wide.len()
                - linear.entries.len();
            for c in compact
                .drain()
                .map(|(_, v)| v)
                .chain(wide_by_fingerprint.drain().map(|(_, v)| v))
                .chain(wide.drain().map(|(_, v)| v))
                .chain(linear.entries.iter().copied())
            {
                route(c);
            }
            (
                finish(output, &payloads),
                count,
                disputed,
                duplicate,
                linear.probes,
                returned,
                returned_vec,
            )
        },
    );
    drop(keys);
    let mut count = 0;
    let mut streams = Vec::with_capacity(results.len());
    for (stream, n, disputed, duplicates, probes, returned, returned_vec) in results {
        count += n;
        work.repeated_rows += disputed;
        work.real_duplicates += duplicates;
        work.linear_probes += probes;
        work.returned_bucket_page_bytes += returned;
        work.returned_bucket_vec_bytes += returned_vec;
        streams.push(stream);
    }
    work.winner_time = winner_start.elapsed();
    Selected {
        streams,
        payloads,
        count,
        work,
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
    let scanned = scan_events(plans, plan, window, dimensions, raw_users, arena)?;
    let compact = plan.person_kernel && switch("PERF_ARCH_WINNERS", "compact") != "full";
    let mut selected = dedup_events(
        scanned,
        plan,
        window,
        arena,
        || {
            (0..plan.user_parts)
                .map(|_| RawWinnerBlock::default())
                .collect::<Vec<_>>()
        },
        |output, c, _| {
            let bucket = hash(c.row.user) as usize & (plan.user_parts - 1);
            output[bucket].push(
                WinnerRow {
                    user: c.row.user,
                    chunk: c.chunk,
                    stamp: c.row.stamp,
                },
                compact,
            );
        },
        |output, _| output,
    );
    let mut partitions = empty_buckets(plan.user_parts);
    for stream in selected.streams {
        for (dest, bucket) in partitions.iter_mut().zip(stream) {
            if bucket.len() != 0 {
                if plan.profile {
                    selected.work.short_winners += bucket.short.len();
                    selected.work.winner_capacity += bucket.short.capacity()
                        * size_of::<ShortWinner>()
                        + bucket.full.capacity() * size_of::<WinnerRow>();
                }
                dest.push(bucket);
            }
        }
    }
    Ok(Winners {
        partitions,
        data: selected.payloads,
        count: selected.count,
        work: selected.work,
    })
}

struct ClickBlock {
    chunk: u32,
    full: Vec<RawClick>,
    short: Vec<packed::ShortClick>,
    pages: NativeBlock<RawClick, packed::ShortClick>,
}

impl ClickBlock {
    fn decoded(self, base: i64, wide: &[[u64; 2]]) -> impl Iterator<Item = RawClick> + '_ {
        let chunk = self.chunk;
        let (full, short) = self.pages.lanes.map(|p| *p).unwrap_or_default();
        self.full
            .into_iter()
            .chain(full.into_iter().flatten())
            .chain(
                self.short
                    .into_iter()
                    .chain(short.into_iter().flatten())
                    .map(move |c| c.unpack(chunk, base, wide)),
            )
    }
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
    arena: &storage::Arena,
) -> Result<ClickInput> {
    let fallback = Names::default();
    let calendar = Calendar::new(window);
    let started = Instant::now();
    let sources = chunks(&data.join("clicks"))?;
    let metadata = plan.user_parts * size_of::<Vec<RawClick>>() * if plan.packed { 2 } else { 1 };
    let groups = crate::csv::work_groups(&sources, plan.chunks_per_task(sources.len()), metadata);
    let groups = groups.into_iter().enumerate().collect();
    let pieces = crate::runtime::stage_map("route_decode_clicks", groups, |(i, sources)| {
        let mut pages = plan
            .input_pages
            .then(|| NativeWriters::new(arena, plan.user_parts, plan, storage::InputKind::Click));
        let mut output: Pieces<RawClick> = if plan.input_pages {
            Vec::new()
        } else {
            empty_buckets(plan.user_parts)
        };
        let mut narrow = if plan.packed && !plan.input_pages {
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
                        if let Some(w) = &mut pages {
                            w.short.as_mut().unwrap().push(part, c);
                        } else {
                            narrow[part].push(c);
                        }
                        return;
                    }
                }
                let id = encoded.unwrap_or_else(|| ids.encode(id, &fallback));
                let row = RawClick {
                    user,
                    time,
                    id,
                    campaign,
                    chunk: chunk_id,
                };
                if let Some(w) = &mut pages {
                    w.full.push(part, row);
                } else {
                    output[part].push(row);
                }
            })?;
        }
        Ok::<_, anyhow::Error>((
            output,
            narrow,
            pages.map(NativeWriters::finish).unwrap_or_default(),
            users,
            ids,
        ))
    })
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    let mut partitions: Vec<Vec<ClickBlock>> = empty_buckets(plan.user_parts);
    let mut users = Vec::with_capacity(pieces.len());
    let mut ids = Vec::with_capacity(pieces.len());
    for (i, (out, narrow, pages, u, k)) in pieces.into_iter().enumerate() {
        users.push(u);
        ids.push(k);
        let mut narrow = narrow.into_iter();
        let mut full = out.into_iter();
        let mut pages = pages.into_iter();
        for dest in &mut partitions {
            let full = full.next().unwrap_or_default();
            let page = pages.next().unwrap_or_default();
            let short = narrow.next().unwrap_or_default();
            if page.len() != 0 || !(full.is_empty() && short.is_empty()) {
                dest.push(ClickBlock {
                    chunk: i as u32,
                    full,
                    short,
                    pages: page,
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
    fn new(plan: Plan) -> Self {
        if plan.flat {
            Self::Dense((0..plan.user_parts).map(|_| Forward::default()).collect())
        } else {
            Self::Sparse(AHashMap::default())
        }
    }
    #[inline]
    fn get(&mut self, bucket: usize) -> &mut Forward {
        match self {
            Self::Sparse(map) => map.entry(bucket).or_default(),
            Self::Dense(rows) => &mut rows[bucket],
        }
    }
    fn into_blocks(self) -> Vec<(usize, Forward)> {
        match self {
            Self::Sparse(map) => map.into_iter().collect(),
            Self::Dense(rows) => rows
                .into_iter()
                .enumerate()
                .filter(|(_, r)| !(r.events.is_empty() && r.clicks.is_empty()))
                .collect(),
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

    fn finish(
        self,
        window: Window,
        data: &[EventData],
        names: Option<&Names>,
    ) -> (SessionBatch, Vec<Click>) {
        let Self {
            groups,
            events,
            clicks,
        } = self;
        let n = events.len();
        Self::finish_groups(
            groups,
            events.into_iter(),
            clicks.into_iter(),
            n,
            false,
            window,
            data,
            names,
        )
    }

    fn finish_groups(
        groups: Vec<(u32, Counts)>,
        events: impl Iterator<Item = PersonEvent>,
        clicks: impl Iterator<Item = Click>,
        event_count: usize,
        ordered_ids: bool,
        window: Window,
        data: &[EventData],
        names: Option<&Names>,
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
                names,
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
            aggregate::sessionize(batch, names)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    narrow: Option<targets::NarrowTargets>,
    hot: Vec<(u64, Target)>,
    bucket_bits: u32,
    numeric_links: Option<&'a input::NumericLinks>,
    local_names: input::OutputSpellings,
}

impl Resolver<'_> {
    #[inline]
    fn target(
        &mut self,
        user: u64,
        wide: &[[u64; 2]],
        local: &mut People,
        outbound: &mut forward::Builder,
    ) -> Target {
        let resolve = || {
            let mut buf = [0u8; 33];
            if let Some(index) = self.numeric_links {
                let linked = if user < WIDE {
                    index.person(user)
                } else if user < OTHER {
                    self.aliases
                        .linked_wide(self.people, wide[user as u32 as usize])
                } else {
                    self.aliases.linked_other(
                        self.people,
                        input::name_for(self.names, user as u32).as_bytes(),
                    )
                };
                if let Some(person) = linked {
                    return Target::Forward(outbound.slot(person));
                }
                // This raw group cannot occur in a second P1 partition.
                // Assign its output name locally, including the exact
                // sidecar spelling for every noncanonical handle.
                let id = u32::try_from(self.local_names.len()).expect("too many local names");
                let bytes = user_bytes(user, wide, self.names, &mut buf, true);
                self.local_names.push(input::text(bytes));
                return Target::Here(local.group(id));
            }
            let s = user_bytes(user, wide, self.names, &mut buf, self.numeric);
            let q = self.cache.prepare(self.people, self.aliases, s);
            let person = self.cache.prepared(q, self.people, self.aliases, s);
            if self.p1 && !self.aliases.is_linked_person(person) {
                Target::Here(local.group(person))
            } else {
                Target::Forward(outbound.slot(person))
            }
        };
        if user <= targets::KEY_MASK {
            if let Some(table) = &mut self.narrow {
                return table.get_or_insert_with(user, resolve);
            }
        }
        if (WIDE..OTHER).contains(&user) {
            *self
                .wide
                .entry(wide[user as u32 as usize])
                .or_insert_with(resolve)
        } else if !self.hot.is_empty() {
            // All spellings reaching this task have the same low partition
            // bits. Use independent fingerprint bits for the private cache.
            let i = (hash(user) as usize >> self.bucket_bits) & (self.hot.len() - 1);
            let (old, value) = self.hot[i];
            if old == user {
                return value;
            }
            let target = *self.compact.entry(user).or_insert_with(resolve);
            self.hot[i] = (user, target);
            target
        } else {
            *self.compact.entry(user).or_insert_with(resolve)
        }
    }
}

struct P1 {
    local: (SessionBatch, Vec<Click>),
    names: input::OutputSpellings,
    forwarded: Vec<(usize, forward::Packet)>,
    spells: usize,
    direct: usize,
    narrow_stats: [usize; 3],
    elapsed: Duration,
    group_time: Duration,
    finish_time: Duration,
}

#[allow(clippy::too_many_arguments)]
fn first_pass(
    events: Vec<RawWinnerBlock>,
    clicks: Vec<ClickBlock>,
    data: &[EventData],
    click_users: &[Vec<[u64; 2]>],
    other: &[Vec<Arc<str>>],
    users: &Names,
    aliases: &Aliases,
    window: Window,
    plan: Plan,
    arena: &storage::Arena,
    person_kernel: Option<(&aggregate::FusedCampaigns, &ClickIds)>,
    numeric_links: Option<&input::NumericLinks>,
) -> P1 {
    let start = Instant::now();
    let narrow = plan.person_kernel && plan.flat && switch("PERF_ARCH_TARGETS", "narrow") != "full";
    let cap = if plan.flat && !narrow {
        let rows = events.iter().map(RawWinnerBlock::len).sum::<usize>()
            + clicks
                .iter()
                .map(|c| c.full.len() + c.short.len() + c.pages.len())
                .sum::<usize>();
        (rows / 8).clamp(128, 32768).next_power_of_two()
    } else {
        0
    };
    let mut ids = Resolver {
        compact: AHashMap::default(),
        wide: AHashMap::default(),
        names: other,
        people: users,
        aliases,
        cache: PersonCache::default(),
        p1: plan.p1,
        numeric: plan.packed,
        narrow: narrow.then(|| targets::NarrowTargets::new(plan.user_parts.trailing_zeros())),
        hot: vec![(u64::MAX, Target::Here(0)); cap],
        bucket_bits: plan.user_parts.trailing_zeros(),
        numeric_links,
        local_names: Default::default(),
    };
    let mut local = People::default();
    let mut forwarded = forward::Builder::new(plan);
    for r in events.into_iter().flat_map(RawWinnerBlock::rows) {
        let dest = ids.target(
            r.user,
            &data[r.chunk as usize].users,
            &mut local,
            &mut forwarded,
        );
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
            Target::Forward(_) => forwarded.event(row, data),
        }
    }
    let mut accept_click = |c: RawClick| {
        let dest = ids.target(
            c.user,
            &click_users[c.chunk as usize],
            &mut local,
            &mut forwarded,
        );
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
            Target::Forward(_) => forwarded.click(row),
        }
    };
    let base = window.start.saturating_sub(input::LOOKBACK);
    let decoded = clicks.into_iter().flat_map(|block| {
        let chunk = block.chunk;
        let wide = &click_users[chunk as usize];
        block.decoded(base, wide)
    });
    if plan.arena && !plan.input_pages {
        arena.pack(decoded).iter().for_each(accept_click);
    } else {
        decoded.for_each(&mut accept_click);
    }
    let spells = ids.compact.len() + ids.wide.len() + ids.narrow.as_ref().map_or(0, |t| t.len());
    let narrow_stats = ids
        .narrow
        .as_ref()
        .map_or([0; 3], |t| [t.len(), t.slots(), t.large_groups()]);
    let direct = local.groups.len();
    let output_names = std::mem::take(&mut ids.local_names);
    drop(ids);
    let group_time = start.elapsed();
    let finish_start = Instant::now();
    let names = if numeric_links.is_some() {
        None
    } else {
        crate::output::aggregate_names(users)
    };
    let local = if let Some((acc, ids)) = person_kernel {
        (
            person::finish(local, data, window, names, acc, ids),
            Vec::new(),
        )
    } else {
        local.finish(window, data, names)
    };
    let finish_time = finish_start.elapsed();
    P1 {
        local,
        names: output_names,
        forwarded: forwarded.finish(),
        spells,
        direct,
        narrow_stats,
        elapsed: start.elapsed(),
        group_time,
        finish_time,
    }
}

fn second_pass(
    pieces: Vec<Forward>,
    data: &[EventData],
    window: Window,
    plan: Plan,
    names: Option<&Names>,
) -> (SessionBatch, Vec<Click>) {
    if plan.flat {
        return second_flat(
            pieces,
            data,
            window,
            plan.user_parts.trailing_zeros(),
            names,
        );
    }
    let mut groups = AHashMap::<u32, u32>::default();
    let mut local = People::default();
    for part in pieces {
        for mut row in part.events {
            row.person = *groups
                .entry(row.person)
                .or_insert_with(|| local.group(row.person));
            local.event(row, data);
        }
        for mut row in part.clicks {
            row.person = *groups
                .entry(row.person)
                .or_insert_with(|| local.group(row.person));
            local.click(row);
        }
    }
    drop(groups);
    local.finish(window, data, names)
}

fn second_flat(
    pieces: Vec<Forward>,
    data: &[EventData],
    window: Window,
    bits: u32,
    names: Option<&Names>,
) -> (SessionBatch, Vec<Click>) {
    // Names assigns monotonic IDs inside interleaved shards. A destination
    // owns all people sharing its low bits, so the quotient is an exact index.
    // First count rows without moving them; the scatter then reads the original
    // blocks directly, avoiding one whole write and read of forwarded rows.
    let mut groups = Vec::<(u32, Counts)>::new();
    fn counts(groups: &mut Vec<(u32, Counts)>, id: u32, bits: u32) -> &mut Counts {
        let slot = (id >> bits) as usize;
        if slot >= groups.len() {
            groups.resize(slot + 1, (0, Counts::default()));
        }
        groups[slot].0 = id;
        &mut groups[slot].1
    }
    let mut event_count = 0;
    for part in &pieces {
        event_count += part.events.len();
        for row in &part.events {
            let g = counts(&mut groups, row.person, bits);
            g.events += 1;
            if row.stamp >= SIDE
                && data[row.chunk as usize].extra[(row.stamp ^ SIDE) as usize]
                    .1
                    .is_some()
            {
                g.sales += 1;
            }
        }
        for c in &part.clicks {
            counts(&mut groups, c.person, bits).clicks += 1;
        }
    }
    let events = pieces
        .iter()
        .flat_map(|p| p.events.iter().copied())
        .map(|mut e| {
            e.person >>= bits;
            e
        });
    let clicks = pieces
        .iter()
        .flat_map(|p| p.clicks.iter().copied())
        .map(|mut c| {
            c.person >>= bits;
            c
        });
    People::finish_groups(
        groups,
        events,
        clicks,
        event_count,
        true,
        window,
        data,
        names,
    )
}

pub enum Campaigns {
    Pending(Vec<Vec<Click>>, ClickIds),
    Ready(aggregate::CampaignDays),
}

pub type Routed = (Vec<SessionBatch>, Campaigns, PersonNames, usize);

pub fn run(data: &Path, window: Window, dimensions: &Dimensions) -> Result<Routed> {
    run_impl(data, window, dimensions, None)
}

pub fn run_arch(
    data: &Path,
    window: Window,
    dimensions: &Dimensions,
    direct: bool,
) -> Result<Routed> {
    run_impl(data, window, dimensions, Some(direct))
}

fn run_impl(
    data: &Path,
    window: Window,
    dimensions: &Dimensions,
    direct: Option<bool>,
) -> Result<Routed> {
    let tasks = chunks(&data.join("events"))?;
    let bytes = tasks.iter().map(Chunk::byte_len).sum();
    let mut plan = Plan::new(bytes, direct.is_some())?;
    if let Some(use_direct) = direct {
        plan.person_kernel = true;
        plan.packed = true;
        plan.p1 = true;
        plan.forward_blocks = true;
        if use_direct {
            return direct::run(data, tasks, plan, window, dimensions, bytes);
        }
    }
    let fused = (plan.person_kernel || crate::runtime::flag("PERF_ROUTE_FUSED"))
        .then(|| aggregate::FusedCampaigns::new(dimensions.campaigns.len(), window));
    let arena = storage::Arena::new(plan.reuse);
    let started = Instant::now();
    let read_links = || -> Result<_> {
        let mut users = Names::default();
        let (aliases, numeric) = if plan.person_kernel {
            let (aliases, numeric) = input::read_numeric_aliases(data, &mut users, packed::number)?;
            (aliases, Some(numeric))
        } else {
            (input::read_aliases(data, &mut users)?, None)
        };
        Ok((users, aliases, numeric, Instant::now()))
    };
    let read_data = || -> Result<_> {
        let users = Names::default();
        let events = || event_winners(tasks, plan, window, dimensions, &users, &arena);
        let clicks = || raw_clicks(data, plan, window, dimensions, &users, &arena);
        let (winners, clicks) = if plan.early_clicks {
            let (ev, cl) = rayon::join(events, clicks);
            (ev?, cl?)
        } else {
            (events()?, clicks()?)
        };
        Ok((winners, clicks, users.into_names(), Instant::now()))
    };
    let ((users, aliases, numeric_links, link_end), (winners, clicks, other, ingest_end)) =
        if plan.overlap {
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
        elapsed: click_time,
    } = clicks;
    let p1_start = Instant::now();
    let (click_count, packed_clicks) = if plan.profile {
        clicks
            .iter()
            .flatten()
            .fold((0usize, 0usize), |(total, small), c| {
                (
                    total + c.full.len() + c.short.len() + c.pages.len(),
                    small + c.short.len() + c.pages.narrow_len(),
                )
            })
    } else {
        (0, 0)
    };
    let click_wide: usize = if plan.profile {
        click_users.iter().map(Vec::len).sum()
    } else {
        0
    };
    let partitions = events.into_iter().zip(clicks).collect();
    let results = crate::runtime::stage_map("route_first_pass", partitions, |(e, c)| {
        let mut result = first_pass(
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
            fused
                .as_ref()
                .filter(|_| plan.person_kernel)
                .map(|f| (f, &ids)),
            numeric_links.as_ref(),
        );
        if let Some(acc) = fused.as_ref().filter(|_| !plan.person_kernel) {
            let start = Instant::now();
            acc.consume(&mut result.local.0, &mut result.local.1, &ids);
            result.elapsed += start.elapsed();
        }
        result
    });
    drop(other);
    let mut next_output_id = aliases.linked_person_count();
    drop(aliases);
    drop(numeric_links);
    drop(click_users);
    let mut sessions = Vec::with_capacity(2 * plan.user_parts);
    let mut click_lists = Vec::with_capacity(2 * plan.user_parts);
    let mut later: Vec<Vec<forward::Packet>> = empty_buckets(plan.user_parts);
    let (mut spells, mut direct) = (0, 0);
    let mut narrow_stats = [0usize; 3];
    let mut forward_work = forward::Work::default();
    let mut longest_p1 = Duration::ZERO;
    let (mut p1_group_tasks, mut p1_finish_tasks) = (Duration::ZERO, Duration::ZERO);
    let mut local_names = Vec::new();
    for mut r in results {
        spells += r.spells;
        direct += r.direct;
        for (total, n) in narrow_stats.iter_mut().zip(r.narrow_stats) {
            *total += n;
        }
        longest_p1 = longest_p1.max(r.elapsed);
        p1_group_tasks += r.group_time;
        p1_finish_tasks += r.finish_time;
        if plan.person_kernel {
            let base = u32::try_from(next_output_id)?;
            next_output_id = next_output_id
                .checked_add(r.names.len())
                .ok_or_else(|| anyhow::anyhow!("too many output names"))?;
            ensure!(next_output_id <= u32::MAX as usize, "too many output names");
            for id in &mut r.local.0.daily.person {
                *id += base;
            }
            local_names.push(r.names);
        }
        sessions.push(r.local.0);
        click_lists.push(r.local.1);
        for (part, rows) in r.forwarded {
            if plan.profile {
                rows.account(&mut forward_work);
            }
            later[part].push(rows);
        }
    }
    let p2_start = Instant::now();
    let later = later.into_iter().filter(|p| !p.is_empty()).collect();
    let rest = crate::runtime::stage_map("route_second_pass", later, |p| {
        if plan.person_kernel {
            let batch = forward::finish_people(
                p,
                &payloads,
                window,
                crate::output::aggregate_names(&users),
                fused.as_ref().unwrap(),
                &ids,
                plan.user_parts.trailing_zeros(),
            );
            return (batch, Vec::new());
        }
        let (mut batch, mut clicks) = forward::finish(
            p,
            &payloads,
            window,
            plan,
            crate::output::aggregate_names(&users),
        );
        if let Some(acc) = &fused {
            acc.consume(&mut batch, &mut clicks, &ids);
        }
        (batch, clicks)
    });
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
            "native_reader_pages":arena.stats.input_pages.load(Relaxed),"native_reader_bytes":arena.stats.input_bytes.load(Relaxed),
            "native_stream_store_bytes":arena.stats.streaming_bytes.load(Relaxed),
            "event_pages_used_for_clicks":arena.stats.event_pages_used_for_clicks.load(Relaxed),
            "packed_events":work.packed_events,"packed_clicks":packed_clicks,
            "event_scan_s":work.scan_time.as_secs_f64(),"event_exchange_s":work.exchange_time.as_secs_f64(),
            "event_winner_route_s":work.winner_time.as_secs_f64(),"click_scan_route_s":click_time.as_secs_f64(),
            "event_scan_tasks":work.scan_tasks,"stripe_rows":work.stripe_rows,"p1_group_task_sum_s":p1_group_tasks.as_secs_f64(),
            "p1_local_aggregate_task_sum_s":p1_finish_tasks.as_secs_f64()
        });
        eprintln!("profile {storage}");
        let short_bytes = work.short_winners * size_of::<ShortWinner>();
        let full_rows = count - work.short_winners;
        let full_bytes = full_rows * size_of::<WinnerRow>();
        let exchange = serde_json::json!({
            "stage":"raw_winner_exchange", "short_rows":work.short_winners, "full_rows":full_rows,
            "short_bytes":short_bytes, "full_bytes":full_bytes, "capacity_bytes":work.winner_capacity,
            "source_bucket_arena_page_bytes_dropped":work.returned_bucket_page_bytes,
            "source_bucket_vec_capacity_bytes_dropped":work.returned_bucket_vec_bytes
        });
        eprintln!("profile {exchange}");
        let targets = serde_json::json!({
            "stage":"p1_short_targets","exact_keys":narrow_stats[0],"table_slots":narrow_stats[1],
            "slot_bytes":narrow_stats[1] * size_of::<u64>(), "overflow_groups":narrow_stats[2]
        });
        eprintln!("profile {targets}");
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
            "ingest_side_bytes":work.ingest_side*8,"winner_route_bytes":short_bytes+full_bytes,
            "click_route_bytes":(click_count-packed_clicks)*size_of::<RawClick>()+packed_clicks*16,"forwarded_events":forward_work.events,
            "forwarded_clicks":forward_work.clicks, "forwarded_descriptors":forward_work.groups,
            "forwarded_descriptor_bytes":forward_work.descriptor_bytes, "forwarded_event_bytes":forward_work.event_bytes,
            "forwarded_click_bytes":forward_work.click_bytes,
            "forwarded_bytes":forward_work.descriptor_bytes+forward_work.event_bytes+forward_work.click_bytes,
        });
        eprintln!("profile {v}");
    }
    let campaigns = if let Some(acc) = fused {
        Campaigns::Ready(acc.finish(&sessions))
    } else {
        Campaigns::Pending(click_lists, ids)
    };
    let mut output_names = users.into_people();
    if plan.person_kernel {
        output_names.append_partitions(local_names);
    }
    Ok((sessions, campaigns, output_names, count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_winner_preserves_user_and_exact_time_across_layout_boundaries() {
        assert_eq!(size_of::<ShortWinner>(), 8);
        assert_eq!(size_of::<WinnerRow>(), 16);
        let user32 = u64::from(u32::MAX);
        let user42 = (1 << 42) - 1;
        for (user, stamp) in [
            (0, 0),
            (user32, SIDE - 1),
            (user32 + 1, 0),
            (user42, ((1 << 21) - 1) * 1000),
        ] {
            let row = ShortWinner::encode(WinnerRow {
                user,
                stamp,
                chunk: 91,
            })
            .unwrap()
            .decode();
            assert_eq!((row.user, row.stamp), (user, stamp));
        }
        for (user, stamp) in [
            (user32 + 1, 1),
            (user42, (1 << 21) * 1000),
            (user42 + 1, 0),
            (WIDE, 0),
            (OTHER, 1000),
            (0, SIDE),
        ] {
            assert!(
                ShortWinner::encode(WinnerRow {
                    user,
                    stamp,
                    chunk: 91
                })
                .is_none()
            );
        }
    }

    #[test]
    fn full_winners_retain_their_source_wide_user_and_sale_payloads() {
        let mut chunks = [EventData::default(), EventData::default()];
        let mut block = RawWinnerBlock::default();
        let dict = Names::default();
        let spellings = [
            b"u00000000000000000000000000000001",
            b"u10000000000000000000000000000001",
        ];
        for (chunk, raw) in spellings.into_iter().enumerate() {
            let data = &mut chunks[chunk];
            let user = encode(raw, b'u', &mut data.users, &dict, true);
            let sale = Purchase {
                category: chunk as u32,
                cents: 123,
            };
            let stamp = data.stamp(0, chunk as i64, Some(sale));
            block.push(
                WinnerRow {
                    user,
                    chunk: chunk as u32,
                    stamp,
                },
                true,
            );
        }
        assert!(block.short.is_empty());
        let names = dict.into_names();
        for (chunk, row) in block.rows().enumerate() {
            assert_eq!(row.chunk as usize, chunk);
            let data = &chunks[row.chunk as usize];
            assert_eq!(
                user_bytes(row.user, &data.users, &names, &mut [0; 33], true),
                spellings[chunk]
            );
            let (time, sale) = data.decode(0, row.stamp);
            assert_eq!((time, sale.unwrap().category), (chunk as i64, chunk as u32));
        }
    }

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
    fn indexed_bucket_confirms_exact_collisions_and_skips_unshared_fingerprints() {
        let mut plan = Plan::new(0, true).unwrap();
        plan.event_parts = 8;
        plan.bitmap = true;
        plan.linear = false;
        plan.arena = false;
        let handle = |h: u32, slot: u32| WIDE | (u64::from(h) << 32) | u64::from(slot);
        let a = handle(0, 0);
        let b = handle(0, 1);
        // These distinct fingerprints share the same bitmap position. Their
        // unavailable side entries make any unnecessary key lookup fail.
        let x = handle(1 << 16, 91);
        let y = handle(2 << 16, 92);
        let source = |chunk: u32, input: &[(u64, u64, u32, u32)]| Fragment {
            chunk,
            bucket: 0,
            prefix_len: 0,
            full: Vec::new(),
            short: input
                .iter()
                .map(|&(id, user, stamp, ingest)| {
                    packed::ShortEvent::pack_indexed(
                        EventRow {
                            id,
                            user,
                            stamp,
                            ingest,
                        },
                        packed::fingerprint(id),
                        3,
                    )
                    .unwrap()
                })
                .collect(),
            pages: NativeBlock::default(),
        };
        let mut pieces = empty_buckets(8);
        pieces[0].push(source(
            0,
            &[
                (0, 9, 1000, 2),
                (a, 10, 1000, 30),
                (b, 11, 2000, 40),
                (x, 12, 3000, 1),
                (y, 13, 4000, 3),
            ],
        ));
        pieces[0].push(source(1, &[(a, 0, INVALID, 5), (b, 14, 5000, 20)]));
        let keys = (0..2)
            .map(|_| EventKeys {
                wide: vec![[0, 0], [1 << 23, 1]],
                ..EventKeys::default()
            })
            .collect();
        let scan = Scanned {
            indexed: true,
            pieces,
            keys,
            payloads: vec![EventData::default(), EventData::default()],
            work: Work::default(),
        };
        let winners = dedup_events(
            scan,
            plan,
            Window {
                start: 0,
                end: 100_000,
            },
            &storage::Arena::new(false),
            Vec::new,
            |out, c, _| out.push((c.row.user, c.row.stamp)),
            std::convert::identity,
        );
        let mut rows: Vec<_> = winners.streams.into_iter().flatten().collect();
        rows.sort_unstable();
        assert_eq!(rows, [(9, 1000), (12, 3000), (13, 4000), (14, 5000)]);
        assert_eq!(winners.work.real_duplicates, 2);
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
