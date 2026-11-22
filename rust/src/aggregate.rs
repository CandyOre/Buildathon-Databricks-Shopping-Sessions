use std::mem::size_of;

use ahash::AHashMap;
use rayon::prelude::*;

use crate::input::{DAY, LOOKBACK};
use crate::records::{Batch, Click, ClickIds, Event};

fn day(time: i64) -> i32 {
    time.div_euclid(DAY) as i32
}

#[derive(Default)]
pub struct UserDay {
    pub person: u32,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top_category: Option<u32>,
}

struct Session {
    time: i64,
    revenue: i64,
    person: u32,
}

#[derive(Default)]
pub struct DailyColumns {
    pub person: Vec<u32>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub events: Vec<i64>,
    pub revenue: Vec<i64>,
    pub category: Vec<Option<u32>>,
}

impl DailyColumns {
    fn push(&mut self, row: UserDay) {
        self.person.push(row.person);
        self.day.push(row.day);
        self.sessions.push(row.sessions);
        self.events.push(row.events);
        self.revenue.push(row.revenue);
        self.category.push(row.top_category);
    }
}

pub struct SessionBatch {
    pub daily: DailyColumns,
    sessions: Vec<Session>,
    work: Option<Box<SessionWork>>,
}

#[derive(Default)]
struct SessionWork {
    groups: usize,
    events: usize,
    purchases: usize,
    medium_attempts: usize,
    medium_accepted: usize,
    binned_groups: usize,
    binned_events: usize,
    purchase_bin_lookups: usize,
    single_purchase_groups: usize,
}

pub fn sessionize(batch: Batch<Event>) -> SessionBatch {
    let mut daily = DailyColumns::default();
    let mut sessions: Vec<Session> = Vec::new();
    let mut categories = AHashMap::<u32, i64>::new();
    for events in batch.rows.chunk_by(|a, b| a.person == b.person) {
        let first = events[0];
        let mut current = UserDay {
            person: first.person,
            day: day(first.time),
            sessions: 1,
            ..UserDay::default()
        };
        sessions.push(Session {
            person: first.person,
            time: first.time,
            revenue: 0,
        });
        let mut previous = first.time;
        for event in events {
            if event.time - previous > 1_800_000 {
                let new_day = day(event.time);
                if new_day != current.day {
                    finish_day(current, &mut categories, &mut daily);
                    current = UserDay {
                        person: first.person,
                        day: new_day,
                        ..UserDay::default()
                    };
                }
                current.sessions += 1;
                sessions.push(Session {
                    person: first.person,
                    time: event.time,
                    revenue: 0,
                });
            }
            previous = event.time;
            current.events += 1;
            if event.purchase != 0 {
                let purchase = batch.purchases[event.purchase as usize - 1];
                let cents = i64::from(purchase.cents);
                current.revenue += cents;
                sessions.last_mut().unwrap().revenue += cents;
                *categories.entry(purchase.category).or_default() += cents;
            }
        }
        finish_day(current, &mut categories, &mut daily);
    }
    SessionBatch { daily, sessions, work: None }
}

const SESSION_GAP: u32 = 1_800_000;

fn purchase_bins() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| crate::runtime::flag("PERF_PURCHASE_BINS"))
}

fn use_radix_time_sort() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| crate::runtime::flag("PERF_TIME_SORT"))
}

#[derive(Clone, Copy)]
struct TimeBin {
    first: u32,
    last: u32,
    count: u32,
}

fn binned_starts(times: &mut [u32], bins: &mut Vec<TimeBin>, positions: &mut Vec<usize>) -> bool {
    const MAX_BINS: usize = 1024;
    let last = times.iter().copied().max().unwrap_or(0);
    let length = (last / SESSION_GAP) as usize + 1;
    if length > MAX_BINS {
        return false;
    }
    bins.clear();
    bins.resize(
        length,
        TimeBin {
            first: u32::MAX,
            last: 0,
            count: 0,
        },
    );
    for &time in times.iter() {
        let bin = &mut bins[(time / SESSION_GAP) as usize];
        bin.first = bin.first.min(time);
        bin.last = bin.last.max(time);
        bin.count += 1;
    }
    // A bin spans strictly less than one session gap. Its extrema suffice
    // to decide whether it joins the preceding nonempty bin.
    positions.clear();
    let mut count = 0usize;
    let mut previous = 0;
    for bin in bins.iter_mut().filter(|bin| bin.count != 0) {
        if count == 0 || bin.first - previous > SESSION_GAP {
            positions.push(count);
            times[count] = bin.first;
        }
        count += bin.count as usize;
        // Every purchase is an event. Its populated bin can reuse this
        // counter as the exact session index after the prefix pass.
        bin.count = (positions.len() - 1) as u32;
        previous = bin.last;
    }
    positions.push(count);
    true
}

// Reuse bucket storage across people, initializing only occupied buckets.
// Medium groups abandon this path once their actual number of occupied
// buckets exceeds the threshold; times remain untouched on fallback.
fn active_binned_starts(
    times: &mut [u32], bins: &mut Vec<TimeBin>, positions: &mut Vec<usize>, max_active: usize,
) -> bool {
    let mut occupied = [0u64; 16];
    let mut active = 0;
    let empty = TimeBin { first: u32::MAX, last: 0, count: 0 };
    for &time in times.iter() {
        let idx = (time / SESSION_GAP) as usize;
        if idx >= 1024 { return false; }
        let word = &mut occupied[idx / 64];
        let mask = 1u64 << (idx % 64);
        if *word & mask == 0 {
            active += 1;
            if active > max_active { return false; }
            *word |= mask;
            if idx >= bins.len() { bins.resize((idx + 1).next_power_of_two(), empty); }
            bins[idx] = empty;
        }
        let bin = &mut bins[idx];
        bin.first = bin.first.min(time);
        bin.last = bin.last.max(time);
        bin.count += 1;
    }
    positions.clear();
    let mut count = 0;
    let mut previous = 0;
    for (word, mut mask) in occupied.into_iter().enumerate() {
        while mask != 0 {
            let bin = &mut bins[word * 64 + mask.trailing_zeros() as usize];
            if count == 0 || bin.first - previous > SESSION_GAP {
                positions.push(count);
                times[count] = bin.first;
            }
            count += bin.count as usize;
            bin.count = (positions.len() - 1) as u32;
            previous = bin.last;
            mask &= mask - 1;
        }
    }
    positions.push(count);
    true
}

fn sorted_time_offsets(values: &mut [u32], scratch: &mut Vec<u32>) {
    if use_radix_time_sort() {
        if values.len() <= 16 {
            for i in 1..values.len() {
                let v = values[i];
                let mut j = i;
                while j != 0 && v < values[j - 1] {
                    values[j] = values[j - 1];
                    j -= 1;
                }
                values[j] = v;
            }
            return;
        }
        if values.len() >= 2048 {
            let first = values[0];
            let changed = values.iter().fold(0, |changed, &x| changed | (first ^ x));
            scratch.resize(values.len(), 0);
            let mut in_scratch = false;
            for shift in [0, 8, 16, 24] {
                if (changed >> shift) & 255 == 0 {
                    continue;
                }
                let (source, dest) = if in_scratch {
                    (&scratch[..], &mut values[..])
                } else {
                    (&values[..], &mut scratch[..])
                };
                let bucket = |x: u32| ((x >> shift) & 255) as usize;
                let mut heads = [0usize; 256];
                for &x in source { heads[bucket(x)] += 1; }
                let mut total = 0;
                for head in &mut heads {
                    let n = *head;
                    *head = total;
                    total += n;
                }
                for &x in source {
                    let head = &mut heads[bucket(x)];
                    dest[*head] = x;
                    *head += 1;
                }
                in_scratch = !in_scratch;
            }
            if in_scratch { values.copy_from_slice(scratch); }
            return;
        }
    }
    if values.len() < 2048 {
        values.sort_unstable();
        return;
    }
    if values.len() > 131_072 {
        values.par_sort_unstable();
        return;
    }
    // Partition on the highest byte that actually varies. Comparison-sort
    // buckets of close timestamps independently in the reusable scratch space.
    let first = values[0];
    let changed = values.iter().fold(0, |bits, &t| bits | (t ^ first));
    if changed == 0 {
        return;
    }
    let shift = (31 - changed.leading_zeros()) & !7;
    let mut ends = [0usize; 256];
    let bucket = |t: u32| ((t >> shift) & 255) as usize;
    for &time in values.iter() {
        ends[bucket(time)] += 1;
    }
    let mut at = 0;
    for count in &mut ends {
        let len = *count;
        *count = at;
        at += len;
    }
    scratch.resize(values.len(), 0);
    for &time in values.iter() {
        let dst = &mut ends[bucket(time)];
        scratch[*dst] = time;
        *dst += 1;
    }
    let mut begin = 0;
    for end in ends {
        if end - begin > 1 {
            scratch[begin..end].sort_unstable();
        }
        begin = end;
    }
    values.copy_from_slice(scratch);
}

fn starts(times: &[u32], positions: &mut Vec<usize>) {
    positions.clear();
    positions.push(0);
    let mut next = 1;
    #[cfg(target_arch = "x86_64")]
    if times.len() >= 9 && std::is_x86_feature_detected!("avx2") {
        next = unsafe { vector_starts(times, positions) };
    }
    for index in next..times.len() {
        if times[index] - times[index - 1] > SESSION_GAP {
            positions.push(index);
        }
    }
    positions.push(times.len());
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn vector_starts(times: &[u32], positions: &mut Vec<usize>) -> usize {
    use std::arch::x86_64::*;
    // The caller checks AVX2 once before entering the vector loop.
    let (sign, bound) = unsafe {
        (
            _mm256_set1_epi32(i32::MIN),
            _mm256_set1_epi32(i32::MIN ^ SESSION_GAP as i32),
        )
    };
    let mut index = 1;
    while index + 8 <= times.len() {
        // Both loads cover eight elements. The loop guard bounds the last
        // element and index starts at one, so the prior load is also in range.
        let mut mask = unsafe {
            let current = _mm256_loadu_si256(times.as_ptr().add(index).cast());
            let previous = _mm256_loadu_si256(times.as_ptr().add(index - 1).cast());
            let delta = _mm256_xor_si256(_mm256_sub_epi32(current, previous), sign);
            let above = _mm256_cmpgt_epi32(delta, bound);
            _mm256_movemask_ps(_mm256_castsi256_ps(above)) as u32
        };
        while mask != 0 {
            positions.push(index + mask.trailing_zeros() as usize);
            mask &= mask - 1;
        }
        index += 8;
    }
    index
}

#[derive(Default)]
struct DayPurchases {
    first: Option<(u32, i64)>,
    others: AHashMap<u32, i64>,
    dense: bool,
    touched: u32,
    cents: [u32; 32],
}

impl DayPurchases {
    fn add(&mut self, purchase: crate::records::Purchase, revenue: &mut i64) {
        let amount = i64::from(purchase.cents);
        *revenue += amount;
        if self.dense {
            if purchase.category < 32 {
                let i = purchase.category as usize;
                self.touched |= 1u32 << i;
                let (low, carry) = self.cents[i].overflowing_add(purchase.cents);
                self.cents[i] = low;
                if carry { *self.others.entry(purchase.category).or_default() += 1i64 << 32; }
            } else {
                *self.others.entry(purchase.category).or_default() += amount;
            }
            return;
        }
        let first = self.first.get_or_insert((purchase.category, 0));
        if first.0 == purchase.category {
            first.1 += amount;
        } else {
            *self.others.entry(purchase.category).or_default() += amount;
        }
    }

    fn top(&mut self) -> Option<u32> {
        if self.dense {
            let mut best: Option<(i64, std::cmp::Reverse<u32>)> = None;
            while self.touched != 0 {
                let id = self.touched.trailing_zeros();
                self.touched &= self.touched - 1;
                let low = std::mem::take(&mut self.cents[id as usize]);
                let sum = i64::from(low) + self.others.remove(&id).unwrap_or(0);
                best = best.max(Some((sum, std::cmp::Reverse(id))));
            }
            for (id, sum) in self.others.drain() {
                best = best.max(Some((sum, std::cmp::Reverse(id))));
            }
            return best.map(|(_, id)| id.0);
        }
        let first = self.first.take()?;
        let best = self.others.drain().fold(first, |best, value| {
            if value.1 > best.1 || (value.1 == best.1 && value.0 < best.0) {
                value
            } else {
                best
            }
        });
        Some(best.0)
    }
}

pub fn compact_sessions(
    shard: usize,
    start: i64,
    times: &mut [u32],
    sales: &mut [(u32, crate::records::Purchase)],
    ends: Vec<(u32, u32)>,
) -> SessionBatch {
    compact_for_people(start, times, sales, ends, |index| {
        (index * crate::input::SHARDS + shard) as u32
    })
}

pub(crate) fn compact_named_sessions(
    start: i64,
    times: &mut [u32],
    sales: &mut [(u32, crate::records::Purchase)],
    ends: Vec<(u32, u32)>,
    people: &[u32],
) -> SessionBatch {
    compact_for_people(start, times, sales, ends, |index| people[index])
}

fn compact_for_people(
    start: i64,
    times: &mut [u32],
    sales: &mut [(u32, crate::records::Purchase)],
    ends: Vec<(u32, u32)>,
    person_at: impl Fn(usize) -> u32,
) -> SessionBatch {
    let mut daily = DailyColumns::default();
    let mut sessions = Vec::new();
    let mut starts_at = Vec::new();
    let mut time_bins = Vec::new();
    let medium_bins = std::env::var_os("PERF_MEDIUM_BINS").is_some_and(|v| v == "1");
    let occupied_bins = std::env::var_os("PERF_BIN_MASK").is_some_and(|v| v == "1");
    let direct_purchase = purchase_bins();
    let single_purchase = crate::runtime::flag("PERF_SINGLE_PURCHASE");
    let mut radix = Vec::new();
    let mut heads = Vec::new();
    let mut next_purchase = Vec::new();
    let mut category = DayPurchases {
        dense: std::env::var_os("PERF_CATEGORY_DENSE").is_some_and(|v| v == "1"),
        ..DayPurchases::default()
    };
    let mut work = crate::records::work_enabled().then(|| Box::new(SessionWork::default()));
    let mut prev = (0, 0);
    for (index, (last, sold)) in ends.into_iter().enumerate() {
        let local = &mut times[prev.0 as usize..last as usize];
        let purchases = &sales[prev.1 as usize..sold as usize];
        prev = (last, sold);
        if local.is_empty() {
            continue;
        }
        let medium = medium_bins && (64..2048).contains(&local.len());
        let binned = if medium {
            let threshold = local.len() / 4;
            active_binned_starts(local, &mut time_bins, &mut starts_at, threshold)
        } else if cfg!(feature = "large-input") && local.len() >= 2048 {
            if occupied_bins {
                active_binned_starts(local, &mut time_bins, &mut starts_at, usize::MAX)
            } else { binned_starts(local, &mut time_bins, &mut starts_at) }
        } else { false };
        if !binned {
            sorted_time_offsets(local, &mut radix);
            starts(local, &mut starts_at);
        }
        let count = starts_at.len() - 1;
        let locate = |time| {
            if direct_purchase && binned {
                time_bins[(time / SESSION_GAP) as usize].count as usize
            } else {
                starts_at[..count].partition_point(|&pos| local[pos] <= time) - 1
            }
        };
        let single = match purchases {
            &[(time, purchase)] if single_purchase && count > 1 => Some((locate(time), purchase)),
            _ => None,
        };
        if let Some(w) = &mut work {
            w.groups += 1;
            w.events += local.len();
            w.purchases += purchases.len();
            w.medium_attempts += usize::from(medium);
            w.medium_accepted += usize::from(medium && binned);
            w.binned_groups += usize::from(binned);
            if binned { w.binned_events += local.len(); }
            if direct_purchase && binned && count > 1 { w.purchase_bin_lookups += purchases.len(); }
            w.single_purchase_groups += usize::from(single.is_some());
        }
        heads.clear();
        if count > 1 && !purchases.is_empty() && single.is_none() {
            heads.resize(count, 0_u32);
            next_purchase.clear();
            for &(time, _) in purchases {
                let s = locate(time);
                next_purchase.push(heads[s]);
                heads[s] = next_purchase.len() as u32;
            }
        }
        let person = person_at(index);
        let mut row = UserDay {
            person,
            ..UserDay::default()
        };
        for (i, interval) in starts_at.windows(2).enumerate() {
            let time = start + i64::from(local[interval[0]]);
            let session_day = day(time);
            if row.sessions > 0 && session_day != row.day {
                row.top_category = category.top();
                daily.push(row);
                row = UserDay {
                    person,
                    ..UserDay::default()
                };
            }
            row.day = session_day;
            row.sessions += 1;
            row.events += (interval[1] - interval[0]) as i64;
            let mut revenue = 0;
            if count == 1 {
                for &(_, p) in purchases {
                    category.add(p, &mut revenue);
                }
            } else if let Some((owner, p)) = single {
                if owner == i { category.add(p, &mut revenue); }
            } else if !heads.is_empty() {
                let mut cursor = heads[i];
                while cursor != 0 {
                    let offset = cursor as usize - 1;
                    category.add(purchases[offset].1, &mut revenue);
                    cursor = next_purchase[offset];
                }
            }
            row.revenue += revenue;
            sessions.push(Session {
                person,
                time,
                revenue,
            });
        }
        row.top_category = category.top();
        daily.push(row);
    }
    SessionBatch { daily, sessions, work }
}

fn finish_day(mut row: UserDay, categories: &mut AHashMap<u32, i64>, daily: &mut DailyColumns) {
    // Category IDs follow the byte-wise order of category names.
    row.top_category = categories
        .drain()
        .max_by_key(|&(id, cents)| (cents, std::cmp::Reverse(id)))
        .map(|(id, _)| id);
    daily.push(row);
}

#[derive(Clone, Copy, Default)]
pub struct CampaignDay {
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue: i64,
    pub assisted_sessions: i64,
}

type CampaignMap = AHashMap<(u32, i32), CampaignDay>;
pub struct CampaignDays(Counters);

impl Default for CampaignDays {
    fn default() -> Self { Self(Counters::Sparse(Default::default())) }
}

impl IntoIterator for CampaignDays {
    type Item = ((u32, i32), CampaignDay);
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.entries()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CampaignMode { Map, Dense, Columns, Buffered }

fn campaign_mode() -> CampaignMode {
    static MODE: std::sync::OnceLock<CampaignMode> = std::sync::OnceLock::new();
    *MODE.get_or_init(|| match std::env::var("PERF_CAMPAIGNS").as_deref() {
        Ok("dense") => CampaignMode::Dense,
        Ok("columns") => CampaignMode::Columns,
        Ok("buffered") => CampaignMode::Buffered,
        _ => CampaignMode::Map,
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AssistMode {
    Intervals,
    Masks,
    Unsorted,
    Simd,
}

fn assist_mode() -> AssistMode {
    static MODE: std::sync::OnceLock<AssistMode> = std::sync::OnceLock::new();
    *MODE.get_or_init(|| match std::env::var("PERF_ASSISTS").as_deref() {
        Ok("unsorted") => AssistMode::Unsorted,
        Ok("mask") => AssistMode::Masks,
        Ok("simd") => AssistMode::Simd,
        _ => AssistMode::Intervals,
    })
}

pub(crate) fn skip_click_sort(n: usize) -> bool {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    let limit = *LIMIT.get_or_init(|| crate::runtime::number("PERF_SMALL_CLICKS",32).clamp(1,64));
    n <= limit && matches!(assist_mode(), AssistMode::Unsorted | AssistMode::Simd)
}

#[derive(Default)]
struct AssistScratch {
    intervals: AHashMap<u32, (i64, i64)>,
    masks: AHashMap<u32, u64>,
    work: Option<AttributionWork>,
}

#[derive(Default)]
struct AttributionWork {
    groups: [u64; 5],
    sessions: [u64; 5],
    clicks: [u64; 5],
    tie_comparisons: u64,
    cross_campaign_ties: u64,
    assist_campaigns_in_masks: u64,
}

impl AttributionWork {
    fn merge(&mut self, other: Self) {
        for i in 0..self.groups.len() {
            self.groups[i] += other.groups[i];
            self.sessions[i] += other.sessions[i];
            self.clicks[i] += other.clicks[i];
        }
        self.tie_comparisons += other.tie_comparisons;
        self.cross_campaign_ties += other.cross_campaign_ties;
        self.assist_campaigns_in_masks += other.assist_campaigns_in_masks;
    }
}

impl AssistScratch {
    fn record(&mut self, branch: usize, sessions: &[Session], clicks: &[Click]) {
        if let Some(stats) = &mut self.work {
            stats.groups[branch] += 1;
            stats.sessions[branch] += sessions.len() as u64;
            stats.clicks[branch] += clicks.len() as u64;
        }
    }

    fn wins_tie(&mut self, ids: &ClickIds, candidate: &Click, incumbent: &Click) -> bool {
        if let Some(stats) = &mut self.work {
            stats.tie_comparisons += 1;
            stats.cross_campaign_ties += u64::from(candidate.campaign != incumbent.campaign);
        }
        ids.compare(candidate.id, incumbent.id).is_lt()
    }
}

// Allocate dense counters only when the manifest and dimension table imply a
// modest working set. The sparse representation handles longer windows.
enum Counters {
    Dense {
        first_day: i32,
        days: usize,
        rows: Vec<CampaignDay>,
    },
    Columns {
        first_day: i32,
        days: usize,
        data: DenseColumns,
    },
    Sparse(CampaignMap),
}

struct DenseColumns {
    sessions: Vec<u16>,
    purchased: Vec<u16>,
    assisted: Vec<u16>,
    revenue: Vec<i64>,
    high: AHashMap<usize, [i64; 3]>,
    pending: Vec<(usize, CampaignDay)>,
    buffered: bool,
}

impl DenseColumns {
    fn new(size: usize, buffered: bool) -> Self {
        Self {
            sessions: vec![0; size], purchased: vec![0; size],
            assisted: vec![0; size], revenue: vec![0; size],
            high: Default::default(), pending: Vec::new(), buffered,
        }
    }

    #[inline]
    fn direct(&mut self, slot: usize, value: CampaignDay) {
        let sessions = i64::from(self.sessions[slot]) + value.sessions;
        let purchased = i64::from(self.purchased[slot]) + value.purchasing_sessions;
        let assisted = i64::from(self.assisted[slot]) + value.assisted_sessions;
        self.sessions[slot] = sessions as u16;
        self.purchased[slot] = purchased as u16;
        self.assisted[slot] = assisted as u16;
        self.revenue[slot] += value.revenue;
        if (sessions | purchased | assisted) > i64::from(u16::MAX) {
            let high = self.high.entry(slot).or_default();
            high[0] += sessions & !0xffff;
            high[1] += purchased & !0xffff;
            high[2] += assisted & !0xffff;
        }
    }

    fn flush(&mut self) {
        let mut pending = std::mem::take(&mut self.pending);
        #[cfg(target_arch = "x86_64")]
        for &(slot, _) in &pending {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            unsafe {
                _mm_prefetch(self.sessions.as_ptr().add(slot).cast(), _MM_HINT_T0);
                _mm_prefetch(self.purchased.as_ptr().add(slot).cast(), _MM_HINT_T0);
                _mm_prefetch(self.assisted.as_ptr().add(slot).cast(), _MM_HINT_T0);
                _mm_prefetch(self.revenue.as_ptr().add(slot).cast(), _MM_HINT_T0);
            }
        }
        for (slot, value) in pending.drain(..) { self.direct(slot, value); }
        self.pending = pending;
    }

    fn update(&mut self, slot: usize, value: CampaignDay) {
        if self.buffered {
            assert!(slot < self.sessions.len());
            self.pending.push((slot, value));
            if self.pending.len() == 32 { self.flush(); }
        } else {
            self.direct(slot, value);
        }
    }

    fn value(&self, slot: usize) -> CampaignDay {
        let mut value = CampaignDay {
            sessions: i64::from(self.sessions[slot]),
            purchasing_sessions: i64::from(self.purchased[slot]),
            assisted_sessions: i64::from(self.assisted[slot]),
            revenue: self.revenue[slot],
        };
        if !self.high.is_empty() {
            if let Some(high) = self.high.get(&slot) {
                value.sessions += high[0];
                value.purchasing_sessions += high[1];
                value.assisted_sessions += high[2];
            }
        }
        value
    }
}

impl Counters {
    fn new(campaigns: usize, window: crate::input::Window) -> Self {
        let first_day = day(window.start);
        let days = (i64::from(day(window.end.saturating_sub(1))) - i64::from(first_day) + 1).max(0)
            as usize;
        if let Some(size) = campaigns
            .checked_mul(days)
            .filter(|&n| n > 0 && n <= 131_072)
        {
            let mode = campaign_mode();
            if matches!(mode, CampaignMode::Columns | CampaignMode::Buffered) {
                Self::Columns { first_day, days, data: DenseColumns::new(size, mode == CampaignMode::Buffered) }
            } else {
                Self::Dense { first_day, days, rows: vec![CampaignDay::default(); size] }
            }
        } else {
            Self::Sparse(CampaignMap::default())
        }
    }

    fn add(&mut self, campaign: u32, day: i32, value: CampaignDay) {
        match self {
            Self::Dense {
                first_day,
                days,
                rows,
            } => rows[campaign as usize * *days + (day - *first_day) as usize].add(value),
            Self::Columns { first_day, days, data } =>
                data.update(campaign as usize * *days + (day - *first_day) as usize, value),
            Self::Sparse(rows) => rows.entry((campaign, day)).or_default().add(value),
        }
    }

    fn into_rows(self) -> CampaignDays {
        if campaign_mode() == CampaignMode::Map && !matches!(&self, Self::Sparse(_)) {
            return CampaignDays(Self::Sparse(self.entries().collect()));
        }
        CampaignDays(self)
    }

    fn entries(self) -> Box<dyn Iterator<Item = ((u32, i32), CampaignDay)>> {
        match self {
            Self::Sparse(rows) => Box::new(rows.into_iter()),
            Self::Dense {
                first_day,
                days,
                rows,
            } => Box::new(rows
                .into_iter()
                .enumerate()
                .filter(|(_, row)| row.sessions != 0 || row.assisted_sessions != 0)
                .map(move |(index, row)| {
                    (
                        ((index / days) as u32, first_day + (index % days) as i32),
                        row,
                    )
                })),
            Self::Columns { first_day, days, mut data } => {
                data.flush();
                Box::new((0..data.sessions.len()).filter_map(move |i| {
                    let value = data.value(i);
                    (value.sessions != 0 || value.assisted_sessions != 0).then_some((
                        ((i / days) as u32, first_day + (i % days) as i32), value))
                }))
            }
        }
    }
}

impl CampaignDay {
    fn credit(session: &Session, assisted: bool) -> Self {
        Self {
            sessions: 1,
            purchasing_sessions: i64::from(session.revenue > 0),
            revenue: session.revenue,
            assisted_sessions: i64::from(assisted),
        }
    }

    fn assists(n: i64) -> Self {
        Self { assisted_sessions: n, ..Self::default() }
    }

    fn add(&mut self, other: Self) {
        self.sessions += other.sessions;
        self.purchasing_sessions += other.purchasing_sessions;
        self.revenue += other.revenue;
        self.assisted_sessions += other.assisted_sessions;
    }
}

impl Counters {
    fn merge(mut self, other: Self) -> Self {
        match (&mut self, other) {
            (Self::Dense { rows, .. }, Self::Dense { rows: right, .. }) => {
                for (left, value) in rows.iter_mut().zip(right) {
                    left.add(value);
                }
            }
            (Self::Sparse(rows), Self::Sparse(right)) => {
                for (key, value) in right {
                    rows.entry(key).or_default().add(value);
                }
            }
            (Self::Columns { data: left, .. }, Self::Columns { data: mut right, .. }) => {
                left.flush();
                right.flush();
                for i in 0..left.sessions.len() {
                    let value = right.value(i);
                    if value.sessions != 0 || value.assisted_sessions != 0 { left.direct(i, value); }
                }
            }
            _ => unreachable!("all workers use the same window and campaign dimension"),
        }
        self
    }
}

fn log_session_work(batches: &[SessionBatch]) {
        let mut total = SessionWork::default();
        for w in batches.iter().filter_map(|b| b.work.as_deref()) {
            total.groups += w.groups;
            total.events += w.events;
            total.purchases += w.purchases;
            total.medium_attempts += w.medium_attempts;
            total.medium_accepted += w.medium_accepted;
            total.binned_groups += w.binned_groups;
            total.binned_events += w.binned_events;
            total.purchase_bin_lookups += w.purchase_bin_lookups;
            total.single_purchase_groups += w.single_purchase_groups;
        }
        eprintln!("profile {}", serde_json::json!({
            "stage":"session_work", "compact_groups":total.groups,
            "compact_events":total.events, "compact_purchases":total.purchases,
            "regrouped_time_bytes":total.events*size_of::<u32>(),
            "regrouped_purchase_bytes":total.purchases*size_of::<(u32,crate::records::Purchase)>(),
            "medium_attempts":total.medium_attempts, "medium_accepted":total.medium_accepted,
            "binned_groups":total.binned_groups, "binned_events":total.binned_events,
            "purchase_bin_lookups":total.purchase_bin_lookups,
            "single_purchase_groups":total.single_purchase_groups
        }));
}

pub fn campaign_daily(
    batches: &mut [SessionBatch],
    clicks: Vec<Vec<Click>>,
    ids: ClickIds,
    campaigns: usize,
    window: crate::input::Window,
) -> CampaignDays {
    let min_len = (batches.len() / (rayon::current_num_threads() * 2)).max(1);
    let work = crate::records::work_enabled();
    if work { log_session_work(batches); }
    let result = batches
        .par_iter_mut()
        .zip(clicks)
        .with_min_len(min_len)
        .fold(
            || (Counters::new(campaigns, window), AssistScratch {
                work: work.then(AttributionWork::default), ..Default::default()
            }),
            |(mut totals, mut scratch), (batch, mut clicks)| {
                summarize_campaigns(
                    std::mem::take(&mut batch.sessions),
                    &mut clicks,
                    &ids,
                    &mut totals,
                    &mut scratch,
                );
                (totals, scratch)
            },
        )
        .map(|(totals, scratch)| (totals, scratch.work))
        .reduce_with(merge_campaign_work);
    finish_campaigns(result)
}

type LocalCampaigns=(Counters,Option<AttributionWork>);

fn merge_campaign_work((left,mut stats):LocalCampaigns,(right,other):LocalCampaigns)->LocalCampaigns {
    if let (Some(s), Some(t))=(&mut stats,other) { s.merge(t); }
    (left.merge(right),stats)
}

fn finish_campaigns(result:Option<LocalCampaigns>)->CampaignDays {
    let Some((totals, stats)) = result else { return CampaignDays::default() };
    if let Some(stats) = stats {
        eprintln!("profile {}",serde_json::json!({
            "stage":"attribution_work", "branches":["zero_or_one_click","single_session","interval","sorted_mask","unsorted_mask"],
            "people_by_branch":stats.groups, "sessions_by_branch":stats.sessions,
            "clicks_considered_by_branch":stats.clicks, "tie_comparisons":stats.tie_comparisons,
            "cross_campaign_tie_comparisons":stats.cross_campaign_ties,
            "mask_user_campaign_pairs":stats.assist_campaigns_in_masks
        }));
    }
    totals.into_rows()
}

pub(crate) struct FusedCampaigns {
    // A partition can create at most one short critical section on its own
    // Rayon worker. The mutex also covers execution outside the global pool.
    workers:Vec<std::sync::Mutex<Option<(Counters,AssistScratch)>>>,
    campaigns:usize,
    window:crate::input::Window,
    work:bool,
    timing:Option<FusedTiming>,
}

#[derive(Default)]
struct FusedTiming {
    batches:std::sync::atomic::AtomicU64,
    sessions:std::sync::atomic::AtomicU64,
    clicks:std::sync::atomic::AtomicU64,
    nanoseconds:std::sync::atomic::AtomicU64,
}

impl FusedCampaigns {
    pub fn new(campaigns:usize,window:crate::input::Window)->Self {
        Self {
            workers:(0..crate::runtime::worker_slots()).map(|_|std::sync::Mutex::new(None)).collect(),
            campaigns,window,work:crate::records::work_enabled(),
            timing:crate::runtime::flag("PROFILE").then(FusedTiming::default),
        }
    }
    pub fn consume(&self,batch:&mut SessionBatch,clicks:&mut Vec<Click>,ids:&ClickIds) {
        let sessions=std::mem::take(&mut batch.sessions);
        let mut clicks=std::mem::take(clicks);
        if sessions.is_empty(){return;}
        let start=self.timing.as_ref().map(|t|{
            use std::sync::atomic::Ordering::Relaxed;
            t.batches.fetch_add(1,Relaxed);
            t.sessions.fetch_add(sessions.len() as u64,Relaxed);
            t.clicks.fetch_add(clicks.len() as u64,Relaxed);
            std::time::Instant::now()
        });
        let index=crate::runtime::worker_slot().unwrap_or(0)%self.workers.len();
        let mut worker=self.workers[index].lock().unwrap();
        let (totals,scratch)=worker.get_or_insert_with(||(
            Counters::new(self.campaigns,self.window),
            AssistScratch{work:self.work.then(AttributionWork::default),..AssistScratch::default()}
        ));
        summarize_campaigns(sessions,&mut clicks,ids,totals,scratch);
        if let (Some(t),Some(start))=(&self.timing,start) {
            t.nanoseconds.fetch_add(start.elapsed().as_nanos() as u64,std::sync::atomic::Ordering::Relaxed);
        }
    }
    pub fn finish(self,batches:&[SessionBatch])->CampaignDays {
        if self.work {log_session_work(batches);}
        if let Some(t)=self.timing {
            eprintln!("profile {}",serde_json::json!({"stage":"fused_attribution","batches":t.batches.into_inner(),
                "sessions_consumed":t.sessions.into_inner(),"clicks_consumed":t.clicks.into_inner(),
                "partition_task_sum_s":t.nanoseconds.into_inner() as f64*1e-9}));
        }
        let merged=self.workers.into_par_iter()
            .filter_map(|s|s.into_inner().unwrap().map(|(counts,scratch)|(counts,scratch.work)))
            .reduce_with(merge_campaign_work);
        finish_campaigns(merged)
    }
}

#[cfg(test)]
#[test]
fn fused_campaigns_release_local_buffers_and_merge_both_passes() {
    let window=crate::input::Window{start:0,end:3*DAY};
    let ids=ClickIds::new(crate::input::Names::default(),Vec::new());
    let local=FusedCampaigns::new(3,window);
    for person in [5,7,11] {
        let mut batch=SessionBatch{daily:DailyColumns::default(),work:None,
            sessions:vec![Session{time:DAY,revenue:10,person}]};
        let mut clicks=if person==7 {Vec::new()} else {vec![
            Click{person,time:0,campaign:2,id:2},Click{person,time:0,campaign:1,id:1},
            Click{person,time:0,campaign:2,id:2}
        ]};
        local.consume(&mut batch,&mut clicks,&ids);
        assert_eq!(batch.sessions.capacity(),0);
        assert_eq!(clicks.capacity(),0);
    }
    let rows:std::collections::BTreeMap<_,_>=local.finish(&[]).into_iter()
        .map(|(k,d)|(k,[d.sessions,d.purchasing_sessions,d.revenue,d.assisted_sessions])).collect();
    assert_eq!(rows[&(0,1)],[1,1,10,0]);
    assert_eq!(rows[&(1,1)],[2,2,20,2]);
    assert_eq!(rows[&(2,1)],[0,0,0,2]);
}

fn summarize_campaigns(
    sessions: Vec<Session>,
    clicks: &mut [Click],
    ids: &ClickIds,
    totals: &mut Counters,
    scratch: &mut AssistScratch,
) {
    let mut cursor = 0;
    let mode = assist_mode();
    for sessions in sessions.chunk_by(|a, b| a.person == b.person) {
        let person = sessions[0].person;
        while cursor < clicks.len() && clicks[cursor].person < person { cursor += 1; }
        let first = cursor;
        while cursor < clicks.len() && clicks[cursor].person == person { cursor += 1; }
        let person_clicks = &mut clicks[first..cursor];
        if person_clicks.len() > 1 && skip_click_sort(person_clicks.len()) {
            if sessions.len() <= 64 {
                scratch.record(4, sessions, person_clicks);
                small_attribution(sessions, person_clicks, ids, totals, scratch, true, mode);
                continue;
            }
            // The click reader cannot know the final session count. Restore
            // chronological order before this unusually large session group.
            person_clicks.sort_unstable_by_key(|c| c.time);
        }

        let start_time = sessions[0].time;
        let end_time = sessions.last().unwrap().time;
        let start = person_clicks.partition_point(|click| click.time < start_time - LOOKBACK);
        let end = person_clicks.partition_point(|click| click.time <= end_time);
        let person_clicks = &person_clicks[start..end];

        if person_clicks.len() <= 1 {
            scratch.record(0, sessions, person_clicks);
            let click = person_clicks.first();
            for session in sessions {
                let campaign = click
                    .filter(|c| c.time <= session.time && c.time >= session.time - LOOKBACK)
                    .map_or(0, |c| c.campaign);
                totals.add(campaign, day(session.time), CampaignDay::credit(session, campaign != 0));
            }
            continue;
        }
        if sessions.len() == 1 && person_clicks.len() <= 8 {
            scratch.record(1, sessions, person_clicks);
            let session = &sessions[0];
            let current_day = day(session.time);
            let mut unique = [0u32; 8];
            let mut used = 0;
            let mut best = person_clicks.last().unwrap();
            for click in person_clicks {
                if !unique[..used].contains(&click.campaign) {
                    unique[used] = click.campaign;
                    used += 1;
                    totals.add(click.campaign, current_day, CampaignDay::assists(1));
                }
                if click.time == best.time && scratch.wins_tie(ids, click, best) {
                    best = click;
                }
            }
            totals.add(best.campaign, current_day, CampaignDay::credit(session, false));
            continue;
        }
        credit(sessions, person_clicks, ids, totals, scratch);
        if mode != AssistMode::Intervals && sessions.len() <= 64 {
            scratch.record(3, sessions, person_clicks);
            small_attribution(sessions, person_clicks, ids, totals, scratch, false, mode);
            continue;
        }
        scratch.record(2, sessions, person_clicks);
        let intervals = &mut scratch.intervals;
        for click in person_clicks {
            let interval = intervals
                .entry(click.campaign)
                .or_insert((click.time, click.time + LOOKBACK));
            if click.time > interval.1 {
                assist(sessions, click.campaign, *interval, totals);
                interval.0 = click.time;
            }
            interval.1 = click.time + LOOKBACK;
        }
        for (campaign, interval) in intervals.drain() {
            assist(sessions, campaign, interval, totals);
        }
    }
}

fn low_bits(count: usize) -> u64 {
    !u64::MAX.checked_shl(count as u32).unwrap_or(0)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_window(times: &[i64], first: i64, last: i64) -> u64 {
    use std::arch::x86_64::*;
    let mut result = 0;
    let mut i = 0;
    unsafe {
        let first = _mm256_set1_epi64x(first);
        let last = _mm256_set1_epi64x(last);
        while i + 4 <= times.len() {
            let t = _mm256_loadu_si256(times.as_ptr().add(i).cast());
            let outside = _mm256_or_si256(_mm256_cmpgt_epi64(first, t), _mm256_cmpgt_epi64(t, last));
            result |= ((!_mm256_movemask_pd(_mm256_castsi256_pd(outside)) & 15) as u64) << i;
            i += 4;
        }
    }
    for (offset, &t) in times[i..].iter().enumerate() {
        result |= u64::from(first <= t && t <= last) << (i + offset);
    }
    result
}

fn small_attribution(
    sessions: &[Session],
    clicks: &[Click],
    ids: &ClickIds,
    totals: &mut Counters,
    scratch: &mut AssistScratch,
    find_winners: bool,
    mode: AssistMode,
) {
    debug_assert!(sessions.len() <= 64);
    let mut best = [None::<&Click>; 64];
    #[cfg(target_arch = "x86_64")]
    let mut times = [0i64; 64];
    #[cfg(target_arch = "x86_64")]
    let vector = mode == AssistMode::Simd && sessions.len() >= 4 && std::is_x86_feature_detected!("avx2");
    #[cfg(target_arch = "x86_64")]
    if vector {
        for (dest, session) in times.iter_mut().zip(sessions) { *dest = session.time; }
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = mode;
    for click in clicks {
        let first = click.time;
        let last = first.saturating_add(LOOKBACK);
        let scalar = || {
            let begin = sessions.partition_point(|s| s.time < first);
            let end = sessions.partition_point(|s| s.time <= last);
            low_bits(end) & !low_bits(begin)
        };
        #[cfg(target_arch = "x86_64")]
        let mask = if vector {
            unsafe { simd_window(&times[..sessions.len()], first, last) }
        } else { scalar() };
        #[cfg(not(target_arch = "x86_64"))]
        let mask = scalar();
        if mask == 0 { continue; }
        *scratch.masks.entry(click.campaign).or_default() |= mask;
        if find_winners {
            let mut bits = mask;
            while bits != 0 {
                let slot = &mut best[bits.trailing_zeros() as usize];
                if slot.is_none_or(|old| click.time > old.time ||
                    (click.time == old.time && scratch.wins_tie(ids, click, old))) {
                    *slot = Some(click);
                }
                bits &= bits - 1;
            }
        }
    }
    if find_winners {
        for (session, winner) in sessions.iter().zip(best) {
            totals.add(winner.map_or(0, |c| c.campaign), day(session.time),
                       CampaignDay::credit(session, false));
        }
    }
    if let Some(stats) = &mut scratch.work { stats.assist_campaigns_in_masks += scratch.masks.len() as u64; }
    for (campaign, mut bits) in scratch.masks.drain() {
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            let d = day(sessions[i].time);
            let midnight = (i64::from(d) + 1) * DAY;
            let end = i + sessions[i..].partition_point(|s| s.time < midnight);
            let through_day = low_bits(end);
            totals.add(campaign, d, CampaignDay::assists(i64::from((bits & through_day).count_ones())));
            bits &= !through_day;
        }
    }
}

fn credit(sessions: &[Session], clicks: &[Click], ids: &ClickIds, totals: &mut Counters, scratch: &mut AssistScratch) {
    let mut clicks = clicks.iter().peekable();
    let mut latest: Option<&Click> = None;
    for session in sessions {
        while clicks
            .peek()
            .is_some_and(|click| click.time <= session.time)
        {
            let click = clicks.next().unwrap();
            if latest
                .is_none_or(|best| click.time > best.time || scratch.wins_tie(ids, click, best))
            {
                latest = Some(click);
            }
        }
        let campaign = latest
            .filter(|click| click.time >= session.time - LOOKBACK)
            .map_or(0, |click| click.campaign);
        totals.add(campaign, day(session.time), CampaignDay::credit(session, false));
    }
}

fn assist(sessions: &[Session], campaign: u32, (first, last): (i64, i64), totals: &mut Counters) {
    let start = sessions.partition_point(|session| session.time < first);
    let end = sessions.partition_point(|session| session.time <= last);
    let mut covered = &sessions[start..end];
    // Count by day without materializing a row for each matching session/campaign.
    while let Some(session) = covered.first() {
        let current_day = day(session.time);
        let next_day = (i64::from(current_day) + 1) * DAY;
        let count = covered.partition_point(|session| session.time < next_day);
        totals.add(campaign, current_day, CampaignDay::assists(count as i64));
        covered = &covered[count..];
    }
}

#[cfg(test)]
mod mask_tests {
    use super::*;
    use crate::input::Names;

    #[test]
    fn buffered_narrow_counters_widen_exactly_across_worker_merges() {
        let new = || Counters::Columns {
            first_day: 900, days: 2, data: DenseColumns::new(6, true),
        };
        let mut left = new();
        let mut right = new();
        let value = CampaignDay { sessions: 70_000, purchasing_sessions: 68_999,
            revenue: 9_876_543_210, assisted_sessions: 123_400 };
        left.add(1, 901, value);
        right.add(1, 901, value);
        for _ in 0..32 { right.add(2, 900, CampaignDay::assists(2048)); }
        let values: CampaignMap = left.merge(right).entries().collect();
        assert_eq!(values.len(), 2);
        let combined = values[&(1, 901)];
        assert_eq!((combined.sessions, combined.purchasing_sessions, combined.revenue,
                    combined.assisted_sessions), (140_000, 137_998, 19_753_086_420, 246_800));
        assert_eq!(values[&(2,900)].assisted_sessions, 65_536);
    }

    #[test]
    fn masks_match_brute_force_for_unsorted_clicks_and_full_64th_bit() {
        let ids = ClickIds::new(Names::default(), vec![]);
        let sessions: Vec<_> = (0..64).map(|i| Session {
            person: 1,
            time: i * DAY,
            revenue: i % 3,
        }).collect();
        let clicks: Vec<_> = [(-LOOKBACK, 21, 1), (-LOOKBACK - 1, 20, 2),
            (0, 14, 4), (0, 9, 3), (0, 9, 3), (1, 8, 2), (63 * DAY, 2, 1)]
            .into_iter().rev().map(|(time, id, campaign)| Click { time, id, person: 1, campaign }).collect();
        let snapshot = |totals: Counters| {
            let mut values: Vec<_> = totals.into_rows().into_iter().map(|(key, c)|
                (key, c.sessions, c.purchasing_sessions, c.revenue, c.assisted_sessions)).collect();
            values.sort_unstable();
            values
        };
        let mut reference = Counters::Sparse(Default::default());
        for session in &sessions {
            let valid: Vec<_> = clicks.iter().filter(|c|
                c.time <= session.time && c.time >= session.time - LOOKBACK).collect();
            let best = valid.iter().min_by_key(|c| (std::cmp::Reverse(c.time), c.id));
            reference.add(best.map_or(0, |c| c.campaign), day(session.time),
                          CampaignDay::credit(session, false));
            let mut campaigns: Vec<_> = valid.iter().map(|c| c.campaign).collect();
            campaigns.sort_unstable();
            campaigns.dedup();
            for c in campaigns { reference.add(c, day(session.time), CampaignDay::assists(1)); }
        }
        let expected = snapshot(reference);
        for mode in [AssistMode::Masks, AssistMode::Unsorted, AssistMode::Simd] {
            let mut result = Counters::Sparse(Default::default());
            small_attribution(&sessions, &clicks, &ids, &mut result, &mut AssistScratch::default(), true, mode);
            assert_eq!(snapshot(result), expected);
        }
    }
}

#[cfg(test)]
mod compact_session_tests {
    use super::*;
    use crate::records::Purchase;

    #[test]
    fn active_buckets_map_real_purchases_and_leave_sparse_fallback_intact() {
        let gap = SESSION_GAP;
        let original = [3*gap+2, 0, gap, gap, 4*gap+2, 3*gap+2];
        let mut times = original;
        let mut bins = Vec::new();
        let mut starts = Vec::new();
        assert!(!active_binned_starts(&mut times, &mut bins, &mut starts, 1));
        assert_eq!(times, original);
        assert!(active_binned_starts(&mut times, &mut bins, &mut starts, 6));
        assert_eq!(starts, [0,3,6]);
        for &time in &original {
            assert_eq!(bins[(time / gap) as usize].count, u32::from(time > gap));
        }
    }

    #[test]
    fn dense_category_amounts_widen_and_ties_preserve_category_byte_order() {
        let mut sums = DayPurchases { dense: true, ..Default::default() };
        let mut revenue = 0;
        for c in [50, 3] {
            sums.add(Purchase { category:c, cents:u32::MAX }, &mut revenue);
            sums.add(Purchase { category:c, cents:20 }, &mut revenue);
        }
        assert_eq!(sums.top(), Some(3));
        assert_eq!(sums.top(), None);
        sums.add(Purchase { category:50, cents:1 }, &mut revenue);
        assert_eq!(sums.top(), Some(50));
    }

    #[cfg(feature = "large-input")]
    #[test]
    fn bins_match_sorted_session_starts_and_counts() {
        let gap = SESSION_GAP;
        let mut times = [6 * gap, gap, 0, 2 * gap + 1, gap, 6 * gap - 1, 3 * gap + 1];
        let mut sorted = times;
        sorted.sort_unstable();
        let mut reference = Vec::new();
        starts(&sorted, &mut reference);
        let summary = |times: &[u32], positions: &[usize]| {
            positions
                .windows(2)
                .map(|w| (times[w[0]], w[1] - w[0]))
                .collect::<Vec<_>>()
        };
        let mut bins = Vec::new();
        let mut positions = Vec::new();
        assert!(binned_starts(&mut times, &mut bins, &mut positions));
        assert_eq!(summary(&times, &positions), summary(&sorted, &reference));

        let mut wide = [u32::MAX, 0, gap];
        assert!(!binned_starts(&mut wide, &mut bins, &mut positions));
        assert_eq!(wide, [u32::MAX, 0, gap]);
    }

    fn daily(batch: &SessionBatch) -> Vec<(u32, i32, i64, i64, i64, Option<u32>)> {
        (0..batch.daily.person.len())
            .map(|i| {
                (
                    batch.daily.person[i],
                    batch.daily.day[i],
                    batch.daily.sessions[i],
                    batch.daily.events[i],
                    batch.daily.revenue[i],
                    batch.daily.category[i],
                )
            })
            .collect()
    }
    fn sessions(batch: &SessionBatch) -> Vec<(u32, i64, i64)> {
        batch
            .sessions
            .iter()
            .map(|s| (s.person, s.time, s.revenue))
            .collect()
    }
    #[test]
    fn grouped_sales_match_full_sort_across_gaps_midnight_and_endpoint_ties() {
        let start = 7 * DAY - 111;
        let shard = 3;
        let mut times = Vec::new();
        let mut sales = Vec::new();
        let mut ends = Vec::new();
        let mut expanded: Batch<Event> = Batch::default();
        for group in 0..4 {
            let person = (group * crate::input::SHARDS + shard) as u32;
            let mut offsets: Vec<u32> = (0..108)
                .map(|i| match group {
                    0 => (i % 3) * SESSION_GAP,
                    1 => (i * 31 % 12) * (SESSION_GAP + 1) + (i % 7),
                    2 => u32::MAX - ((i * 71 % 85) * (SESSION_GAP + 1) + i),
                    _ => (i * (SESSION_GAP + 1) + i) / 7,
                })
                .collect();
            offsets.extend([
                0,
                111,
                SESSION_GAP,
                (SESSION_GAP + 1),
                u32::MAX,
                u32::MAX - SESSION_GAP,
            ]);
            for (n, t) in offsets.into_iter().rev().enumerate() {
                let purchase = if n % 3 == 0 {
                    let p = Purchase {
                        category: (n % 4) as u32,
                        cents: (9 + n % 7) as u32,
                    };
                    sales.push((t, p));
                    expanded.purchases.push(p);
                    expanded.purchases.len() as u32
                } else {
                    0
                };
                times.push(t);
                expanded.rows.push(Event {
                    person,
                    time: start + i64::from(t),
                    purchase,
                });
            }
            ends.push((times.len() as u32, sales.len() as u32));
        }
        expanded.rows.sort_unstable_by_key(|e| (e.person, e.time));
        let expected = sessionize(expanded);
        let actual = compact_sessions(shard, start, &mut times, &mut sales, ends);
        assert_eq!(daily(&actual), daily(&expected));
        assert_eq!(sessions(&actual), sessions(&expected));
    }

    #[test]
    fn clock_partition_and_gap_boundaries_match_scalar() {
        let mut scratch = Vec::new();
        let mut positions = Vec::new();
        for count in [1, 8, 9, 31, 256, 2048, 6000, 131_073] {
            let mut values: Vec<_> = (0..count as u32)
                .map(|i| i.wrapping_mul(0x350c_619f).rotate_left(12) % 3_000_000_000)
                .collect();
            values.extend([0, SESSION_GAP, SESSION_GAP * 2 + 1, u32::MAX]);
            let mut expected = values.clone();
            expected.sort_unstable();
            sorted_time_offsets(&mut values, &mut scratch);
            assert_eq!(values, expected);
            starts(&values, &mut positions);
            let expected: Vec<_> = std::iter::once(0)
                .chain((1..values.len()).filter(|&i| values[i] - values[i - 1] > SESSION_GAP))
                .chain(std::iter::once(values.len()))
                .collect();
            assert_eq!(positions, expected);
        }
    }

    #[test]
    fn compact_offsets_match_full_events_at_gaps_midnight_and_purchase_ties() {
        let start = DAY - 500;
        let shard = 7;
        let mut times = Vec::new();
        let mut sales = Vec::new();
        let mut ends = Vec::new();
        let mut expanded: Batch<Event> = Batch::default();
        let data: &[&[(u32, Option<(u32, u32)>)]] = &[
            &[
                (1_800_501, Some((1, 50))),
                (500, Some((2, 75))),
                (0, None),
                (500, Some((1, 25))),
                (3_600_501, None),
                (5_400_502, None),
            ],
            &[],
            &[(11, Some((7, 10))), (11, Some((6, 10)))],
        ];
        for (group, events) in data.iter().enumerate() {
            let person = (group * crate::input::SHARDS + shard) as u32;
            for &(t, p) in *events {
                times.push(t);
                let purchase = p.map_or(0, |(category, cents)| {
                    let p = Purchase { category, cents };
                    sales.push((t, p));
                    expanded.purchases.push(p);
                    expanded.purchases.len() as u32
                });
                expanded.rows.push(Event {
                    person,
                    time: start + i64::from(t),
                    purchase,
                });
            }
            ends.push((times.len() as u32, sales.len() as u32));
        }
        expanded.rows.sort_unstable_by_key(|e| (e.person, e.time));
        let expected = sessionize(expanded);
        let actual = compact_sessions(shard, start, &mut times, &mut sales, ends);
        assert_eq!(daily(&actual), daily(&expected));
        assert_eq!(sessions(&actual), sessions(&expected));
    }
}
