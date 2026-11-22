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
    SessionBatch { daily, sessions }
}

const SESSION_GAP: u32 = 1_800_000;

#[cfg(feature = "large-input")]
fn purchase_bins() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("PERF_PURCHASE_BINS").is_none_or(|v| v != "0"))
}

fn use_radix_time_sort() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("PERF_TIME_SORT").is_none_or(|v| v != "0"))
}

#[cfg(feature = "large-input")]
#[derive(Clone, Copy)]
struct TimeBin {
    first: u32,
    last: u32,
    count: u32,
}

#[cfg(feature = "large-input")]
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
}

impl DayPurchases {
    fn add(&mut self, purchase: crate::records::Purchase, revenue: &mut i64) {
        let amount = i64::from(purchase.cents);
        *revenue += amount;
        let first = self.first.get_or_insert((purchase.category, 0));
        if first.0 == purchase.category {
            first.1 += amount;
        } else {
            *self.others.entry(purchase.category).or_default() += amount;
        }
    }

    fn top(&mut self) -> Option<u32> {
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
    #[cfg(feature = "large-input")]
    let mut time_bins = Vec::new();
    let mut radix = Vec::new();
    let mut heads = Vec::new();
    let mut next_purchase = Vec::new();
    let mut category = DayPurchases::default();
    let mut prev = (0, 0);
    for (index, (last, sold)) in ends.into_iter().enumerate() {
        let local = &mut times[prev.0 as usize..last as usize];
        let purchases = &sales[prev.1 as usize..sold as usize];
        prev = (last, sold);
        if local.is_empty() {
            continue;
        }
        #[cfg(feature = "large-input")]
        let binned = local.len() >= 2048 && binned_starts(local, &mut time_bins, &mut starts_at);
        #[cfg(not(feature = "large-input"))]
        let binned = false;
        if !binned {
            sorted_time_offsets(local, &mut radix);
            starts(local, &mut starts_at);
        }
        let count = starts_at.len() - 1;
        heads.clear();
        if count > 1 && !purchases.is_empty() {
            heads.resize(count, 0_u32);
            next_purchase.clear();
            for &(time, _) in purchases {
                #[cfg(feature = "large-input")]
                let bin = if binned && purchase_bins() {
                    Some(time_bins[(time / SESSION_GAP) as usize].count as usize)
                } else { None };
                #[cfg(not(feature = "large-input"))]
                let bin: Option<usize> = None;
                let s = bin.unwrap_or_else(||
                    starts_at[..count].partition_point(|&pos| local[pos] <= time) - 1);
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
    SessionBatch { daily, sessions }
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

pub type CampaignDays = AHashMap<(u32, i32), CampaignDay>;

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
        Ok("interval") => AssistMode::Intervals,
        Ok("mask") => AssistMode::Masks,
        Ok("simd") => AssistMode::Simd,
        _ => AssistMode::Unsorted,
    })
}

pub(crate) fn skip_click_sort(n: usize) -> bool {
    n <= 32 && matches!(assist_mode(), AssistMode::Unsorted | AssistMode::Simd)
}

#[derive(Default)]
struct AssistScratch {
    intervals: AHashMap<u32, (i64, i64)>,
    masks: AHashMap<u32, u64>,
}

// Allocate dense counters only when the manifest and dimension table imply a
// modest working set. The sparse representation handles longer windows.
enum Counters {
    Dense {
        first_day: i32,
        days: usize,
        rows: Vec<CampaignDay>,
    },
    Sparse(CampaignDays),
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
            Self::Dense {
                first_day,
                days,
                rows: vec![CampaignDay::default(); size],
            }
        } else {
            Self::Sparse(CampaignDays::default())
        }
    }

    fn row(&mut self, campaign: u32, day: i32) -> &mut CampaignDay {
        match self {
            Self::Dense {
                first_day,
                days,
                rows,
            } => &mut rows[campaign as usize * *days + (day - *first_day) as usize],
            Self::Sparse(rows) => rows.entry((campaign, day)).or_default(),
        }
    }

    fn into_rows(self) -> CampaignDays {
        match self {
            Self::Sparse(rows) => rows,
            Self::Dense {
                first_day,
                days,
                rows,
            } => rows
                .into_iter()
                .enumerate()
                .filter(|(_, row)| row.sessions != 0 || row.assisted_sessions != 0)
                .map(|(index, row)| {
                    (
                        ((index / days) as u32, first_day + (index % days) as i32),
                        row,
                    )
                })
                .collect(),
        }
    }
}

impl CampaignDay {
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
            _ => unreachable!("all workers use the same window and campaign dimension"),
        }
        self
    }
}

pub fn campaign_daily(
    batches: &mut [SessionBatch],
    clicks: Vec<Vec<Click>>,
    ids: ClickIds,
    campaigns: usize,
    window: crate::input::Window,
) -> CampaignDays {
    let min_len = (batches.len() / (rayon::current_num_threads() * 2)).max(1);
    batches
        .par_iter_mut()
        .zip(clicks)
        .with_min_len(min_len)
        .fold(
            || (Counters::new(campaigns, window), AssistScratch::default()),
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
        .map(|(totals, _)| totals)
        .reduce_with(Counters::merge)
        .map_or_else(CampaignDays::default, Counters::into_rows)
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
                small_attribution(sessions, person_clicks, ids, totals, &mut scratch.masks, true, mode);
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
            let click = person_clicks.first();
            for session in sessions {
                let campaign = click
                    .filter(|c| c.time <= session.time && c.time >= session.time - LOOKBACK)
                    .map_or(0, |c| c.campaign);
                let row = totals.row(campaign, day(session.time));
                row.sessions += 1;
                row.purchasing_sessions += i64::from(session.revenue > 0);
                row.revenue += session.revenue;
                row.assisted_sessions += i64::from(campaign != 0);
            }
            continue;
        }
        if sessions.len() == 1 && person_clicks.len() <= 8 {
            let session = &sessions[0];
            let current_day = day(session.time);
            let mut unique = [0u32; 8];
            let mut used = 0;
            let mut best = person_clicks.last().unwrap();
            for click in person_clicks {
                if !unique[..used].contains(&click.campaign) {
                    unique[used] = click.campaign;
                    used += 1;
                    totals.row(click.campaign, current_day).assisted_sessions += 1;
                }
                if click.time == best.time && ids.compare(click.id, best.id).is_lt() {
                    best = click;
                }
            }
            let row = totals.row(best.campaign, current_day);
            row.sessions += 1;
            row.purchasing_sessions += i64::from(session.revenue > 0);
            row.revenue += session.revenue;
            continue;
        }
        credit(sessions, person_clicks, ids, totals);
        if mode != AssistMode::Intervals && sessions.len() <= 64 {
            small_attribution(sessions, person_clicks, ids, totals, &mut scratch.masks, false, mode);
            continue;
        }
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
    masks: &mut AHashMap<u32, u64>,
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
        *masks.entry(click.campaign).or_default() |= mask;
        if find_winners {
            let mut bits = mask;
            while bits != 0 {
                let slot = &mut best[bits.trailing_zeros() as usize];
                if slot.is_none_or(|old| click.time > old.time ||
                    (click.time == old.time && ids.compare(click.id, old.id).is_lt())) {
                    *slot = Some(click);
                }
                bits &= bits - 1;
            }
        }
    }
    if find_winners {
        for (session, winner) in sessions.iter().zip(best) {
            let row = totals.row(winner.map_or(0, |c| c.campaign), day(session.time));
            row.sessions += 1;
            row.purchasing_sessions += i64::from(session.revenue > 0);
            row.revenue += session.revenue;
        }
    }
    for (campaign, mut bits) in masks.drain() {
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            let d = day(sessions[i].time);
            let midnight = (i64::from(d) + 1) * DAY;
            let end = i + sessions[i..].partition_point(|s| s.time < midnight);
            let through_day = low_bits(end);
            totals.row(campaign, d).assisted_sessions += i64::from((bits & through_day).count_ones());
            bits &= !through_day;
        }
    }
}

fn credit(sessions: &[Session], clicks: &[Click], ids: &ClickIds, totals: &mut Counters) {
    let mut clicks = clicks.iter().peekable();
    let mut latest: Option<&Click> = None;
    for session in sessions {
        while clicks
            .peek()
            .is_some_and(|click| click.time <= session.time)
        {
            let click = clicks.next().unwrap();
            if latest
                .is_none_or(|best| click.time > best.time || ids.compare(click.id, best.id).is_lt())
            {
                latest = Some(click);
            }
        }
        let campaign = latest
            .filter(|click| click.time >= session.time - LOOKBACK)
            .map_or(0, |click| click.campaign);
        let row = totals.row(campaign, day(session.time));
        row.sessions += 1;
        row.purchasing_sessions += i64::from(session.revenue > 0);
        row.revenue += session.revenue;
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
        totals.row(campaign, current_day).assisted_sessions += count as i64;
        covered = &covered[count..];
    }
}

#[cfg(test)]
mod mask_tests {
    use super::*;
    use crate::input::Names;

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
            let row = reference.row(best.map_or(0, |c| c.campaign), day(session.time));
            row.sessions += 1;
            row.revenue += session.revenue;
            row.purchasing_sessions += i64::from(session.revenue > 0);
            let mut campaigns: Vec<_> = valid.iter().map(|c| c.campaign).collect();
            campaigns.sort_unstable();
            campaigns.dedup();
            for c in campaigns { reference.row(c, day(session.time)).assisted_sessions += 1; }
        }
        let expected = snapshot(reference);
        for mode in [AssistMode::Masks, AssistMode::Unsorted, AssistMode::Simd] {
            let mut result = Counters::Sparse(Default::default());
            small_attribution(&sessions, &clicks, &ids, &mut result, &mut AHashMap::default(), true, mode);
            assert_eq!(snapshot(result), expected);
        }
    }
}

#[cfg(test)]
mod compact_session_tests {
    use super::*;
    use crate::records::Purchase;

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
