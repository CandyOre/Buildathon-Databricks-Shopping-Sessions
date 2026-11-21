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

pub struct SessionBatch {
    pub daily: Vec<UserDay>,
    sessions: Vec<Session>,
}

pub fn sessionize(batch: Batch<Event>) -> SessionBatch {
    let mut daily = Vec::new();
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

fn finish_day(mut row: UserDay, categories: &mut AHashMap<u32, i64>, daily: &mut Vec<UserDay>) {
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

// Allocate dense counters only when the manifest and dimension table imply a
// modest working set. The sparse representation handles longer windows.
enum Counters {
    Dense { first_day: i32, days: usize, rows: Vec<CampaignDay> },
    Sparse(CampaignDays),
}

impl Counters {
    fn new(campaigns: usize, window: crate::input::Window) -> Self {
        let first_day = day(window.start);
        let days = (i64::from(day(window.end.saturating_sub(1))) - i64::from(first_day) + 1).max(0) as usize;
        if let Some(size) = campaigns.checked_mul(days).filter(|&n| n > 0 && n <= 131_072) {
            Self::Dense { first_day, days, rows: vec![CampaignDay::default(); size] }
        } else {
            Self::Sparse(CampaignDays::default())
        }
    }

    fn row(&mut self, campaign: u32, day: i32) -> &mut CampaignDay {
        match self {
            Self::Dense { first_day, days, rows } => {
                &mut rows[campaign as usize * *days + (day - *first_day) as usize]
            }
            Self::Sparse(rows) => rows.entry((campaign, day)).or_default(),
        }
    }

    fn into_rows(self) -> CampaignDays {
        match self {
            Self::Sparse(rows) => rows,
            Self::Dense { first_day, days, rows } => rows.into_iter().enumerate()
                .filter(|(_, row)| row.sessions != 0 || row.assisted_sessions != 0)
                .map(|(index, row)| (((index / days) as u32, first_day + (index % days) as i32), row))
                .collect(),
        }
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
    batches.par_iter_mut().zip(clicks).with_min_len(min_len).fold(
        || (Counters::new(campaigns, window), AHashMap::default()),
        |(mut totals, mut intervals), (batch, clicks)| {
            summarize_campaigns(std::mem::take(&mut batch.sessions), &clicks, &ids, &mut totals, &mut intervals);
            (totals, intervals)
        },
    ).map(|(totals, _)| totals.into_rows()).reduce(CampaignDays::default, |mut left, right| {
        for (key, value) in right {
            let target = left.entry(key).or_default();
            target.sessions += value.sessions;
            target.purchasing_sessions += value.purchasing_sessions;
            target.revenue += value.revenue;
            target.assisted_sessions += value.assisted_sessions;
        }
        left
    })
}

fn summarize_campaigns(
    sessions: Vec<Session>,
    mut clicks: &[Click],
    ids: &ClickIds,
    totals: &mut Counters,
    intervals: &mut AHashMap<u32, (i64, i64)>,
) {
    for sessions in sessions.chunk_by(|a, b| a.person == b.person) {
        let person = sessions[0].person;
        while clicks.first().is_some_and(|click| click.person < person) {
            clicks = &clicks[1..];
        }
        let end = clicks
            .iter()
            .take_while(|click| click.person == person)
            .count();
        let person_clicks = &clicks[..end];
        clicks = &clicks[end..];

        let start_time = sessions[0].time;
        let end_time = sessions.last().unwrap().time;
        let start = person_clicks.partition_point(|click| click.time < start_time - LOOKBACK);
        let end = person_clicks.partition_point(|click| click.time <= end_time);
        let person_clicks = &person_clicks[start..end];

        credit(sessions, person_clicks, ids, totals);
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

fn assist(
    sessions: &[Session],
    campaign: u32,
    (first, last): (i64, i64),
    totals: &mut Counters,
) {
    let start = sessions.partition_point(|session| session.time < first);
    let end = sessions.partition_point(|session| session.time <= last);
    let mut covered = &sessions[start..end];
    // Count by day without materializing a row for each matching session/campaign.
    while let Some(session) = covered.first() {
        let current_day = day(session.time);
        let next_day = (i64::from(current_day) + 1) * DAY;
        let count = covered.partition_point(|session| session.time < next_day);
        totals.row(campaign, current_day)
            .assisted_sessions += count as i64;
        covered = &covered[count..];
    }
}
