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

#[derive(Default)]
pub struct CampaignDay {
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue: i64,
    pub assisted_sessions: i64,
}

pub type CampaignDays = AHashMap<(u32, i32), CampaignDay>;

pub fn campaign_daily(
    batches: &mut [SessionBatch],
    clicks: Vec<Vec<Click>>,
    ids: ClickIds,
) -> CampaignDays {
    batches
        .par_iter_mut()
        .zip(clicks)
        .map(|(batch, clicks)| {
            summarize_campaigns(std::mem::take(&mut batch.sessions), &clicks, &ids)
        })
        .reduce(CampaignDays::default, |mut left, right| {
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
) -> CampaignDays {
    let mut totals = CampaignDays::default();
    let mut intervals = AHashMap::<u32, (i64, i64)>::new();
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

        credit(sessions, person_clicks, ids, &mut totals);
        for click in person_clicks {
            let interval = intervals
                .entry(click.campaign)
                .or_insert((click.time, click.time + LOOKBACK));
            if click.time > interval.1 {
                assist(sessions, click.campaign, *interval, &mut totals);
                interval.0 = click.time;
            }
            interval.1 = click.time + LOOKBACK;
        }
        for (campaign, interval) in intervals.drain() {
            assist(sessions, campaign, interval, &mut totals);
        }
    }
    totals
}

fn credit(sessions: &[Session], clicks: &[Click], ids: &ClickIds, totals: &mut CampaignDays) {
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
        let row = totals.entry((campaign, day(session.time))).or_default();
        row.sessions += 1;
        row.purchasing_sessions += i64::from(session.revenue > 0);
        row.revenue += session.revenue;
    }
}

fn assist(
    sessions: &[Session],
    campaign: u32,
    (first, last): (i64, i64),
    totals: &mut CampaignDays,
) {
    let start = sessions.partition_point(|session| session.time < first);
    let end = sessions.partition_point(|session| session.time <= last);
    let mut covered = &sessions[start..end];
    // Count by day without materializing a row for each matching session/campaign.
    while let Some(session) = covered.first() {
        let current_day = day(session.time);
        let next_day = (i64::from(current_day) + 1) * DAY;
        let count = covered.partition_point(|session| session.time < next_day);
        totals
            .entry((campaign, current_day))
            .or_default()
            .assisted_sessions += count as i64;
        covered = &covered[count..];
    }
}
