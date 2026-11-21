use std::cmp::Ordering;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use rayon::prelude::*;

use crate::csv::{chunks, fields};
use crate::input::{
    self, Aliases, DAY, Dimensions, LOOKBACK, Names, PersonCache, SHARDS, Window, text, timestamp,
    unsigned,
};

#[path = "polars_dedup.rs"]
mod polars_dedup;
pub use polars_dedup::{dedup_polars_events, prepare_polars_events, read_polars_events};

const EXTERNAL_ID: u64 = 1 << 63;
pub(crate) const MAX_INLINE_ID: usize = 12;
const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const INVALID_PERSON: u32 = u32::MAX;

fn id_for(id: &[u8], fallback: &Names) -> u64 {
    if id.len() <= MAX_INLINE_ID {
        let mut value = 0;
        for &byte in id {
            let digit = match byte {
                b'0'..=b'9' => byte - b'0' + 1,
                b'a'..=b'z' => byte - b'a' + 11,
                _ => return EXTERNAL_ID | u64::from(fallback.intern(text(id))),
            };
            value = value * 37 + u64::from(digit);
        }
        // Zero is padding. Every byte in the alphabet maps to a nonzero digit.
        return value * 37_u64.pow((MAX_INLINE_ID - id.len()) as u32);
    }
    EXTERNAL_ID | u64::from(fallback.intern(text(id)))
}

pub struct ClickIds(Vec<Vec<Arc<str>>>);

impl ClickIds {
    pub fn compare(&self, left: u64, right: u64) -> Ordering {
        if left < EXTERNAL_ID && right < EXTERNAL_ID {
            return left.cmp(&right);
        }
        let (mut left_buffer, mut right_buffer) = ([0; MAX_INLINE_ID], [0; MAX_INLINE_ID]);
        self.bytes(left, &mut left_buffer)
            .cmp(self.bytes(right, &mut right_buffer))
    }

    pub(crate) fn bytes<'a>(&'a self, id: u64, buffer: &'a mut [u8; MAX_INLINE_ID]) -> &'a [u8] {
        if id >= EXTERNAL_ID {
            return input::name_for(&self.0, id as u32).as_bytes();
        }
        let mut code = id;
        let mut length = MAX_INLINE_ID;
        for (position, byte) in buffer.iter_mut().enumerate().rev() {
            let digit = (code % 37) as usize;
            code /= 37;
            if digit == 0 {
                length = position;
            } else {
                *byte = ALPHABET[digit - 1];
            }
        }
        &buffer[..length]
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

pub struct Candidate {
    id: u64,
    ingested: i64,
    event: Event,
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

    fn append(&mut self, batch: &mut Self, index: impl Fn(&mut T) -> &mut u32) {
        if !batch.purchases.is_empty() {
            let offset = self.purchases.len() as u32;
            for row in &mut batch.rows {
                let purchase = index(row);
                if *purchase != 0 {
                    *purchase += offset;
                }
            }
            append(&mut self.purchases, &mut batch.purchases);
        }
        append(&mut self.rows, &mut batch.rows);
    }
}

fn append<T>(destination: &mut Vec<T>, source: &mut Vec<T>) {
    if destination.capacity() - destination.len() < source.len() {
        // Keep headroom for the next chunk without doubling large vectors.
        destination.reserve_exact(source.len().max(destination.len() / 8));
    }
    destination.append(source);
}

fn shards<T: Default>() -> Vec<T> {
    (0..SHARDS).map(|_| T::default()).collect()
}

trait CandidateStore: Default + Send {
    fn push_candidate(
        &mut self,
        id: u64,
        ingested: i64,
        time: i64,
        person: u32,
        purchase: Option<Purchase>,
    );
    fn flush_into(&mut self, destination: &Mutex<Self>);
}

impl CandidateStore for Batch<Candidate> {
    fn push_candidate(
        &mut self,
        id: u64,
        ingested: i64,
        time: i64,
        person: u32,
        purchase: Option<Purchase>,
    ) {
        let purchase = self.add_purchase(purchase);
        self.rows.push(Candidate {
            id,
            ingested,
            event: Event {
                time,
                person,
                purchase,
            },
        });
    }

    fn flush_into(&mut self, destination: &Mutex<Self>) {
        if !self.rows.is_empty() {
            destination
                .lock()
                .unwrap()
                .append(self, |row| &mut row.event.purchase);
        }
    }
}

pub fn read_events(
    data: &Path,
    window: Window,
    dimensions: &Dimensions,
    users: &Names,
    aliases: &Aliases,
) -> Result<Vec<Batch<Candidate>>> {
    read_candidates(data, window, dimensions, users, aliases)
}

fn read_candidates<T: CandidateStore>(
    data: &Path,
    window: Window,
    dimensions: &Dimensions,
    users: &Names,
    aliases: &Aliases,
) -> Result<Vec<T>> {
    let ids = Names::default();
    let shared = shards::<Mutex<T>>();
    let worker_state = || (PersonCache::default(), shards::<T>());
    chunks(&data.join("events"))?
        .into_par_iter()
        .try_for_each_init(worker_state, |(people, local), chunk| {
            chunk.scan(|line| {
                let Some([id, user, ts, ingested, kind, product, price]) = fields(line) else {
                    return;
                };
                if id.is_empty() {
                    return;
                }
                let ingested = timestamp(ingested).expect("ingest_ts is valid F1");
                let id = id_for(id, &ids);
                // Mix high and low bits because short IDs have trailing base-37 padding.
                let shard = ((id ^ (id >> 32)).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 57) as usize;
                let batch = &mut local[shard];
                let mut valid = || {
                    if user.is_empty() || product.is_empty() {
                        return None;
                    }
                    let time = timestamp(ts)?;
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
                    Some((time, people.resolve(users, aliases, user), purchase))
                };
                // Invalid candidates still compete in deduplication. The earliest copy wins.
                let (time, person, purchase) = valid().unwrap_or((0, INVALID_PERSON, None));
                batch.push_candidate(id, ingested, time, person, purchase);
            })?;
            for (destination, batch) in shared.iter().zip(local) {
                batch.flush_into(destination);
            }
            Ok::<_, anyhow::Error>(())
        })?;
    Ok(shared
        .into_iter()
        .map(|batch| batch.into_inner().unwrap())
        .collect())
}

pub fn dedup_events(candidates: Vec<Batch<Candidate>>) -> (Vec<Batch<Event>>, usize) {
    let shared = shards::<Mutex<Batch<Event>>>();
    let count = candidates
        .into_par_iter()
        .map(|mut batch| {
            batch
                .rows
                .par_sort_unstable_by_key(|row| (row.id, row.ingested));
            batch.rows.dedup_by_key(|row| row.id);
            let mut local = shards::<Batch<Event>>();
            let mut count = 0;
            // Flush in batches so concurrent workers limit rebucketing memory.
            for chunk in batch.rows.chunks(100_000) {
                for row in chunk {
                    let mut event = row.event;
                    if event.person == INVALID_PERSON {
                        continue;
                    }
                    let destination = &mut local[event.person as usize % SHARDS];
                    let purchase = event
                        .purchase
                        .checked_sub(1)
                        .map(|at| batch.purchases[at as usize]);
                    event.purchase = destination.add_purchase(purchase);
                    destination.rows.push(event);
                    count += 1;
                }
                for (destination, batch) in shared.iter().zip(&mut local) {
                    if !batch.rows.is_empty() {
                        destination
                            .lock()
                            .unwrap()
                            .append(batch, |row| &mut row.purchase);
                    }
                }
            }
            count
        })
        .sum();
    (
        shared
            .into_iter()
            .map(|batch| batch.into_inner().unwrap())
            .collect(),
        count,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Click {
    pub time: i64,
    pub id: u64,
    pub person: u32,
    pub campaign: u32,
}

pub fn read_clicks(
    data: &Path,
    window: Window,
    dimensions: &Dimensions,
    users: &Names,
    aliases: &Aliases,
) -> Result<(Vec<Vec<Click>>, ClickIds)> {
    let ids = Names::default();
    let shared = shards::<Mutex<Vec<Click>>>();
    let worker_state = || (PersonCache::default(), shards::<Vec<Click>>());
    chunks(&data.join("clicks"))?
        .into_par_iter()
        .try_for_each_init(worker_state, |(people, local), chunk| {
            chunk.scan(|line| {
                let Some([id, user, campaign, ts]) = fields(line) else {
                    return;
                };
                if id.is_empty() || user.is_empty() {
                    return;
                }
                let Some(&campaign) = dimensions.campaign_ids.get(text(campaign)) else {
                    return;
                };
                let Some(time) = timestamp(ts) else {
                    return;
                };
                if time < window.start - LOOKBACK || time >= window.end {
                    return;
                }
                let person = people.resolve(users, aliases, user);
                let id = id_for(id, &ids);
                local[person as usize % SHARDS].push(Click {
                    time,
                    id,
                    person,
                    campaign,
                });
            })?;
            for (destination, batch) in shared.iter().zip(local) {
                if !batch.is_empty() {
                    append(&mut destination.lock().unwrap(), batch);
                }
            }
            Ok::<_, anyhow::Error>(())
        })?;
    let clicks = shared
        .into_par_iter()
        .map(|shard| {
            let mut clicks = shard.into_inner().unwrap();
            clicks.par_sort_unstable_by_key(|click| {
                (click.person, click.time, click.campaign, click.id)
            });
            // Copies of a click have identical values, including validity.
            clicks.dedup();
            clicks
        })
        .collect();
    Ok((clicks, ClickIds(ids.into_names())))
}
