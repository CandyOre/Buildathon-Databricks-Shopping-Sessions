use std::cmp::Ordering;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use rayon::prelude::*;

use crate::csv::{chunks, fields};
use crate::input::{
    self, Aliases, Dimensions, LOOKBACK, Names, PersonCache, SHARDS, Window, text, timestamp,
};

const EXTERNAL_ID: u64 = 1 << 63;
const MAX_INLINE_ID: usize = 12;
const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

pub(crate) fn id_for(id: &[u8], fallback: &Names) -> u64 {
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

    fn bytes<'a>(&'a self, id: u64, buffer: &'a mut [u8; MAX_INLINE_ID]) -> &'a [u8] {
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
    pub(crate) fn add_purchase(&mut self, purchase: Option<Purchase>) -> u32 {
        purchase.map_or(0, |value| {
            self.purchases.push(value);
            u32::try_from(self.purchases.len()).expect("too many purchases in one shard")
        })
    }
}

pub(crate) fn append<T>(destination: &mut Vec<T>, source: &mut Vec<T>) {
    if destination.capacity() - destination.len() < source.len() {
        // Keep headroom for the next chunk without doubling large vectors.
        destination.reserve_exact(source.len().max(destination.len() / 8));
    }
    destination.append(source);
}

pub(crate) fn shards<T: Default>() -> Vec<T> {
    (0..SHARDS).map(|_| T::default()).collect()
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
