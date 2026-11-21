use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use ahash::{AHashMap, RandomState};
use anyhow::{Context, Result};
use chrono::NaiveDate;
use rayon::prelude::*;
use serde_json::Value;

use crate::csv::{fields, parts, scan_file};

pub const DAY: i64 = 86_400_000;
pub const LOOKBACK: i64 = 7 * DAY;
pub const SHARDS: usize = 128;

#[derive(Clone, Copy)]
pub struct Window {
    pub start: i64,
    pub end: i64,
}

impl Window {
    pub fn read(data: &Path) -> Result<Self> {
        let manifest: Value = serde_json::from_reader(File::open(data.join("manifest.json"))?)?;
        let field = |key: &str| {
            manifest[key]
                .as_str()
                .and_then(|s| timestamp(s.as_bytes()))
                .with_context(|| format!("invalid manifest {key}"))
        };
        Ok(Self {
            start: field("window_start")?,
            end: field("window_end")?,
        })
    }
}

pub fn unsigned(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    // Every unsigned decimal with at most 18 digits fits in i64.
    if bytes.len() <= 18 {
        let mut value = 0;
        for &byte in bytes {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            value = value * 10 + i64::from(digit);
        }
        return Some(value);
    }
    let mut value = 0_i64;
    for &byte in bytes {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
    }
    Some(value)
}

fn two_digits(tens: u8, units: u8) -> Option<u32> {
    let tens = tens.wrapping_sub(b'0');
    let units = units.wrapping_sub(b'0');
    if tens > 9 || units > 9 {
        return None;
    }
    Some(u32::from(tens) * 10 + u32::from(units))
}

fn midnight(date: &[u8; 10]) -> Option<i64> {
    // Most rows in a chunk share a small set of calendar dates. A slot is keyed
    // by the full date so other months, years and malformed dates cannot alias it.
    type Entry = std::cell::Cell<Option<([u8; 10], i64)>>;
    thread_local! {
        static MIDNIGHTS: [Entry; 32] = const { [const { Entry::new(None) }; 32] };
    }
    let day = two_digits(date[8], date[9])?;
    MIDNIGHTS.with(|cache| {
        let slot = &cache[day as usize % cache.len()];
        if let Some((key, value)) = slot.get() {
            if key == *date {
                return Some(value);
            }
        }
        if date[4] != b'-' || date[7] != b'-' {
            return None;
        }
        let year = two_digits(date[0], date[1])? * 100 + two_digits(date[2], date[3])?;
        let month = two_digits(date[5], date[6])?;
        let value = NaiveDate::from_ymd_opt(year as i32, month, day)?
            .and_hms_opt(0, 0, 0)?
            .and_utc()
            .timestamp_millis();
        slot.set(Some((*date, value)));
        Some(value)
    })
}

pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    let offset_minutes = match bytes {
        [.., b'Z'] if bytes.len() == 20 => 0,
        [
            ..,
            sign @ (b'+' | b'-'),
            hour_a,
            hour_b,
            b':',
            minute_a,
            minute_b,
        ] if bytes.len() == 25 => {
            let hours = two_digits(*hour_a, *hour_b)?;
            let minutes = two_digits(*minute_a, *minute_b)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            (if *sign == b'+' { 1 } else { -1 }) * i64::from(hours * 60 + minutes)
        }
        _ => return unsigned(bytes),
    };
    if bytes[10] != b'T' || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let hour = two_digits(bytes[11], bytes[12])?;
    let minute = two_digits(bytes[14], bytes[15])?;
    let second = two_digits(bytes[17], bytes[18])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let start = midnight(bytes[..10].try_into().ok()?)?;
    let seconds = i64::from(hour * 3600 + minute * 60 + second);
    Some(start + seconds * 1000 - offset_minutes * 60_000)
}

#[cfg(test)]
mod timestamp_tests {
    use super::{DAY, timestamp, unsigned};

    #[test]
    fn cached_midnights_distinguish_months_years_and_invalid_dates() {
        let leap_day = timestamp(b"2028-02-29T00:00:00Z").unwrap();
        assert_eq!(timestamp(b"2028-02-29T01:00:00+01:00"), Some(leap_day));
        assert_eq!(timestamp(b"2027-02-29T00:00:00Z"), None);
        assert_eq!(timestamp(b"2028/02-29T00:00:00Z"), None);
        assert_eq!(
            timestamp(b"2028-03-29T00:00:00Z"),
            Some(leap_day + 29 * DAY)
        );
        assert_eq!(timestamp(b"2028-02-29T00:00:00Z"), Some(leap_day));
    }

    #[test]
    fn epoch_numbers_support_padding_and_reject_overflow() {
        assert_eq!(unsigned(b"00000000000000000000000000001"), Some(1));
        assert_eq!(unsigned(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(unsigned(b"9223372036854775808"), None);
        assert_eq!(unsigned(b"00000000000000:1"), None);
    }
}

pub fn text(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).expect("input fields must be UTF-8")
}

pub struct Dimensions {
    pub categories: Vec<String>,
    pub products: AHashMap<String, u32>,
    pub unknown_category: u32,
    pub campaigns: Vec<(String, String)>,
    pub campaign_ids: AHashMap<String, u32>,
}

impl Dimensions {
    pub fn read(data: &Path) -> Result<Self> {
        let mut pairs = Vec::new();
        scan_file(&data.join("products.csv"), |row| {
            if let Some([product, category]) = fields(row) {
                pairs.push((text(product).to_owned(), text(category).to_owned()));
            }
        })?;
        let mut categories: Vec<_> = pairs.iter().map(|(_, category)| category.clone()).collect();
        categories.push("unknown".into());
        categories.sort_unstable();
        categories.dedup();
        let unknown_category = categories
            .binary_search_by(|s| s.as_str().cmp("unknown"))
            .unwrap() as u32;
        let products = pairs
            .into_iter()
            .map(|(product, category)| {
                (product, categories.binary_search(&category).unwrap() as u32)
            })
            .collect();

        let mut campaigns = vec![("organic".into(), "organic".into())];
        scan_file(&data.join("campaigns.csv"), |row| {
            if let Some([campaign, channel]) = fields(row) {
                campaigns.push((text(campaign).to_owned(), text(channel).to_owned()));
            }
        })?;
        let campaign_ids = campaigns
            .iter()
            .enumerate()
            .skip(1)
            .map(|(id, (campaign, _))| (campaign.clone(), id as u32))
            .collect();
        Ok(Self {
            categories,
            products,
            unknown_category,
            campaigns,
            campaign_ids,
        })
    }
}

#[derive(Default)]
struct NameShard {
    ids: AHashMap<Arc<str>, u32>,
    names: Vec<Arc<str>>,
}

impl NameShard {
    fn intern(&mut self, shard: usize, name: &str) -> u32 {
        if let Some(&id) = self.ids.get(name) {
            return id;
        }
        let id = id_for(shard, self.names.len());
        let name: Arc<str> = name.into();
        self.ids.insert(name.clone(), id);
        self.names.push(name);
        id
    }
}

pub struct Names {
    hash: RandomState,
    shards: Vec<Mutex<NameShard>>,
}

impl Default for Names {
    fn default() -> Self {
        Self {
            hash: RandomState::new(),
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
        }
    }
}

impl Names {
    pub fn intern(&self, name: &str) -> u32 {
        let shard = self.hash.hash_one(name.as_bytes()) as usize % SHARDS;
        self.intern_in(shard, name)
    }

    fn intern_in(&self, shard: usize, name: &str) -> u32 {
        self.shards[shard].lock().unwrap().intern(shard, name)
    }

    fn intern_exclusive(&mut self, name: &str) -> u32 {
        let shard = self.hash.hash_one(name.as_bytes()) as usize % SHARDS;
        self.shards[shard].get_mut().unwrap().intern(shard, name)
    }

    pub fn into_names(self) -> Vec<Vec<Arc<str>>> {
        self.shards
            .into_par_iter()
            .map(|s| s.into_inner().unwrap().names)
            .collect()
    }
}

fn id_for(shard: usize, slot: usize) -> u32 {
    u32::try_from(slot * SHARDS + shard)
        .ok()
        .filter(|&id| id != u32::MAX)
        .expect("too many distinct names")
}

pub fn name_for<T: AsRef<[Arc<str>]>>(names: &[T], id: u32) -> &str {
    let id = id as usize;
    &names[id % SHARDS].as_ref()[id / SHARDS]
}

#[derive(Default)]
struct Links {
    parents: Vec<u32>,
    sizes: Vec<u32>,
}

impl Links {
    fn grow(&mut self, id: u32) {
        let count = id as usize + 1;
        if self.parents.len() < count {
            self.parents.extend(self.parents.len() as u32..count as u32);
            self.sizes.resize(count, 1);
        }
    }

    fn root(&mut self, mut id: u32) -> u32 {
        while self.parents[id as usize] != id {
            let parent = self.parents[id as usize];
            self.parents[id as usize] = self.parents[parent as usize];
            id = self.parents[id as usize];
        }
        id
    }

    fn join(&mut self, left: u32, right: u32) {
        self.grow(left.max(right));
        let (mut left, mut right) = (self.root(left), self.root(right));
        if left == right {
            return;
        }
        if self.sizes[left as usize] < self.sizes[right as usize] {
            std::mem::swap(&mut left, &mut right);
        }
        self.parents[right as usize] = left;
        self.sizes[left as usize] += self.sizes[right as usize];
    }
}

pub struct Aliases(Vec<AHashMap<Arc<str>, u32>>);

pub fn read_aliases(data: &Path, users: &mut Names) -> Result<Aliases> {
    let mut links = Links::default();
    for path in parts(&data.join("links"))? {
        scan_file(&path, |row| {
            if let Some([left, right]) = fields(row) {
                if !left.is_empty() && !right.is_empty() && left != right {
                    links.join(
                        users.intern_exclusive(text(left)),
                        users.intern_exclusive(text(right)),
                    );
                }
            }
        })?;
    }
    let names: Vec<_> = users
        .shards
        .iter_mut()
        .map(|s| s.get_mut().unwrap().names.as_slice())
        .collect();
    let mut minimum = vec![u32::MAX; links.parents.len()];
    for (shard, list) in names.iter().enumerate() {
        for (slot, name) in list.iter().enumerate() {
            let id = id_for(shard, slot);
            let root = links.root(id);
            links.parents[id as usize] = root;
            let best = &mut minimum[root as usize];
            if *best == u32::MAX || name.as_ref() < name_for(&names, *best) {
                *best = id;
            }
        }
    }
    // The link graph is complete. Move the existing key tables into a read-only
    // index, so all workers can resolve linked names without locking a shard.
    let aliases = users
        .shards
        .par_iter_mut()
        .map(|shard| {
            let mut ids = std::mem::take(&mut shard.get_mut().unwrap().ids);
            for person in ids.values_mut() {
                *person = minimum[links.parents[*person as usize] as usize];
            }
            ids
        })
        .collect();
    Ok(Aliases(aliases))
}

#[derive(Clone, Copy, Default)]
struct CachedPerson {
    bytes: [u8; 16],
    len: u8,
    person: u32,
}

pub struct PersonCache(Vec<CachedPerson>);

impl Default for PersonCache {
    fn default() -> Self {
        Self(vec![CachedPerson::default(); 16_384])
    }
}

impl PersonCache {
    pub fn resolve(&mut self, users: &Names, aliases: &Aliases, bytes: &[u8]) -> u32 {
        debug_assert!(!bytes.is_empty());
        let hash = users.hash.hash_one(bytes) as usize;
        let shard = hash % SHARDS;
        let slot = (hash / SHARDS) % self.0.len();
        let entry = &mut self.0[slot];
        if bytes.len() == usize::from(entry.len) && bytes == &entry.bytes[..bytes.len()] {
            return entry.person;
        }
        let name = text(bytes);
        let person = aliases.0[shard]
            .get(name)
            .copied()
            .unwrap_or_else(|| users.intern_in(shard, name));
        if bytes.len() <= entry.bytes.len() {
            entry.bytes[..bytes.len()].copy_from_slice(bytes);
            entry.len = bytes.len() as u8;
            entry.person = person;
        }
        person
    }
}

#[cfg(test)]
mod person_tests {
    use super::*;

    #[test]
    fn local_cache_keeps_linked_and_unlinked_ids_exact_on_collision() {
        let users = Names::default();
        let canonical = users.intern("canonical");
        let mut aliases = Aliases((0..SHARDS).map(|_| AHashMap::default()).collect());
        let shard = users.hash.hash_one(b"alias".as_slice()) as usize % SHARDS;
        aliases.0[shard].insert(Arc::from("alias"), canonical);
        let mut cache = PersonCache(vec![CachedPerson::default(); 1]);
        for name in [
            b"x".as_slice(),
            b"y",
            b"x",
            b"0123456789abcdefx",
            b"0123456789abcdefy",
        ] {
            assert_eq!(cache.resolve(&users, &aliases, b"alias"), canonical);
            assert_eq!(
                cache.resolve(&users, &aliases, name),
                users.intern(text(name))
            );
        }
    }
}
