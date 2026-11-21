use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use ahash::{AHashMap, RandomState};
use anyhow::{Context, Result};
use chrono::NaiveDate;
use rayon::prelude::*;
use serde_json::Value;

use crate::csv::{chunks, fields, scan_file};

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
    offset: usize,
    ids: AHashMap<Arc<str>, u32>,
    names: Vec<Arc<str>>,
}

impl NameShard {
    fn intern(&mut self, shard: usize, name: &str) -> u32 {
        if let Some(&id) = self.ids.get(name) {
            return id;
        }
        let id = id_for(shard, self.offset + self.names.len());
        let name: Arc<str> = name.into();
        self.ids.insert(name.clone(), id);
        self.names.push(name);
        id
    }
}

pub struct Names {
    packed: Option<Arc<NameTable>>,
    hash: RandomState,
    shards: Vec<Mutex<NameShard>>,
}

impl Default for Names {
    fn default() -> Self {
        Self {
            packed: None,
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

    pub fn into_names(self) -> Vec<Vec<Arc<str>>> {
        self.shards.into_iter().map(|s| s.into_inner().unwrap().names).collect()
    }

    pub fn into_people(mut self) -> PersonNames {
        let table = self.packed.take().expect("link graph must be initialized");
        PersonNames { table, external: self.into_names() }
    }
}

pub struct PersonNames {
    table: Arc<NameTable>,
    external: Vec<Vec<Arc<str>>>,
}

impl PersonNames {
    pub fn get<'a>(&'a self, id: u32, buffer: &'a mut [u8; 9]) -> &'a str {
        if (id as usize) < self.table.entries.len() {
            let key = self.table.entries[id as usize].key.load(Ordering::Relaxed);
            self.table.spelling(key, buffer)
        } else {
            let external = id as usize - self.table.entries.len();
            &self.external[external % SHARDS][external / SHARDS]
        }
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

// The high 32 bits cannot be all ones for any packed UTF-8 key.
const EXTERNAL_NAME: u64 = 0xffff_ffff_0000_0000;
const EMPTY_KEY: u64 = u64::MAX;
const MAX_PROBES: usize = 16;

struct Entry {
    key: AtomicU64,
    // Starts as the slot itself. Once the graph is flattened, it is the
    // canonical person's slot, also the public person ID.
    parent: AtomicU32,
}

struct NameTable {
    entries: Vec<Entry>,
    external_names: Vec<Vec<Arc<str>>>,
    overflow: AHashMap<u64, u32>,
}

pub struct Aliases {
    table: Arc<NameTable>,
    external: Vec<AHashMap<Arc<str>, u32>>,
}

impl NameTable {
    fn new(edges: usize, external_names: Vec<Vec<Arc<str>>>) -> Self {
        // There are at most two distinct endpoints per edge. Leave a free slot
        // for the terminating search, even when every endpoint is distinct.
        let capacity = (2 * edges + 1).max(1024).next_power_of_two();
        assert!(capacity <= u32::MAX as usize, "too many linked names");
        let entries = (0..capacity).into_par_iter().map(|index| Entry {
            key: AtomicU64::new(EMPTY_KEY),
            parent: AtomicU32::new(index as u32),
        }).collect();
        Self { entries, external_names, overflow: AHashMap::default() }
    }

    fn spelling<'a>(&'a self, key: u64, buffer: &'a mut [u8; 9]) -> &'a str {
        if key >= EXTERNAL_NAME {
            name_for(&self.external_names, key as u32)
        } else {
            unpack_name(key, buffer)
        }
    }

    fn slot_for_link(&self, key: u64, hash: usize) -> u32 {
        let mask = self.entries.len() - 1;
        let mut index = hash & mask;
        loop {
            let slot = &self.entries[index];
            let stored = slot.key.load(Ordering::Acquire);
            if stored == key {
                return index as u32;
            }
            if stored == EMPTY_KEY {
                match slot.key.compare_exchange(EMPTY_KEY, key, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => return index as u32,
                    Err(raced) if raced == key => return index as u32,
                    Err(_) => {}
                }
            }
            index = (index + 1) & mask;
        }
    }

    fn root(&self, mut id: u32) -> u32 {
        loop {
            let entry = &self.entries[id as usize];
            let parent = entry.parent.load(Ordering::Acquire);
            if parent == id {
                return id;
            }
            let grandparent = self.entries[parent as usize].parent.load(Ordering::Acquire);
            if grandparent != parent {
                let _ = entry.parent.compare_exchange(parent, grandparent, Ordering::AcqRel, Ordering::Acquire);
            }
            id = parent;
        }
    }

    fn join(&self, mut left: u32, mut right: u32) {
        loop {
            left = self.root(left);
            right = self.root(right);
            if left == right {
                break;
            }
            let a = self.entries[left as usize].key.load(Ordering::Relaxed);
            let b = self.entries[right as usize].key.load(Ordering::Relaxed);
            let left_first = if a < EXTERNAL_NAME && b < EXTERNAL_NAME {
                a < b
            } else {
                self.spelling(a, &mut [0; 9]) < self.spelling(b, &mut [0; 9])
            };
            let (lower, higher) = if left_first { (left, right) } else { (right, left) };
            // Edges always point toward a smaller spelling. This prevents cycles
            // even when another worker concurrently changes the lower root.
            if self.entries[higher as usize].parent.compare_exchange(
                higher, lower, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                break;
            }
        }
    }

    fn finish(&mut self, hash: &RandomState) {
        let mask = self.entries.len() - 1;
        let beyond: Vec<_> = self.entries.par_iter().enumerate().filter_map(|(index, entry)| {
            let key = entry.key.load(Ordering::Relaxed);
            if key == EMPTY_KEY {
                return None;
            }
            let root = self.root(index as u32);
            entry.parent.store(root, Ordering::Relaxed);
            let distance = index.wrapping_sub(hash.hash_one(key) as usize) & mask;
            (distance >= MAX_PROBES).then_some((key, root))
        }).collect();
        self.overflow = beyond.into_iter().collect();
    }

    fn resolve(&self, key: u64, hash: usize, bytes: &[u8], users: &Names) -> u32 {
        let mask = self.entries.len() - 1;
        for step in 0..MAX_PROBES {
            let slot = &self.entries[hash.wrapping_add(step) & mask];
            let stored = slot.key.load(Ordering::Relaxed);
            if stored == key {
                return slot.parent.load(Ordering::Relaxed);
            }
            if stored == EMPTY_KEY {
                // Keys never move. Every link in overflow already had a full
                // probe range at construction, so this spelling is unlinked.
                match slot.key.compare_exchange(EMPTY_KEY, key, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => return slot.parent.load(Ordering::Relaxed),
                    Err(raced) if raced == key => return slot.parent.load(Ordering::Relaxed),
                    Err(_) => {}
                }
            }
        }
        self.overflow.get(&key).copied().unwrap_or_else(|| users.intern(text(bytes)))
    }
}

// For short u/U-prefixed names, store the suffix in big-endian byte order.
// The high bit distinguishes the prefixes. Padding is unambiguous because
// nonempty suffixes must end in a nonzero byte. Numeric order is byte order.
fn packed_name(name: &[u8]) -> Option<u64> {
    let (&prefix, suffix) = name.split_first()?;
    let prefix_bit = match prefix {
        b'u' => 1 << 63,
        b'U' => 0,
        _ => return None,
    };
    if suffix.len() > 8 || suffix.first().is_some_and(|b| *b >= 128) || suffix.last() == Some(&0) {
        return None;
    }
    let word = if let Ok(full) = <&[u8; 8]>::try_from(suffix) {
        u64::from_be_bytes(*full)
    } else {
        let mut padded = [0u8; 8];
        padded[..suffix.len()].copy_from_slice(suffix);
        u64::from_be_bytes(padded)
    };
    Some(prefix_bit | word)
}

fn unpack_name(key: u64, buffer: &mut [u8; 9]) -> &str {
    buffer[0] = if key >> 63 == 0 { b'U' } else { b'u' };
    buffer[1..].copy_from_slice(&(key & !(1 << 63)).to_be_bytes());
    let length = 1 + buffer.iter().rposition(|&byte| byte != 0).unwrap();
    text(&buffer[..length])
}

pub fn read_aliases(data: &Path, users: &mut Names) -> Result<Aliases> {
    let external = Names { hash: users.hash.clone(), ..Names::default() };
    let key_for = |bytes: &[u8]| {
        packed_name(bytes).unwrap_or_else(|| EXTERNAL_NAME | u64::from(external.intern(text(bytes))))
    };
    let batches = chunks(&data.join("links"))?
        .into_par_iter()
        .map(|chunk| {
            let mut edges = Vec::new();
            chunk.scan(|row| {
                if let Some([left, right]) = fields(row) {
                    if !left.is_empty() && !right.is_empty() && left != right {
                        edges.push((key_for(left), key_for(right)));
                    }
                }
            })?;
            Ok(edges)
        })
        .collect::<Result<Vec<_>>>()?;

    let edge_count = batches.iter().map(Vec::len).sum::<usize>();
    let (mut external, external_names): (Vec<_>, Vec<_>) =
        external.shards.into_iter().map(|shard| {
            let state = shard.into_inner().unwrap();
            (state.ids, state.names)
        }).unzip();
    let mut table = NameTable::new(edge_count, external_names);
    for shard in &mut users.shards {
        shard.get_mut().unwrap().offset = table.entries.len() / SHARDS;
    }
    batches.into_par_iter().for_each(|edges| {
        edges.par_chunks(4096).for_each(|part| {
            for &(left, right) in part {
                let a = table.slot_for_link(left, users.hash.hash_one(left) as usize);
                let b = table.slot_for_link(right, users.hash.hash_one(right) as usize);
                table.join(a, b);
            }
        });
    });
    table.finish(&users.hash);
    external.par_iter_mut().for_each(|ids| {
        for id in ids.values_mut() {
            let key = EXTERNAL_NAME | u64::from(*id);
            let slot = table.slot_for_link(key, users.hash.hash_one(key) as usize);
            *id = table.entries[slot as usize].parent.load(Ordering::Relaxed);
        }
    });
    let table = Arc::new(table);
    users.packed = Some(Arc::clone(&table));
    Ok(Aliases { table, external })
}

#[derive(Clone, Copy, Default)]
struct CachedPerson {
    key: u64,
    person: u32,
    occupied: bool,
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
        let Some(key) = packed_name(bytes) else {
            let shard = users.hash.hash_one(bytes) as usize % SHARDS;
            let name = text(bytes);
            return aliases.external[shard].get(name).copied()
                .unwrap_or_else(|| users.intern_in(shard, name));
        };
        let hash = users.hash.hash_one(key) as usize;
        let slot = hash % self.0.len();
        let entry = &mut self.0[slot];
        if entry.occupied && entry.key == key {
            return entry.person;
        }
        let person = aliases.table.resolve(key, hash, bytes, users);
        *entry = CachedPerson { key, person, occupied: true };
        person
    }
}

#[cfg(test)]
mod person_tests {
    use super::*;

    #[test]
    fn local_cache_keeps_linked_and_unlinked_ids_exact_on_collision() {
        let mut users = Names::default();
        let table = NameTable::new(1, Vec::new());
        for shard in &mut users.shards {
            shard.get_mut().unwrap().offset = table.entries.len() / SHARDS;
        }
        let canonical = users.intern("canonical");
        let key = packed_name(b"ualias").unwrap();
        let hash = users.hash.hash_one(key) as usize;
        let index = table.slot_for_link(key, hash) as usize;
        table.entries[index].parent.store(canonical, Ordering::Relaxed);
        let table = Arc::new(table);
        users.packed = Some(Arc::clone(&table));
        let aliases = Aliases { table, external: (0..SHARDS).map(|_| AHashMap::default()).collect() };
        let mut cache = PersonCache(vec![CachedPerson::default(); 1]);
        let spellings = ["ux", "uy", "ux", "0123456789abcdefx", "0123456789abcdefy"];
        let ids = spellings.map(|name| {
            assert_eq!(cache.resolve(&users, &aliases, b"ualias"), canonical);
            cache.resolve(&users, &aliases, name.as_bytes())
        });
        assert_eq!(ids[0], ids[2]);
        let names = users.into_people();
        for (key, id) in spellings.into_iter().zip(ids) {
            assert_eq!(names.get(id, &mut [0; 9]), key);
        }
    }

    #[test]
    fn packed_spelling_preserves_bytes_and_lexicographic_order() {
        let ordered = ["U", "U0", "Uabc", "u", "u\0x", "u1", "u1\0x", "u2", "uabcdefgh"];
        let keys = ordered.map(|s| packed_name(s.as_bytes()).unwrap());
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        for (name, key) in ordered.into_iter().zip(keys) {
            assert_eq!(unpack_name(key, &mut [0; 9]), name);
        }
        for fallback in ["z", "uuuuuuuuuu", "u\0", "ué"] {
            assert!(packed_name(fallback.as_bytes()).is_none());
        }
    }

    #[test]
    fn mixed_link_keys_choose_the_true_minimum_and_preserve_unlinked_names() -> Result<()> {
        use std::{fs, time::{SystemTime, UNIX_EPOCH}};
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let directory = std::env::temp_dir().join(format!("shopping-names-{}-{nonce}", std::process::id()));
        let links = directory.join("links");
        fs::create_dir_all(&links)?;
        let part = links.join("part.csv");
        fs::write(&part, b"left,right\nu3,ua\nlong-name-at-least-ten,ua\nlong-name-at-least-ten,U7\nu9,u\0\nu\0, a\nu4,u4\n")?;
        let mut users = Names::default();
        let aliases = read_aliases(&directory, &mut users)?;
        let mut cache = PersonCache::default();
        let cases = [("u3", "U7"), ("ua", "U7"), ("long-name-at-least-ten", "U7"),
            ("U7", "U7"), ("u9", " a"), ("u\0", " a"), ("u4", "u4"),
            ("u12345678", "u12345678"), ("unlinked and long", "unlinked and long")];
        let resolved = cases.map(|(key, _)| cache.resolve(&users, &aliases, key.as_bytes()));
        let names = users.into_people();
        for ((_, expected), id) in cases.into_iter().zip(resolved) {
            assert_eq!(names.get(id, &mut [0; 9]), expected);
        }
        fs::remove_file(part)?;
        fs::remove_dir(links)?;
        fs::remove_dir(directory)?;
        Ok(())
    }

    #[test]
    fn parallel_unions_choose_minimum_on_overlapping_chains() {
        let n = 4096;
        let hash = RandomState::new();
        let keys: Vec<_> = (0..n).map(|i| packed_name(format!("u{i:04}").as_bytes()).unwrap()).collect();
        let mut table = NameTable::new(n, Vec::new());
        (1..n).into_par_iter().for_each(|i| {
            for (left, right) in [(i, i - 1), (n - 1 - i, n - i)] {
                let a = table.slot_for_link(keys[left], hash.hash_one(keys[left]) as usize);
                let b = table.slot_for_link(keys[right], hash.hash_one(keys[right]) as usize);
                table.join(a, b);
            }
        });
        table.finish(&hash);
        let first = table.slot_for_link(keys[0], hash.hash_one(keys[0]) as usize);
        for key in keys {
            let slot = table.slot_for_link(key, hash.hash_one(key) as usize) as usize;
            assert_eq!(table.entries[slot].parent.load(Ordering::Relaxed), first);
        }
    }

    #[test]
    fn concurrent_colliding_unlinked_names_keep_distinct_stable_ids() {
        let table = NameTable::new(1, Vec::new());
        let mut users = Names::default();
        for shard in &mut users.shards {
            shard.get_mut().unwrap().offset = table.entries.len() / SHARDS;
        }
        let names: Vec<_> = (0..48).map(|i| format!("u{i:02}")).collect();
        let results = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..8).map(|offset| {
                let (table, users, names) = (&table, &users, &names);
                scope.spawn(move || {
                    let mut ids = vec![0; names.len()];
                    for j in 0..names.len() {
                        let i = (j + offset) % names.len();
                        let bytes = names[i].as_bytes();
                        ids[i] = table.resolve(packed_name(bytes).unwrap(), 0, bytes, users);
                    }
                    ids
                })
            }).collect();
            workers.into_iter().map(|worker| worker.join().unwrap()).collect::<Vec<_>>()
        });
        assert!(results.iter().all(|ids| *ids == results[0]));
        let distinct: std::collections::HashSet<_> = results[0].iter().collect();
        assert_eq!(distinct.len(), names.len());
        users.packed = Some(Arc::new(table));
        let people = users.into_people();
        for (id, expected) in results[0].iter().zip(&names) {
            assert_eq!(people.get(*id, &mut [0; 9]), expected);
        }
    }
}
