use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ahash::{AHashMap, RandomState};
use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use memchr::memchr;
use rayon::prelude::*;
use serde_json::Value;

pub const DAY: i64 = 86_400_000;
pub const LOOKBACK: i64 = 7 * DAY;
pub const SHARDS: usize = 128;
const CHUNK_BYTES: u64 = 4 * 1024 * 1024;

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
            let hours = unsigned(&[*hour_a, *hour_b])?;
            let minutes = unsigned(&[*minute_a, *minute_b])?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            (if *sign == b'+' { 1 } else { -1 }) * (hours * 60 + minutes)
        }
        _ => return unsigned(bytes),
    };
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(
        unsigned(&bytes[..4])? as i32,
        unsigned(&bytes[5..7])? as u32,
        unsigned(&bytes[8..10])? as u32,
    )?;
    let time: NaiveDateTime = date.and_hms_opt(
        unsigned(&bytes[11..13])? as u32,
        unsigned(&bytes[14..16])? as u32,
        unsigned(&bytes[17..19])? as u32,
    )?;
    Some(time.and_utc().timestamp_millis() - offset_minutes * 60_000)
}

pub fn fields<const N: usize>(mut line: &[u8]) -> Option<[&[u8]; N]> {
    let mut fields = [&[][..]; N];
    for (index, field) in fields.iter_mut().enumerate() {
        let value = match memchr(b',', line) {
            Some(at) if index + 1 < N => {
                let (value, rest) = line.split_at(at);
                line = &rest[1..];
                value
            }
            None if index + 1 == N => line,
            _ => return None,
        };
        *field = if value.first() == Some(&b'"') {
            let inner = value.get(1..value.len() - 1)?;
            if value.last() != Some(&b'"') || memchr(b'"', inner).is_some() {
                return None;
            }
            inner
        } else {
            if memchr(b'"', value).is_some() {
                return None;
            }
            value
        };
    }
    Some(fields)
}

pub fn text(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).expect("input fields must be UTF-8")
}

fn parts(path: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "csv") && path.is_file() {
            paths.push(path);
        }
    }
    Ok(paths)
}

pub struct Chunk {
    path: Arc<Path>,
    start: u64,
    end: u64,
}

impl Chunk {
    pub fn scan(&self, mut row: impl FnMut(&[u8])) -> Result<()> {
        let mut reader = BufReader::with_capacity(128 * 1024, File::open(&self.path)?);
        let mut offset = self.start.saturating_sub(1);
        reader.seek(SeekFrom::Start(offset))?;
        let mut line = Vec::with_capacity(256);
        // At offset zero skip the header; otherwise skip the line spanning the boundary.
        offset += reader.read_until(b'\n', &mut line)? as u64;
        while offset < self.end {
            line.clear();
            let len = reader.read_until(b'\n', &mut line)?;
            if len == 0 {
                break;
            }
            offset += len as u64;
            row(line.strip_suffix(b"\n").unwrap_or(&line));
        }
        Ok(())
    }
}

pub fn chunks(directory: &Path) -> Result<Vec<Chunk>> {
    let mut paths = parts(directory)?
        .into_iter()
        .map(|path| {
            let length = path.metadata()?.len();
            Ok((length, Arc::<Path>::from(path)))
        })
        .collect::<Result<Vec<_>>>()?;
    paths.sort_unstable_by_key(|(len, _)| std::cmp::Reverse(*len));
    let jobs = paths.into_iter().flat_map(|(len, path)| {
        (0..len)
            .step_by(CHUNK_BYTES as usize)
            .map(move |start| Chunk {
                path: path.clone(),
                start,
                end: (start + CHUNK_BYTES).min(len),
            })
    });
    Ok(jobs.collect())
}

fn scan_file(path: &Path, row: impl FnMut(&[u8])) -> Result<()> {
    Chunk {
        path: Arc::from(path),
        start: 0,
        end: u64::MAX,
    }
    .scan(row)
    .with_context(|| format!("reading {}", path.display()))
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
        let shard = self.hash.hash_one(name) as usize % SHARDS;
        let mut state = self.shards[shard].lock().unwrap();
        if let Some(&id) = state.ids.get(name) {
            return id;
        }
        let id = id_for(shard, state.names.len());
        let name: Arc<str> = name.into();
        state.ids.insert(name.clone(), id);
        state.names.push(name);
        id
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

pub fn name_for(names: &[Vec<Arc<str>>], id: u32) -> &str {
    let id = id as usize;
    &names[id % SHARDS][id / SHARDS]
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

pub fn read_aliases(data: &Path, users: &Names) -> Result<Vec<u32>> {
    let mut links = Links::default();
    for path in parts(&data.join("links"))? {
        scan_file(&path, |row| {
            if let Some([left, right]) = fields(row) {
                if !left.is_empty() && !right.is_empty() && left != right {
                    links.join(users.intern(text(left)), users.intern(text(right)));
                }
            }
        })?;
    }
    let names: Vec<_> = users
        .shards
        .iter()
        .map(|s| s.lock().unwrap().names.clone())
        .collect();
    let mut minimum = vec![u32::MAX; links.parents.len()];
    for (shard, list) in names.iter().enumerate() {
        for (slot, name) in list.iter().enumerate() {
            let id = id_for(shard, slot);
            let best = &mut minimum[links.root(id) as usize];
            if *best == u32::MAX || name.as_ref() < name_for(&names, *best) {
                *best = id;
            }
        }
    }
    Ok((0..minimum.len() as u32)
        .map(|id| minimum[links.root(id) as usize])
        .collect())
}

pub fn person(users: &Names, aliases: &[u32], bytes: &[u8]) -> u32 {
    let id = users.intern(text(bytes));
    aliases
        .get(id as usize)
        .copied()
        .filter(|&alias| alias != u32::MAX)
        .unwrap_or(id)
}
