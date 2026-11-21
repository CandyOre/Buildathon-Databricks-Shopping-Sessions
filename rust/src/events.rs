use std::path::Path;
use std::sync::Mutex;

use ahash::RandomState;
use anyhow::Result;
use rayon::prelude::*;
use serde_json::json;

use crate::csv::{chunks, fields};
use crate::input::{
    Aliases, DAY, Dimensions, Names, PersonCache, SHARDS, Window, text, timestamp, unsigned,
};
use crate::profile::Profiler;
use crate::records::{Batch, Event, Purchase, append, id_for, shards};

const CHUNKS_PER_TASK: usize = 8;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Position {
    block: u32,
    row: u32,
}

impl Position {
    const INVALID: Self = Self {
        block: u32::MAX,
        row: 0,
    };

    fn new(task: usize, person_shard: usize, row: usize) -> Self {
        let block = u32::try_from(task * SHARDS + person_shard).expect("too many event blocks");
        assert_ne!(block, Self::INVALID.block, "event block ID is reserved");
        Self {
            block,
            row: u32::try_from(row).expect("event block is too large"),
        }
    }
}

#[derive(Clone, Copy)]
struct IndexEntry {
    id: u64,
    ingested: i64,
    position: Position,
}

pub struct ParsedEvents {
    blocks: Vec<Vec<Batch<Event>>>,
    indexes: Vec<Vec<IndexEntry>>,
}

pub fn read(
    data: &Path,
    window: Window,
    dimensions: &Dimensions,
    users: &Names,
    aliases: &Aliases,
) -> Result<ParsedEvents> {
    let ids = Names::default();
    let index_shards = shards::<Mutex<Vec<IndexEntry>>>();
    let blocks = chunks(&data.join("events"))?
        .par_chunks(CHUNKS_PER_TASK)
        .enumerate()
        .map_init(PersonCache::default, |people, (task, chunks)| {
            let mut person_blocks = shards::<Batch<Event>>();
            let mut indexes = shards::<Vec<IndexEntry>>();
            for chunk in chunks {
                chunk.scan(|line| {
                    let Some([id, user, ts, ingested, kind, product, price]) = fields(line) else {
                        return;
                    };
                    if id.is_empty() {
                        return;
                    }
                    let ingested = timestamp(ingested).expect("ingest_ts is valid F1");
                    let id = id_for(id, &ids);
                    let id_shard =
                        ((id ^ (id >> 32)).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 57) as usize;
                    let validate = || {
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
                        Some((time, purchase))
                    };
                    let position = validate().map_or(Position::INVALID, |(time, purchase)| {
                        let person = people.resolve(users, aliases, user);
                        let person_shard = person as usize % SHARDS;
                        let block = &mut person_blocks[person_shard];
                        let position = Position::new(task, person_shard, block.rows.len());
                        let purchase = block.add_purchase(purchase);
                        block.rows.push(Event {
                            time,
                            person,
                            purchase,
                        });
                        position
                    });
                    // Invalid rows with a well-formed ID still compete on ingestion time.
                    indexes[id_shard].push(IndexEntry {
                        id,
                        ingested,
                        position,
                    });
                })?;
            }
            for (target, source) in index_shards.iter().zip(&mut indexes) {
                if !source.is_empty() {
                    append(&mut target.lock().unwrap(), source);
                }
            }
            Ok::<_, anyhow::Error>(person_blocks)
        })
        .collect::<Result<Vec<_>>>()?;
    let indexes = index_shards
        .into_iter()
        .map(|shard| shard.into_inner().unwrap())
        .collect();
    Ok(ParsedEvents { blocks, indexes })
}

fn filter_possible_duplicates(
    entries: &mut Vec<IndexEntry>,
    counts: &mut Vec<u8>,
    capacity: usize,
    hash: &RandomState,
) {
    assert!(capacity.is_power_of_two());
    counts.clear();
    counts.resize(capacity, 0);
    let slot = |id| hash.hash_one(id) as usize & (capacity - 1);
    for entry in entries.iter() {
        let count = &mut counts[slot(entry.id)];
        *count = (*count + 1).min(2);
    }
    // Compaction is in place even when every entry collides. Full IDs are kept.
    entries.retain(|entry| counts[slot(entry.id)] == 2);
}

fn exact_losers(mut entries: Vec<IndexEntry>) -> Vec<Position> {
    entries.par_sort_unstable_by_key(|entry| (entry.id, entry.ingested));
    entries
        .chunk_by(|a, b| a.id == b.id)
        .flat_map(|group| {
            group[1..]
                .iter()
                .map(|entry| entry.position)
                .filter(|&position| position != Position::INVALID)
        })
        .collect()
}

pub fn deduplicate(parsed: ParsedEvents, profile: &mut Profiler) -> (Vec<Batch<Event>>, usize) {
    let ParsedEvents {
        mut blocks,
        mut indexes,
    } = parsed;
    let profile_enabled = std::env::var_os("PROFILE").is_some_and(|p| p == "1");
    let full_sort = std::env::var_os("DEDUP_FULL_SORT").is_some_and(|p| p == "1");
    let candidates: usize = indexes.iter().map(Vec::len).sum();
    let largest_shard = indexes.iter().map(Vec::len).max().unwrap_or_default();
    if !full_sort {
        let hash = RandomState::with_seeds(19, 41, 53, 67);
        indexes
            .par_iter_mut()
            .for_each_init(Vec::new, |counts, entries| {
                let capacity = (entries.len() * 8).next_power_of_two();
                filter_possible_duplicates(entries, counts, capacity, &hash);
            });
    }
    let exact_candidates: usize = indexes.iter().map(Vec::len).sum();
    profile.stage("dedup_filter");
    let losers = indexes
        .into_par_iter()
        .map(exact_losers)
        .collect::<Vec<_>>();
    let eliminated = losers.iter().map(Vec::len).sum::<usize>();
    profile.stage("dedup_exact");
    let mut by_block: Vec<Vec<u32>> = (0..blocks.len() * SHARDS).map(|_| Vec::new()).collect();
    for position in losers.into_iter().flatten() {
        by_block[position.block as usize].push(position.row);
    }
    blocks
        .par_iter_mut()
        .zip(by_block.par_chunks_mut(SHARDS))
        .for_each(|(task, losers)| {
            for (block, rows) in task.iter_mut().zip(losers) {
                if rows.is_empty() {
                    continue;
                }
                rows.sort_unstable();
                let mut losers = rows.iter().copied().peekable();
                let mut row = 0;
                block.rows.retain(|_| {
                    let keep = losers.next_if_eq(&row).is_none();
                    row += 1;
                    keep
                });
            }
        });
    drop(by_block);
    profile.stage("dedup_apply");
    let mut by_person = shards::<Vec<Batch<Event>>>();
    for task in blocks {
        for (person, block) in by_person.iter_mut().zip(task) {
            if !block.rows.is_empty() {
                person.push(block);
            }
        }
    }
    let events: Vec<_> = by_person.into_par_iter().map(merge_person_blocks).collect();
    let event_count = events.iter().map(|block| block.rows.len()).sum();
    profile.stage("dedup_merge");
    if profile_enabled {
        eprintln!(
            "dedup {}",
            json!({"candidates": candidates, "exact_candidates": exact_candidates,
            "largest_shard": largest_shard, "eliminated": eliminated, "merged_rows": event_count,
            "purchases": events.iter().map(|block| block.purchases.len()).sum::<usize>()})
        );
    }
    (events, event_count)
}

fn merge_person_blocks(blocks: Vec<Batch<Event>>) -> Batch<Event> {
    let mut merged = Batch {
        rows: Vec::with_capacity(blocks.iter().map(|block| block.rows.len()).sum()),
        purchases: Vec::with_capacity(blocks.iter().map(|block| block.purchases.len()).sum()),
    };
    for block in blocks {
        for mut event in block.rows {
            if event.purchase != 0 {
                event.purchase =
                    merged.add_purchase(Some(block.purchases[event.purchase as usize - 1]));
            }
            merged.rows.push(event);
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_copy_does_not_claim_id_but_field_invalid_copy_does() -> Result<()> {
        let root = std::env::temp_dir().join(format!("shopping-routing-{}", std::process::id()));
        let events = root.join("events");
        std::fs::create_dir_all(&events)?;
        std::fs::write(root.join("products.csv"), "product_id,category\np1,books\n")?;
        std::fs::write(root.join("campaigns.csv"), "campaign_id,channel\n")?;
        let header = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n";
        std::fs::write(
            events.join("a.csv"),
            format!(
                "{header}\
bad-format,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1\n\
bad-field,u1,invalid,2026-10-03T10:01:00Z,purchase,p1,200\n"
            ),
        )?;
        std::fs::write(
            events.join("b.csv"),
            format!(
                "{header}\
bad-format,u2,2026-10-03T10:00:00Z,2026-10-03T10:02:00Z,purchase,p1,100\n\
bad-field,u2,2026-10-03T10:00:00Z,2026-10-03T10:02:00Z,purchase,p1,200\n"
            ),
        )?;
        let dimensions = Dimensions::read(&root)?;
        let window = Window {
            start: timestamp(b"2026-10-03T00:00:00Z").unwrap(),
            end: timestamp(b"2026-10-04T00:00:00Z").unwrap(),
        };
        for threads in [1, 2, 8] {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()?
                .install(|| {
                    let mut users = Names::default();
                    let aliases = crate::input::read_aliases(&root, &mut users)?;
                    let parsed = read(&root, window, &dimensions, &users, &aliases)?;
                    let (batches, count) = deduplicate(parsed, &mut Profiler::new(false));
                    assert_eq!(count, 1);
                    let batch = batches.iter().find(|batch| !batch.rows.is_empty()).unwrap();
                    assert_eq!(batch.rows[0].person, users.intern("u2"));
                    assert_eq!(batch.purchases[0].cents, 100);
                    Ok::<_, anyhow::Error>(())
                })?;
        }
        for path in [
            events.join("a.csv"),
            events.join("b.csv"),
            root.join("products.csv"),
            root.join("campaigns.csv"),
        ] {
            std::fs::remove_file(path)?;
        }
        std::fs::remove_dir(events)?;
        std::fs::remove_dir(root)?;
        Ok(())
    }

    #[test]
    fn saturated_collisions_keep_distinct_full_keys_and_discard_valid_loser_of_invalid_row() {
        let names = Names::default();
        let repeated = id_for(b"this-is-a-long-event-id", &names);
        let unrelated = id_for(b"this-is-another-long-event-id", &names);
        let other = id_for(b"another", &names);
        let mut entries = vec![
            IndexEntry {
                id: repeated,
                ingested: 0,
                position: Position::INVALID,
            },
            IndexEntry {
                id: unrelated,
                ingested: 5,
                position: Position::new(0, 0, 301),
            },
            IndexEntry {
                id: other,
                ingested: 3,
                position: Position::new(0, 0, 302),
            },
        ];
        let expected: Vec<_> = (0..300).map(|n| Position::new(n % 2, n % 3, n)).collect();
        entries.extend(expected.iter().map(|&position| IndexEntry {
            id: repeated,
            ingested: 1,
            position,
        }));
        let mut counts = vec![2; 8];
        let hash = RandomState::with_seeds(19, 41, 53, 67);
        filter_possible_duplicates(&mut entries, &mut counts, 1, &hash);
        assert_eq!(counts, [2]);
        assert_eq!(entries.len(), 303);
        let mut losers = exact_losers(entries);
        losers.sort_unstable();
        let mut expected = expected;
        expected.sort_unstable();
        assert_eq!(losers, expected);
        let mut singleton = vec![IndexEntry {
            id: other,
            ingested: 0,
            position: Position::INVALID,
        }];
        filter_possible_duplicates(&mut singleton, &mut counts, 1, &hash);
        assert!(singleton.is_empty());
    }

    #[test]
    fn independent_id_shards_remove_adjacent_rows_and_remap_surviving_purchases() {
        for threads in [1, 2, 8] {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap()
                .install(|| {
                    let mut blocks = vec![shards::<Batch<Event>>(), shards::<Batch<Event>>()];
                    for (task, shard, cents) in [
                        (0, 0, 10),
                        (0, 0, 20),
                        (0, 0, 30),
                        (1, 0, 40),
                        (1, 0, 40),
                        (1, 1, 50),
                    ] {
                        let block = &mut blocks[task][shard];
                        let purchase = block.add_purchase(Some(Purchase { category: 0, cents }));
                        block.rows.push(Event {
                            person: shard as u32,
                            time: 0,
                            purchase,
                        });
                    }
                    let entry = |id, ingested, position| IndexEntry {
                        id,
                        ingested,
                        position,
                    };
                    let indexes = vec![
                        vec![
                            entry(8, 0, Position::new(1, 1, 0)),
                            entry(8, 1, Position::new(0, 0, 0)),
                        ],
                        vec![
                            entry(9, 0, Position::INVALID),
                            entry(9, 0, Position::INVALID),
                            entry(9, 1, Position::new(0, 0, 1)),
                        ],
                        vec![
                            entry(10, 0, Position::new(0, 0, 2)),
                            entry(11, 0, Position::new(1, 0, 0)),
                            entry(11, 0, Position::new(1, 0, 1)),
                        ],
                    ];
                    let (result, count) =
                        deduplicate(ParsedEvents { blocks, indexes }, &mut Profiler::new(false));
                    assert_eq!(count, 3);
                    assert_eq!(
                        result[0]
                            .rows
                            .iter()
                            .map(|row| row.purchase)
                            .collect::<Vec<_>>(),
                        [1, 2]
                    );
                    assert_eq!(
                        result[0]
                            .purchases
                            .iter()
                            .map(|purchase| purchase.cents)
                            .collect::<Vec<_>>(),
                        [30, 40]
                    );
                    assert_eq!(result[1].purchases[0].cents, 50);
                });
        }
    }
}
