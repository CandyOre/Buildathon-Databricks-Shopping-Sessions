//! Candidate B. Winner producers and click readers both address the final
//! representative's partition. Raw-user partitions and P1/P2 are bypassed.
use super::*;
use crate::input::{NumericLinks, OutputSpellings};

const LINKED: u64 = OTHER | (1 << 61);
const SHORT_LINK: u64 = 1 << 31;
const CLICK_LINK: u64 = 1 << 39;
const CACHE: usize = 2048;

#[derive(Default)]
struct Traffic {
    queries: usize,
    probes: usize,
    event_bytes: usize,
    short_events: usize,
    wide_events: usize,
    click_bytes: usize,
    click_count: usize,
    click_wide: usize,
    click_id_wide: usize,
}

/// Prefetch within one producer. Its rows never enter another queue before
/// their final-representative partition is determined.
const PREFETCH_ROWS: usize = 32;

#[derive(Clone, Copy)]
struct LinkIndex<'a> {
    raw: &'a Names,
    people: &'a Names,
    aliases: &'a Aliases,
    numeric: &'a NumericLinks,
}

impl LinkIndex<'_> {
    #[inline]
    fn prefetch(self, owner: u64) {
        if owner < WIDE {
            self.numeric.prefetch(owner);
        }
    }
}

struct DirectoryCache {
    short: Vec<(u64, u32)>,
    wide: Vec<([u64; 2], Option<u32>)>,
    traffic: Traffic,
}
impl DirectoryCache {
    fn new() -> Self {
        Self {
            short: vec![(u64::MAX, INVALID); CACHE],
            wide: vec![([0; 2], None); CACHE / 4],
            traffic: Traffic::default(),
        }
    }

    #[inline]
    fn resolve(&mut self, user: u64, wide: &[[u64; 2]], links: LinkIndex<'_>) -> u64 {
        self.traffic.queries += 1;
        let fingerprint = hash(user) as usize;
        let lookup = || {
            if user < WIDE {
                return links.numeric.person(user);
            }
            if user < OTHER {
                return links
                    .aliases
                    .linked_wide(links.people, wide[user as u32 as usize]);
            }
            links.raw.with_raw_name(user as u32, |s| {
                links.aliases.linked_other(links.people, s.as_bytes())
            })
        };
        let id = if (WIDE..OTHER).contains(&user) {
            let key = wide[user as u32 as usize];
            let entry = &mut self.wide[fingerprint & (CACHE / 4 - 1)];
            if entry.0 != key || entry.1.is_none() {
                self.traffic.probes += 1;
                *entry = (key, Some(lookup().unwrap_or(INVALID)));
            }
            entry.1.unwrap()
        } else {
            let entry = &mut self.short[fingerprint & (CACHE - 1)];
            if entry.0 != user {
                self.traffic.probes += 1;
                *entry = (user, lookup().unwrap_or(INVALID));
            }
            entry.1
        };
        if id == INVALID {
            user
        } else {
            LINKED | u64::from(id)
        }
    }
}

#[derive(Clone, Copy)]
struct ShortWinner(u64);

impl ShortWinner {
    #[inline]
    fn encode(row: WinnerRow) -> Option<Self> {
        if row.stamp >= SIDE {
            return None;
        }
        let compact = if row.user < SHORT_LINK {
            Some(row.user)
        } else if row.user >= LINKED && u64::from(row.user as u32) < SHORT_LINK {
            Some(SHORT_LINK | u64::from(row.user as u32))
        } else {
            None
        };
        if let Some(id) = compact {
            return Some(Self((1 << 63) | (u64::from(row.stamp) << 32) | id));
        }
        let seconds = row.stamp / 1000;
        (row.user < (1 << 42) && row.stamp % 1000 == 0 && seconds < (1 << 21))
            .then(|| Self((u64::from(seconds) << 42) | row.user))
    }

    #[inline]
    fn decode(self) -> WinnerRow {
        let (user, stamp) = if self.0 >> 63 != 0 {
            let id = u64::from(self.0 as u32);
            let owner = if id >= SHORT_LINK {
                LINKED | (id - SHORT_LINK)
            } else {
                id
            };
            (owner, (self.0 >> 32) as u32 & (SIDE - 1))
        } else {
            (self.0 & ((1 << 42) - 1), (self.0 >> 42) as u32 * 1000)
        };
        WinnerRow {
            user,
            stamp,
            chunk: 0,
        }
    }
}

#[derive(Clone, Copy)]
struct WideWinner {
    slot: u32,
    chunk: u32,
    stamp: u32,
}

#[derive(Default)]
struct EventPacket {
    short: Vec<ShortWinner>,
    wide: Vec<WideWinner>,
    full: Vec<WinnerRow>,
}

impl EventPacket {
    #[inline]
    fn push(&mut self, row: WinnerRow) {
        if let Some(short) = ShortWinner::encode(row) {
            self.short.push(short);
        } else if (WIDE..OTHER).contains(&row.user) {
            // This producer already routed using both halves. The receiver
            // needs only this stable source reference for its exact map.
            self.wide.push(WideWinner {
                slot: row.user as u32,
                chunk: row.chunk,
                stamp: row.stamp,
            });
        } else {
            self.full.push(row);
        }
    }
}

struct EventProducer {
    cache: DirectoryCache,
    buckets: Vec<EventPacket>,
    pending: Vec<WinnerRow>,
}

impl EventProducer {
    fn new(parts: usize) -> Self {
        Self {
            cache: DirectoryCache::new(),
            buckets: (0..parts).map(|_| EventPacket::default()).collect(),
            pending: Vec::with_capacity(PREFETCH_ROWS),
        }
    }

    #[inline]
    fn push(&mut self, c: Candidate, data: &[EventData], links: LinkIndex<'_>) {
        links.prefetch(c.row.user);
        self.pending.push(WinnerRow {
            user: c.row.user,
            stamp: c.row.stamp,
            chunk: c.chunk,
        });
        if self.pending.len() == PREFETCH_ROWS {
            self.flush(data, links);
        }
    }

    fn flush(&mut self, data: &[EventData], links: LinkIndex<'_>) {
        let mask = self.buckets.len() - 1;
        for mut row in self.pending.drain(..) {
            row.user = self
                .cache
                .resolve(row.user, &data[row.chunk as usize].users, links);
            self.buckets[hash(row.user) as usize & mask].push(row);
        }
    }
}

#[derive(Default)]
struct ClickPacket {
    short: Vec<packed::ShortClick>,
    full: Vec<RawClick>,
}

struct ClickProducer {
    buckets: Vec<ClickPacket>,
    users: Vec<[u64; 2]>,
    ids: ClickKeyChunk,
    traffic: Traffic,
}

fn read_clicks(
    path: &Path,
    plan: Plan,
    window: Window,
    dimensions: &Dimensions,
    links: LinkIndex<'_>,
) -> Result<(Vec<ClickProducer>, Names)> {
    use crate::records::{HEX_CLICK, NUMERIC_CLICK};
    let fallback = Names::default();
    let calendar = Calendar::new(window);
    let chunks = chunks(&path.join("clicks"))?;
    let chunks = crate::csv::work_groups(
        &chunks,
        plan.chunks_per_task(chunks.len()),
        plan.user_parts * size_of::<ClickPacket>(),
    );
    let jobs = chunks.into_iter().enumerate().collect();
    let base = window.start.saturating_sub(input::LOOKBACK);
    let producers = crate::runtime::stage_map("arch_b_decode_final_clicks", jobs, |(i, chunks)| {
        let mut cache = DirectoryCache::new();
        let mut wide = Vec::new();
        let mut ids = ClickKeyChunk::new(i);
        let chunk_id = u32::try_from(i)?;
        let mut buckets: Vec<_> = (0..plan.user_parts)
            .map(|_| ClickPacket::default())
            .collect();
        let mut pending = Vec::with_capacity(PREFETCH_ROWS);
        let mut flush = |pending: &mut Vec<RawClick>, wide: &[[u64; 2]]| {
            for mut row in pending.drain(..) {
                let owner = cache.resolve(row.user, wide, links);
                let out = &mut buckets[hash(owner) as usize & (plan.user_parts - 1)];
                let compact_user = if owner >= LINKED {
                    Some(CLICK_LINK | u64::from(owner as u32))
                } else {
                    (owner < CLICK_LINK || (WIDE..OTHER).contains(&owner)).then_some(owner)
                };
                let click = if row.id >= HEX_CLICK {
                    cache.traffic.click_id_wide += 1;
                    Some(packed::ShortClick::WIDE_SLOT | u64::from(row.id as u32))
                } else if row.id >= NUMERIC_CLICK {
                    let number = row.id ^ NUMERIC_CLICK;
                    (number < packed::ShortClick::WIDE_SLOT).then_some(number)
                } else {
                    None
                };
                if let Some(compact) = compact_user.zip(click).and_then(|(u, id)| {
                    packed::ShortClick::pack(u, id, row.time, base, row.campaign)
                }) {
                    out.short.push(compact);
                } else {
                    row.user = owner;
                    out.full.push(row);
                }
            }
        };
        for chunk in chunks {
            chunk.scan_clicks(
                &calendar,
                window,
                dimensions,
                |spelling, user, time, campaign| {
                    let handle = encode(user, b'u', &mut wide, links.raw, true);
                    links.prefetch(handle);
                    // The exact bijective rank also serves as the full-row click
                    // ID. Only a lexical tie needs to materialize its characters.
                    let id = spelling
                        .strip_prefix(b"k")
                        .and_then(packed::number)
                        .map_or_else(|| ids.encode(spelling, &fallback), |n| NUMERIC_CLICK | n);
                    pending.push(RawClick {
                        user: handle,
                        time,
                        id,
                        campaign,
                        chunk: chunk_id,
                    });
                    if pending.len() == PREFETCH_ROWS {
                        flush(&mut pending, &wide);
                    }
                },
            )?;
        }
        flush(&mut pending, &wide);
        let mut traffic = cache.traffic;
        traffic.click_count = traffic.queries;
        traffic.click_wide = wide.len();
        traffic.click_bytes = buckets
            .iter()
            .map(|p| p.short.len() * 16 + p.full.len() * size_of::<RawClick>())
            .sum();
        Ok(ClickProducer {
            buckets,
            users: wide,
            ids,
            traffic,
        })
    })
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    Ok((producers, fallback))
}

struct Partition {
    events: Vec<EventPacket>,
    clicks: Vec<(u32, ClickPacket)>,
}

// A person's first event is in the map entry. Further events are appended to
// small contiguous blocks, which reduces random pointer reads on large groups.
// Every link is built during initial grouping; no later scatter or count pass.
const END: u32 = u32::MAX;
const BLOCK: usize = 4;

#[derive(Clone, Copy)]
struct EventValue {
    chunk: u32,
    stamp: u32,
}

struct EventBlock {
    rows: [EventValue; BLOCK],
    next: u32,
}

struct PersonRows {
    first: EventValue,
    more: u32,
    event_head: u32,
    click_head: u32,
}

impl PersonRows {
    fn new(first: EventValue) -> Self {
        Self {
            first,
            more: 0,
            event_head: END,
            click_head: END,
        }
    }

    #[inline]
    fn append_event(&mut self, value: EventValue, blocks: &mut Vec<EventBlock>) {
        let offset = self.more as usize % BLOCK;
        if offset == 0 {
            let head = u32::try_from(blocks.len()).expect("too many event blocks");
            assert_ne!(head, END);
            blocks.push(EventBlock {
                rows: [value; BLOCK],
                next: self.event_head,
            });
            self.event_head = head;
        } else {
            blocks[self.event_head as usize].rows[offset] = value;
        }
        self.more = self
            .more
            .checked_add(1)
            .expect("too many events for one group");
    }

    fn values(self, blocks: &[EventBlock]) -> impl Iterator<Item = EventValue> + '_ {
        let mut head = self.event_head;
        let mut remaining = self.more as usize;
        std::iter::once(self.first).chain(std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            remaining -= 1;
            let block = &blocks[head as usize];
            let offset = remaining % BLOCK;
            if offset == 0 {
                head = block.next;
            }
            Some(block.rows[offset])
        }))
    }
}

#[derive(Default)]
struct Groups {
    small: AHashMap<u64, PersonRows>,
    wide: AHashMap<[u64; 2], PersonRows>,
    events: Vec<EventBlock>,
    clicks: Vec<Click>,
}

impl Groups {
    #[inline]
    fn event(&mut self, row: WinnerRow, wide: &[[u64; 2]]) {
        let value = EventValue {
            chunk: row.chunk,
            stamp: row.stamp,
        };
        let insert = |group: &mut PersonRows| group.append_event(value, &mut self.events);
        // Feed each map only its matching exact key type.
        if (WIDE..OTHER).contains(&row.user) {
            self.wide
                .entry(wide[row.user as u32 as usize])
                .and_modify(insert)
                .or_insert_with(|| PersonRows::new(value));
        } else {
            self.small
                .entry(row.user)
                .and_modify(insert)
                .or_insert_with(|| PersonRows::new(value));
        }
    }

    #[inline]
    fn click(&mut self, row: RawClick, wide: &[[u64; 2]]) {
        let group = if (WIDE..OTHER).contains(&row.user) {
            self.wide.get_mut(&wide[row.user as u32 as usize])
        } else {
            self.small.get_mut(&row.user)
        };
        let Some(group) = group else { return };
        let current = u32::try_from(self.clicks.len()).expect("too many clicks in one partition");
        assert_ne!(current, END);
        self.clicks.push(Click {
            person: group.click_head,
            time: row.time,
            campaign: row.campaign,
            id: row.id,
        });
        group.click_head = current;
    }
}

#[allow(clippy::too_many_arguments)]
fn consume(
    p: Partition,
    linked_count: usize,
    other: &[Vec<Arc<str>>],
    data: &[EventData],
    click_users: &[Vec<[u64; 2]>],
    window: Window,
    fused: &aggregate::FusedCampaigns,
    ids: &ClickIds,
) -> (SessionBatch, OutputSpellings, usize, Duration) {
    let start = Instant::now();
    let mut groups = Groups::default();
    for source in p.events {
        for row in source.short {
            groups.event(row.decode(), &[]);
        }
        for row in source.wide {
            groups.event(
                WinnerRow {
                    user: WIDE | u64::from(row.slot),
                    chunk: row.chunk,
                    stamp: row.stamp,
                },
                &data[row.chunk as usize].users,
            );
        }
        for row in source.full {
            groups.event(row, &[]);
        }
    }
    let base = window.start.saturating_sub(input::LOOKBACK);
    for (chunk, source) in p.clicks {
        let wide = &click_users[chunk as usize];
        for row in source.full {
            groups.click(row, wide);
        }
        for row in source.short {
            let mut row = row.unpack(chunk, base, wide);
            if row.user < WIDE && row.user & CLICK_LINK != 0 {
                row.user = LINKED | u64::from(row.user as u32);
            }
            groups.click(row, wide);
        }
    }

    let Groups {
        small,
        wide,
        events,
        clicks,
    } = groups;
    let person_count = small.len() + wide.len();
    let mut names = OutputSpellings::default();
    let batch = fused.with_people(None, ids, |kernel| {
        let mut compute = |local: u32, rows: PersonRows| {
            let mut next = rows.click_head;
            let click_rows = std::iter::from_fn(|| {
                if next == END {
                    return None;
                }
                let mut c = clicks[next as usize];
                next = c.person;
                c.person = local;
                Some(c)
            });
            let event_rows = rows.values(&events).map(|e| {
                if e.stamp < SIDE {
                    (window.start + i64::from(e.stamp), None)
                } else {
                    data[e.chunk as usize].decode(window.start, e.stamp)
                }
            });
            kernel.compute(local, event_rows, click_rows);
        };
        let mut buffer = [0; 33];
        for (owner, rows) in small {
            if owner >= LINKED {
                // The immutable graph already owns this exact UTF-8 spelling.
                compute(owner as u32, rows);
                continue;
            }
            let local = u32::try_from(linked_count + names.len()).expect("too many output people");
            if owner >= OTHER {
                names.push_shared(input::shared_name_for(other, owner as u32));
            } else {
                names.push(input::text(user_bytes(
                    owner,
                    &[],
                    other,
                    &mut buffer,
                    true,
                )));
            }
            compute(local, rows);
        }
        for (key, rows) in wide {
            let local = u32::try_from(linked_count + names.len()).expect("too many output people");
            names.push(input::text(user_bytes(
                WIDE,
                std::slice::from_ref(&key),
                other,
                &mut buffer,
                true,
            )));
            compute(local, rows);
        }
    });
    (batch, names, person_count, start.elapsed())
}

pub(super) fn run(
    path: &Path,
    tasks: Vec<Chunk>,
    plan: Plan,
    window: Window,
    dimensions: &Dimensions,
    bytes: usize,
) -> Result<Routed> {
    let start = Instant::now();
    let arena = storage::Arena::new(plan.reuse);
    let raw = Names::default();
    // No winner can be lost or sent under a provisional representative:
    // parsing fills the necessary immutable event-ID buckets while all Rayon
    // workers remain available to complete links. Pending winner queue bound = 0.
    let (scan, links) = rayon::join(
        || scan_events(tasks, plan, window, dimensions, &raw, &arena).map(|s| (s, Instant::now())),
        || {
            let link_start = Instant::now();
            let mut users = Names::default();
            input::read_numeric_aliases(path, &mut users, packed::number)
                .map(|(a, i)| (users, a, i, link_start, Instant::now()))
        },
    );
    let (scan, scan_end) = scan?;
    let (users, aliases, numeric, link_start, link_end) = links?;
    let candidates = scan.work.parsed;
    let index = LinkIndex {
        raw: &raw,
        people: &users,
        aliases: &aliases,
        numeric: &numeric,
    };
    let selected = dedup_events(
        scan,
        plan,
        window,
        &arena,
        || EventProducer::new(plan.user_parts),
        |out, c, p| out.push(c, p, index),
        |mut out, p| {
            out.flush(p, index);
            (out.buckets, out.cache.traffic)
        },
    );
    let mut partitions: Vec<Partition> = (0..plan.user_parts)
        .map(|_| Partition {
            events: Vec::new(),
            clicks: Vec::new(),
        })
        .collect();
    let mut traffic = Traffic::default();
    for (buckets, counters) in selected.streams {
        traffic.queries += counters.queries;
        traffic.probes += counters.probes;
        for (part, p) in partitions.iter_mut().zip(buckets) {
            if !p.short.is_empty() || !p.wide.is_empty() || !p.full.is_empty() {
                traffic.short_events += p.short.len();
                traffic.wide_events += p.wide.len();
                traffic.event_bytes += p.short.len() * 8
                    + p.wide.len() * size_of::<WideWinner>()
                    + p.full.len() * size_of::<WinnerRow>();
                part.events.push(p);
            }
        }
    }
    let click_start = Instant::now();
    let (cs, fallback) = read_clicks(path, plan, window, dimensions, index)?;
    let mut click_users = Vec::with_capacity(cs.len());
    let mut ids = Vec::with_capacity(cs.len());
    for (i, s) in cs.into_iter().enumerate() {
        traffic.queries += s.traffic.queries;
        traffic.probes += s.traffic.probes;
        traffic.click_bytes += s.traffic.click_bytes;
        traffic.click_count += s.traffic.click_count;
        traffic.click_wide += s.traffic.click_wide;
        traffic.click_id_wide += s.traffic.click_id_wide;
        for (part, p) in partitions.iter_mut().zip(s.buckets) {
            if !p.short.is_empty() || !p.full.is_empty() {
                part.clicks.push((i as u32, p));
            }
        }
        ids.push(s.ids);
        click_users.push(s.users);
    }
    let linked_count = aliases.linked_person_count();
    drop(numeric);
    drop(aliases);
    let ids = ClickIds::new(fallback, ids);
    let other = raw.into_names();
    let fused = aggregate::FusedCampaigns::new(dimensions.campaigns.len(), window);
    let group_start = Instant::now();
    let max_partition_rows = if plan.profile {
        partitions
            .iter()
            .map(|p| {
                p.events
                    .iter()
                    .map(|b| b.short.len() + b.wide.len() + b.full.len())
                    .sum::<usize>()
                    + p.clicks
                        .iter()
                        .map(|(_, b)| b.short.len() + b.full.len())
                        .sum::<usize>()
            })
            .max()
            .unwrap_or(0)
    } else {
        0
    };
    let outputs = crate::runtime::stage_map("arch_b_final_people", partitions, |p| {
        consume(
            p,
            linked_count,
            &other,
            &selected.payloads,
            &click_users,
            window,
            &fused,
            &ids,
        )
    });
    let mut sessions = Vec::with_capacity(outputs.len());
    let mut names = Vec::with_capacity(outputs.len());
    let mut longest = Duration::ZERO;
    let mut persons = 0usize;
    let mut extra = 0usize;
    for (mut result, book, active, elapsed) in outputs {
        let base = u32::try_from(extra)?;
        extra = extra
            .checked_add(book.len())
            .ok_or_else(|| anyhow::anyhow!("too many people"))?;
        ensure!(
            linked_count + extra <= u32::MAX as usize,
            "too many people for output"
        );
        if base != 0 {
            for id in &mut result.daily.person {
                if *id as usize >= linked_count {
                    *id += base;
                }
            }
        }
        persons += active;
        sessions.push(result);
        names.push(book);
        longest = longest.max(elapsed);
    }
    let mut output_names = users.into_people();
    output_names.append_partitions(names);
    let campaigns = fused.finish(&sessions);
    if plan.profile {
        let w = &selected.work;
        eprintln!(
            "profile {}",
            serde_json::json!({
                "stage":"arch_b_direct","input_event_bytes":bytes,"event_candidates":candidates,"valid_winners":selected.count,
                "compact_winners":traffic.short_events,"wide_unlinked_winners":traffic.wide_events,
                "source_bucket_bytes_released":w.returned_bucket_page_bytes+w.returned_bucket_vec_bytes,
                "valid_clicks":traffic.click_count,"final_people":persons,"event_scan_s":w.scan_time.as_secs_f64(),
                "event_exchange_s":w.exchange_time.as_secs_f64(),"event_winner_route_s":w.winner_time.as_secs_f64(),
                "click_scan_route_s":group_start.duration_since(click_start).as_secs_f64(),"final_people_s":group_start.elapsed().as_secs_f64(),
                "longest_person_partition_s":longest.as_secs_f64(),"link_elapsed_s":link_end.duration_since(link_start).as_secs_f64(),
                "link_published_s":link_end.duration_since(start).as_secs_f64(),"max_final_partition_rows":max_partition_rows,
                "link_barrier_s":link_end.saturating_duration_since(scan_end).as_secs_f64(),"pending_winner_bytes_peak":0,
                "post_publication_prefetch_bytes_per_producer":PREFETCH_ROWS*size_of::<WinnerRow>(),
                "directory_layout":"reversible_numeric_and_full_width","directory_queries":traffic.queries,"directory_probes":traffic.probes,"first_event_exchange_bytes":traffic.event_bytes,
                "first_click_exchange_bytes":traffic.click_bytes,"raw_user_partitions_written":0,"p2_forwarded_bytes":0,
                "candidate_bytes":w.packed_events*16+(candidates-w.packed_events)*size_of::<EventRow>(),
                "wide_event_key_bytes":w.wide_events*16,"wide_user_bytes":(w.wide_users+traffic.click_wide)*16,
                "wide_click_id_bytes":traffic.click_id_wide*16,"event_payload_side_bytes":w.side_rows*size_of::<(i64,Option<Purchase>)>(),
                "ingest_side_bytes":w.ingest_side*8,"event_partitions":plan.event_parts,"person_partitions":plan.user_parts,
                "elapsed_s":start.elapsed().as_secs_f64()
            })
        );
    }
    Ok((
        sessions,
        Campaigns::Ready(campaigns),
        output_names,
        selected.count,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fractional_winners_and_block_boundaries_preserve_every_event() {
        for (user, stamp, packed) in [
            (SHORT_LINK - 1, SIDE - 1, true),
            (LINKED | (SHORT_LINK - 1), SIDE - 1, true),
            (SHORT_LINK, 123, false),
            (SHORT_LINK, 123_000, true),
            (LINKED | SHORT_LINK, 123, false),
            (WIDE, 8_000, false),
            (OTHER, 0, false),
            (1, SIDE, false),
        ] {
            let encoded = ShortWinner::encode(WinnerRow {
                user,
                chunk: 7,
                stamp,
            });
            assert_eq!(encoded.is_some(), packed);
            if let Some(encoded) = encoded {
                let row = encoded.decode();
                assert_eq!((row.user, row.stamp), (user, stamp));
            }
        }
        for count in 1..=18 {
            let mut blocks = Vec::new();
            let mut rows = PersonRows::new(EventValue { chunk: 7, stamp: 0 });
            for n in 1..count {
                rows.append_event(
                    EventValue {
                        chunk: n + 7,
                        stamp: n,
                    },
                    &mut blocks,
                );
            }
            let mut values: Vec<_> = rows.values(&blocks).map(|e| (e.stamp, e.chunk)).collect();
            values.sort_unstable();
            assert_eq!(values, (0..count).map(|n| (n, n + 7)).collect::<Vec<_>>());
        }
    }

    #[test]
    fn colliding_wide_users_keep_exact_identity_across_chunks() -> Result<()> {
        use std::{
            fs,
            time::{SystemTime, UNIX_EPOCH},
        };
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path =
            std::env::temp_dir().join(format!("direct-route-{}-{nonce}", std::process::id()));
        fs::create_dir_all(path.join("links"))?;
        let linked = b"u00000000000000000000000000000000";
        let different = b"u00000000008000000000000000000001";
        let edges = [
            "left,right",
            "u00000000000000000000000000000000,u0",
            "u0,U0",
            "u00,uUPPER",
        ]
        .join("\n");
        fs::write(path.join("links/part.csv"), edges)?;
        let mut users = Names::default();
        let (aliases, numeric) = input::read_numeric_aliases(&path, &mut users, packed::number)?;
        fs::remove_file(path.join("links/part.csv"))?;
        fs::remove_dir(path.join("links"))?;
        fs::remove_dir(path)?;

        let raw = Names::default();
        let mut cache = DirectoryCache::new();
        let mut first = Vec::new();
        let mut second = Vec::new();
        let mut encode_resolve = |s: &[u8], chunk: &mut Vec<[u64; 2]>| {
            let handle = encode(s, b'u', chunk, &raw, true);
            let owner = cache.resolve(
                handle,
                chunk,
                LinkIndex {
                    raw: &raw,
                    people: &users,
                    aliases: &aliases,
                    numeric: &numeric,
                },
            );
            (handle, owner)
        };
        let (x, linked_owner) = encode_resolve(linked, &mut first);
        let (y, unlinked) = encode_resolve(different, &mut second);
        assert_eq!(x, y); // Equal fingerprint and identical local slot, different complete keys.
        assert!(linked_owner >= LINKED);
        assert_eq!(unlinked, y);
        let (_, again) = encode_resolve(linked, &mut second);
        assert_eq!(again, linked_owner);
        let (_, later) = encode_resolve(different, &mut first);
        assert_ne!(later, unlinked);
        assert_eq!(hash(later), hash(unlinked));
        let (_, zero) = encode_resolve(b"u0", &mut first);
        let (_, zeros) = encode_resolve(b"u00", &mut first);
        let (_, uppercase) = encode_resolve(b"uUPPER", &mut first);
        assert_eq!(zero, linked_owner);
        assert_ne!(zeros, zero);
        assert_eq!(zeros, uppercase);

        let mut groups = Groups::default();
        let event = |user| WinnerRow {
            user,
            chunk: 0,
            stamp: 100,
        };
        groups.event(event(unlinked), &second);
        groups.event(event(later), &first);
        groups.event(event(linked_owner), &first);
        assert_eq!(groups.wide.len(), 1);
        assert_eq!(groups.small.len(), 1);
        assert_eq!(groups.wide.values().next().unwrap().more, 1);
        let mut first_part = OutputSpellings::default();
        first_part.push(input::text(different));
        users.with_spelling(linked_owner as u32, |name| first_part.push(name));
        let mut other_part = OutputSpellings::default();
        other_part.push("u0\0");
        other_part.push("U0");
        let book = PersonNames::from_partitions(vec![first_part, other_part]);
        for (id, expected) in [input::text(different), "U0", "u0\0", "U0"]
            .into_iter()
            .enumerate()
        {
            assert_eq!(book.get(id as u32, &mut [0; 9]), expected);
        }
        Ok(())
    }
}
