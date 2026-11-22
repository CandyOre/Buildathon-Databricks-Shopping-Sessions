//! Candidate B. Winner producers and click readers both address the final
//! representative's partition. Raw-user partitions and P1/P2 are bypassed.
use super::*;
use crate::input::OutputSpellings;

const LINKED: u64 = OTHER | (1 << 61);
const SHORT_LINK: u64 = 1 << 42;
const CLICK_LINK: u64 = 1 << 39;
const CACHE: usize = 2048;

#[derive(Default)]
struct Traffic {
    queries: usize,
    probes: usize,
    event_bytes: usize,
    click_bytes: usize,
    click_count: usize,
    click_wide: usize,
    click_id_wide: usize,
}

struct DirectoryCache {
    short: Vec<(u64, u32)>,
    wide: Vec<([u64; 2], Option<u32>)>,
    traffic: Traffic,
}
impl DirectoryCache {
    fn new() -> Self {
        Self { short: vec![(u64::MAX, INVALID); CACHE],
               wide: vec![([0; 2], None); CACHE / 4],
               traffic: Traffic::default() }
    }

    #[inline]
    fn resolve(&mut self, user: u64, wide: &[[u64; 2]], raw: &Names,
               people: &Names, aliases: &Aliases, spelling: Option<&[u8]>) -> u64 {
        self.traffic.queries += 1;
        let fingerprint = hash(user) as usize;
        let lookup = || {
            if let Some(s) = spelling { return aliases.linked_only(people, s); }
            if user >= OTHER {
                return raw.with_raw_name(user as u32, |s| aliases.linked_only(people, s.as_bytes()));
            }
            let mut buffer = [0; 33];
            aliases.linked_only(people, user_bytes(user, wide, &[], &mut buffer, true))
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
        if id == INVALID { user } else { LINKED | u64::from(id) }
    }
}

#[derive(Default)]
struct EventPacket {
    short: Vec<u64>,
    full: Vec<WinnerRow>,
}
impl EventPacket {
    #[inline]
    fn push(&mut self, row: WinnerRow) {
        let user = if row.user >= LINKED { SHORT_LINK | u64::from(row.user as u32) } else { row.user };
        if (row.user < SHORT_LINK || row.user >= LINKED) && row.stamp < SIDE && row.stamp % 1000 == 0 {
            let seconds = row.stamp / 1000;
            if seconds < 1 << 21 {
                self.short.push(user | (u64::from(seconds) << 43));
                return;
            }
        }
        self.full.push(row);
    }
}

struct EventProducer {
    cache: DirectoryCache,
    buckets: Vec<EventPacket>,
}
impl EventProducer {
    fn new(parts: usize) -> Self {
        Self { cache: DirectoryCache::new(), buckets: (0..parts).map(|_| EventPacket::default()).collect() }
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

fn read_clicks(path: &Path, plan: Plan, window: Window, dimensions: &Dimensions,
               raw: &Names, users: &Names, aliases: &Aliases) -> Result<(Vec<ClickProducer>, Names)> {
    let fallback = Names::default();
    let calendar = Calendar::new(window);
    let chunks = chunks(&path.join("clicks"))?;
    let chunks = crate::csv::work_groups(&chunks, plan.chunks_per_task(chunks.len()),
                                       plan.user_parts * size_of::<ClickPacket>());
    let jobs = chunks.into_iter().enumerate().collect();
    let base = window.start.saturating_sub(input::LOOKBACK);
    let producers = crate::runtime::stage_map("arch_b_decode_final_clicks", jobs, |(i, chunks)| {
        let mut cache = DirectoryCache::new();
        let mut wide = Vec::new();
        let mut ids = ClickKeyChunk::new(i);
        let chunk_id = u32::try_from(i)?;
        let mut buckets: Vec<_> = (0..plan.user_parts).map(|_| ClickPacket::default()).collect();
        for chunk in chunks {
            chunk.scan_clicks(&calendar, window, dimensions, |spelling, user, time, campaign| {
                let handle = encode(user, b'u', &mut wide, raw, true);
                let owner = cache.resolve(handle, &wide, raw, users, aliases, Some(user));
                let out = &mut buckets[hash(owner) as usize & (plan.user_parts - 1)];
                let id = ids.encode(spelling, &fallback);
                let compact_user = if owner >= LINKED { Some(CLICK_LINK | u64::from(owner as u32)) }
                                   else { (owner < CLICK_LINK).then_some(owner) };
                let click = if id >= crate::records::HEX_CLICK {
                    cache.traffic.click_id_wide += 1;
                    Some(packed::ShortClick::WIDE_SLOT | u64::from(id as u32))
                } else {
                    spelling.strip_prefix(b"k").and_then(packed::number)
                                    .filter(|n| *n < packed::ShortClick::WIDE_SLOT)
                };
                if let Some(row) = compact_user.zip(click).and_then(|(owner,id)| packed::ShortClick::pack(owner,id,time,base,campaign)) {
                    out.short.push(row);
                } else {
                    out.full.push(RawClick { user: owner, time, id, campaign, chunk: chunk_id });
                }
            })?;
        }
        let mut traffic = cache.traffic;
        traffic.click_count = traffic.queries;
        traffic.click_wide = wide.len();
        traffic.click_bytes = buckets.iter().map(|p| p.short.len()*16+p.full.len()*size_of::<RawClick>()).sum();
        Ok(ClickProducer { buckets, users: wide, ids, traffic })
    }).into_iter().collect::<Result<Vec<_>>>()?;
    Ok((producers, fallback))
}

struct Partition {
    events: Vec<EventPacket>,
    clicks: Vec<(u32, ClickPacket)>,
}

#[derive(Default)]
struct Groups {
    small: AHashMap<u64,u32>,
    wide: AHashMap<[u64; 2],u32>,
    rows: People,
    names: OutputSpellings,
}

impl Groups {
    #[inline]
    fn group(&mut self, user: u64, wide: &[[u64;2]], other: &[Vec<Arc<str>>], users: &Names) -> u32 {
        let Self { small, wide: map, rows, names } = self;
        let entry = if (WIDE..OTHER).contains(&user) {
            map.entry(wide[user as u32 as usize])
        } else {
            return *small.entry(user).or_insert_with(|| {
                if user >= LINKED { users.with_spelling(user as u32, |s| names.push(s)); }
                else {
                    let mut buffer = [0;33];
                    names.push(input::text(user_bytes(user, wide, other, &mut buffer, true)));
                }
                let local = u32::try_from(names.len()-1).expect("too many people in one partition");
                rows.group(local)
            });
        };
        *entry.or_insert_with(|| {
            let mut buffer = [0;33];
            names.push(input::text(user_bytes(user,wide,other,&mut buffer,true)));
            let local = u32::try_from(names.len()-1).expect("too many people in one partition");
            rows.group(local)
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn consume(p: Partition, users: &Names, other: &[Vec<Arc<str>>], data: &[EventData], clicks: &[Vec<[u64;2]>],
           window: Window, fused: &aggregate::FusedCampaigns, ids: &ClickIds) -> (SessionBatch,OutputSpellings,Duration) {
    let start=Instant::now();
    let mut groups = Groups::default();
    for source in p.events {
        for row in source.short {
            let key = row & ((1<<43)-1);
            let user = if key & SHORT_LINK != 0 { LINKED | u64::from(key as u32) } else {key};
            let local = groups.group(user,&[],other,users);
            groups.rows.event(PersonEvent { person:local, chunk:0, stamp:(row >>43) as u32*1000 },data);
        }
        for row in source.full {
            let local = groups.group(row.user,&data[row.chunk as usize].users,other,users);
            groups.rows.event(PersonEvent { person:local, chunk:row.chunk, stamp:row.stamp },data);
        }
    }
    let base=window.start.saturating_sub(input::LOOKBACK);
    for (chunk,p) in p.clicks {
        let wide=&clicks[chunk as usize];
        let compact=p.short.into_iter().map(|c| {
            let mut row=c.unpack(chunk,base,&[]);
            if row.user & CLICK_LINK != 0 {row.user=LINKED|u64::from(row.user as u32);}
            row
        });
        for row in p.full.into_iter().chain(compact) {
            let local=groups.group(row.user,wide,other,users);
            groups.rows.click(Click { person: local, time:row.time, campaign:row.campaign, id:row.id });
        }
    }
    let Groups {rows,names,..}=groups;
    let batch=person::finish(rows,data,window,None,fused,ids);
    (batch,names,start.elapsed())
}

pub(super) fn run(path: &Path,tasks:Vec<Chunk>,plan:Plan,window:Window,dimensions:&Dimensions,bytes:usize)->Result<Routed>{
    let start=Instant::now();
    let arena=storage::Arena::new(plan.reuse);
    let raw=Names::default();
    // No winner can be lost or sent under a provisional representative:
    // parsing fills the necessary immutable event-ID buckets while all Rayon
    // workers remain available to complete links. Pending winner queue bound = 0.
    let (scan,links)=rayon::join(
        ||scan_events(tasks,plan,window,dimensions,&raw,&arena).map(|s|(s,Instant::now())),
        ||{let mut users=Names::default();input::read_aliases(path,&mut users).map(|a|(users,a,Instant::now()))});
    let (scan,scan_end)=scan?;
    let (users,aliases,link_end)=links?;
    let candidates=scan.work.parsed;
    let selected=dedup_events(scan,plan,window,&arena,||EventProducer::new(plan.user_parts),|out,c,p|{
        let owner=out.cache.resolve(c.row.user,&p[c.chunk as usize].users,&raw,&users,&aliases,None);
        out.buckets[hash(owner) as usize & (plan.user_parts-1)].push(WinnerRow {user:owner,chunk:c.chunk,stamp:c.row.stamp});
    });
    let mut partitions:Vec<Partition>=(0..plan.user_parts).map(|_|Partition{events:Vec::new(),clicks:Vec::new()}).collect();
    let mut traffic=Traffic::default();
    for stream in selected.streams {
        traffic.queries+=stream.cache.traffic.queries;
        traffic.probes+=stream.cache.traffic.probes;
        for (part,p) in partitions.iter_mut().zip(stream.buckets) {
            if !p.short.is_empty() || !p.full.is_empty() {
                traffic.event_bytes+=p.short.len()*8+p.full.len()*size_of::<WinnerRow>();
                part.events.push(p);
            }
        }
    }
    let click_start=Instant::now();
    let (cs,fallback)=read_clicks(path,plan,window,dimensions,&raw,&users,&aliases)?;
    let mut click_users=Vec::with_capacity(cs.len());
    let mut ids=Vec::with_capacity(cs.len());
    for (i,s) in cs.into_iter().enumerate() {
        traffic.queries+=s.traffic.queries;
        traffic.probes+=s.traffic.probes;
        traffic.click_bytes+=s.traffic.click_bytes;
        traffic.click_count+=s.traffic.click_count;
        traffic.click_wide+=s.traffic.click_wide;
        traffic.click_id_wide+=s.traffic.click_id_wide;
        for (part,p) in partitions.iter_mut().zip(s.buckets) {
            if !p.short.is_empty() || !p.full.is_empty() {part.clicks.push((i as u32,p));}
        }
        ids.push(s.ids);click_users.push(s.users);
    }
    drop(aliases);
    let ids=ClickIds::new(fallback,ids);
    let other=raw.into_names();
    let fused=aggregate::FusedCampaigns::new(dimensions.campaigns.len(),window);
    let group_start=Instant::now();
    let outputs=crate::runtime::stage_map("arch_b_final_people",partitions,|p|{
        consume(p,&users,&other,&selected.payloads,&click_users,window,&fused,&ids)
    });
    let mut sessions=Vec::with_capacity(outputs.len());
    let mut names=Vec::with_capacity(outputs.len());
    let mut longest=Duration::ZERO;
    let mut persons=0usize;
    for (mut result,book,elapsed) in outputs {
        let base=u32::try_from(persons)?;
        persons=persons.checked_add(book.len()).ok_or_else(||anyhow::anyhow!("too many people"))?;
        ensure!(persons<=u32::MAX as usize,"too many people for output");
        for id in &mut result.daily.person {*id+=base;}
        sessions.push(result);names.push(book);longest=longest.max(elapsed);
    }
    let output_names=PersonNames::from_partitions(names);
    let campaigns=fused.finish(&sessions);
    if plan.profile {
        let w=&selected.work;
        eprintln!("profile {}",serde_json::json!({
            "stage":"arch_b_direct","input_event_bytes":bytes,"event_candidates":candidates,"valid_winners":selected.count,
            "valid_clicks":traffic.click_count,"final_people":persons,"event_scan_s":w.scan_time.as_secs_f64(),
            "event_exchange_s":w.exchange_time.as_secs_f64(),"event_winner_route_s":w.winner_time.as_secs_f64(),
            "click_scan_route_s":group_start.duration_since(click_start).as_secs_f64(),"final_people_s":group_start.elapsed().as_secs_f64(),
            "longest_person_partition_s":longest.as_secs_f64(),"link_elapsed_s":link_end.duration_since(start).as_secs_f64(),
            "link_barrier_s":link_end.saturating_duration_since(scan_end).as_secs_f64(),"pending_winner_bytes_peak":0,
            "directory_queries":traffic.queries,"directory_probes":traffic.probes,"first_event_exchange_bytes":traffic.event_bytes,
            "first_click_exchange_bytes":traffic.click_bytes,"raw_user_partitions_written":0,"p2_forwarded_bytes":0,
            "candidate_bytes":w.packed_events*16+(candidates-w.packed_events)*size_of::<EventRow>(),
            "wide_event_key_bytes":w.wide_events*16,"wide_user_bytes":(w.wide_users+traffic.click_wide)*16,
            "wide_click_id_bytes":traffic.click_id_wide*16,"event_payload_side_bytes":w.side_rows*size_of::<(i64,Option<Purchase>)>(),
            "ingest_side_bytes":w.ingest_side*8,"event_partitions":plan.event_parts,"person_partitions":plan.user_parts,
            "elapsed_s":start.elapsed().as_secs_f64()
        }));
    }
    Ok((sessions,Campaigns::Ready(campaigns),output_names,selected.count))
}
