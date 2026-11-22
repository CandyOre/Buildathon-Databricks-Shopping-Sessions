//! Group a P1 worker's outbound rows by their resolved person. Each person
//! appears once per destination block, irrespective of its raw spellings.
use super::{Counts, Destinations, EventData, Forward, People, PersonEvent, Plan};
use crate::aggregate::SessionBatch;
use crate::input::{Names, Window};
use crate::records::Click;
use ahash::AHashMap;
use std::mem::size_of;
use std::sync::Arc;

pub(super) enum Packet {
    Rows(Forward),
    Groups(Block),
}

#[derive(Default)]
pub(super) struct Work {
    pub events: usize,
    pub clicks: usize,
    pub groups: usize,
    pub descriptor_bytes: usize,
    pub event_bytes: usize,
    pub click_bytes: usize,
    pub producer_payload_buffers: usize,
    pub producer_payload_capacity: usize,
    pub block_view_bytes: usize,
}

impl Packet {
    pub(super) fn row_count(&self) -> usize {
        match self {
            Self::Rows(rows) => rows.events.len() + rows.clicks.len(),
            Self::Groups(block) => {
                block.end.events - block.start.events + block.end.clicks - block.start.clicks
            }
        }
    }

    pub fn account(&self, work: &mut Work) {
        match self {
            Self::Rows(rows) => {
                work.events += rows.events.len();
                work.clicks += rows.clicks.len();
                work.event_bytes += rows.events.len() * size_of::<PersonEvent>();
                work.click_bytes += rows.clicks.len() * size_of::<Click>();
            }
            Self::Groups(block) => {
                let sizes = block.end.difference(block.start);
                work.events += sizes.events;
                work.clicks += sizes.clicks;
                work.groups += sizes.groups;
                work.descriptor_bytes += sizes.groups * size_of::<Descriptor>();
                work.event_bytes +=
                    sizes.events * size_of::<u32>() + sizes.sources * size_of::<[u32; 2]>();
                work.click_bytes += sizes.clicks * (size_of::<(i64, u64)>() + size_of::<u32>());
                work.block_view_bytes += size_of::<Block>();
                // The first nonempty destination always starts at group zero.
                // Count this producer's allocations once across all its views.
                if block.start.groups == 0 {
                    let (buffers, bytes) = block.payload.capacity();
                    work.producer_payload_buffers += buffers;
                    work.producer_payload_capacity += bytes;
                }
            }
        }
    }
}

#[derive(Default, Clone, Copy)]
struct Descriptor {
    person: u32,
    event_end: u32,
    click_end: u32,
    sales: u32,
}

#[derive(Default, Clone, Copy)]
struct Offsets {
    groups: usize,
    events: usize,
    sources: usize,
    clicks: usize,
}

impl Offsets {
    fn advance(&mut self, by: Self) {
        self.groups += by.groups;
        self.events += by.events;
        self.sources += by.sources;
        self.clicks += by.clicks;
    }

    fn difference(self, start: Self) -> Self {
        Self {
            groups: self.groups - start.groups,
            events: self.events - start.events,
            sources: self.sources - start.sources,
            clicks: self.clicks - start.clicks,
        }
    }
}

struct Payload {
    groups: Vec<Descriptor>,
    events: Vec<u32>,
    sources: Vec<[u32; 2]>,
    clicks: Vec<(i64, u64)>,
    campaigns: Vec<u32>,
}

impl Payload {
    fn new(size: Offsets) -> Self {
        Self {
            groups: vec![Descriptor::default(); size.groups],
            events: vec![0; size.events],
            sources: vec![[0; 2]; size.sources],
            clicks: vec![(0, 0); size.clicks],
            campaigns: vec![0; size.clicks],
        }
    }

    fn capacity(&self) -> (usize, usize) {
        let capacity = [
            self.groups.capacity() * size_of::<Descriptor>(),
            self.events.capacity() * size_of::<u32>(),
            self.sources.capacity() * size_of::<[u32; 2]>(),
            self.clicks.capacity() * size_of::<(i64, u64)>(),
            self.campaigns.capacity() * size_of::<u32>(),
        ];
        (
            capacity.iter().filter(|&&n| n != 0).count(),
            capacity.iter().sum(),
        )
    }
}

/// All P2 destinations of this P1 share one immutable payload. Descriptor
/// boundaries are relative to the destination, including source-side slots.
pub(super) struct Block {
    payload: Arc<Payload>,
    start: Offsets,
    end: Offsets,
}

impl Block {
    #[inline]
    fn groups(&self) -> &[Descriptor] {
        &self.payload.groups[self.start.groups..self.end.groups]
    }
    #[inline]
    fn events(&self) -> &[u32] {
        &self.payload.events[self.start.events..self.end.events]
    }
    #[inline]
    fn clicks(&self) -> &[(i64, u64)] {
        &self.payload.clicks[self.start.clicks..self.end.clicks]
    }
    #[inline]
    fn campaigns(&self) -> &[u32] {
        &self.payload.campaigns[self.start.clicks..self.end.clicks]
    }
    #[inline]
    fn event(&self, stamp: u32) -> [u32; 2] {
        if stamp < super::SIDE {
            [0, stamp]
        } else {
            self.payload.sources[self.start.sources + (stamp ^ super::SIDE) as usize]
        }
    }
}

enum Building {
    Rows(Destinations),
    Groups {
        slots: AHashMap<u32, u32>,
        rows: People,
        side_counts: Vec<usize>,
    },
}

pub(super) struct Builder {
    mask: usize,
    inner: Building,
}

impl Builder {
    pub fn new(plan: Plan) -> Self {
        Self {
            mask: plan.user_parts - 1,
            inner: if plan.forward_blocks {
                Building::Groups {
                    slots: AHashMap::default(),
                    rows: People::default(),
                    side_counts: vec![0; plan.user_parts],
                }
            } else {
                Building::Rows(Destinations::new(plan))
            },
        }
    }

    // Resolver caches this slot with the raw spelling, so repeated event
    // and click rows avoid looking up the resolved person a second time.
    pub fn slot(&mut self, person: u32) -> u32 {
        match &mut self.inner {
            Building::Rows(_) => person,
            Building::Groups { slots, rows, .. } => {
                *slots.entry(person).or_insert_with(|| rows.group(person))
            }
        }
    }

    pub fn event(&mut self, event: PersonEvent, data: &[EventData]) {
        match &mut self.inner {
            Building::Rows(dest) => dest
                .get(event.person as usize & self.mask)
                .events
                .push(event),
            Building::Groups {
                rows, side_counts, ..
            } => {
                if event.stamp >= super::SIDE {
                    let person = rows.groups[event.person as usize].0;
                    side_counts[person as usize & self.mask] += 1;
                }
                rows.event(event, data);
            }
        }
    }

    pub fn click(&mut self, click: Click) {
        match &mut self.inner {
            Building::Rows(dest) => dest
                .get(click.person as usize & self.mask)
                .clicks
                .push(click),
            Building::Groups { rows, .. } => rows.click(click),
        }
    }

    pub fn finish(self) -> Vec<(usize, Packet)> {
        match self.inner {
            Building::Rows(dest) => dest
                .into_blocks()
                .into_iter()
                .map(|(b, p)| (b, Packet::Rows(p)))
                .collect(),
            Building::Groups {
                rows, side_counts, ..
            } => pack(rows, side_counts, self.mask),
        }
    }
}

fn index(n: usize) -> u32 {
    u32::try_from(n).expect("forwarded person block exceeds u32 range")
}

fn pack(mut source: People, side_counts: Vec<usize>, mask: usize) -> Vec<(usize, Packet)> {
    if source.groups.is_empty() {
        return Vec::new();
    }
    let mut bounds: Vec<Offsets> = side_counts
        .into_iter()
        .map(|sources| Offsets {
            sources,
            ..Offsets::default()
        })
        .collect();
    for (person, counts) in &source.groups {
        let end = &mut bounds[*person as usize & mask];
        end.groups += 1;
        end.events += counts.events;
        end.clicks += counts.clicks;
    }
    // Turn destination totals into exclusive starts; the extra endpoint also
    // describes the exact total to allocate once for each payload column.
    let mut total = Offsets::default();
    for position in &mut bounds {
        let size = *position;
        *position = total;
        total.advance(size);
    }
    bounds.push(total);
    let mut heads = bounds[..=mask].to_vec();
    let mut payload = Payload::new(total);
    for (person, counts) in &mut source.groups {
        let dest = *person as usize & mask;
        let head = &mut heads[dest];
        let start = bounds[dest];
        let e = head.events;
        let c = head.clicks;
        head.events += counts.events;
        head.clicks += counts.clicks;
        payload.groups[head.groups] = Descriptor {
            person: *person,
            event_end: index(head.events - start.events),
            click_end: index(head.clicks - start.clicks),
            sales: index(counts.sales),
        };
        head.groups += 1;
        counts.events = e;
        counts.clicks = c;
    }
    for event in source.events {
        let (person, cursor) = &mut source.groups[event.person as usize];
        let word = if event.stamp < super::SIDE {
            event.stamp
        } else {
            let dest = *person as usize & mask;
            let next = &mut heads[dest].sources;
            let slot = index(*next - bounds[dest].sources);
            assert!(slot < super::SIDE);
            payload.sources[*next] = [event.chunk, event.stamp];
            *next += 1;
            super::SIDE | slot
        };
        payload.events[cursor.events] = word;
        cursor.events += 1;
    }
    for click in source.clicks {
        let cursor = &mut source.groups[click.person as usize].1.clicks;
        payload.clicks[*cursor] = (click.time, click.id);
        payload.campaigns[*cursor] = click.campaign;
        *cursor += 1;
    }
    let payload = Arc::new(payload);
    bounds
        .windows(2)
        .enumerate()
        .filter(|(_, pair)| pair[0].groups != pair[1].groups)
        .map(|(part, pair)| {
            (
                part,
                Packet::Groups(Block {
                    payload: Arc::clone(&payload),
                    start: pair[0],
                    end: pair[1],
                }),
            )
        })
        .collect()
}

pub(super) fn finish(
    packets: Vec<Packet>,
    data: &[EventData],
    window: Window,
    plan: Plan,
    names: Option<&Names>,
) -> (SessionBatch, Vec<Click>) {
    if !plan.forward_blocks {
        let rows = packets
            .into_iter()
            .map(|p| match p {
                Packet::Rows(rows) => rows,
                Packet::Groups(_) => unreachable!(),
            })
            .collect();
        return super::second_pass(rows, data, window, plan, names);
    }
    let blocks = packets
        .into_iter()
        .map(|p| match p {
            Packet::Groups(block) => block,
            Packet::Rows(_) => unreachable!(),
        })
        .collect();
    merge(
        blocks,
        data,
        window,
        plan.flat,
        plan.user_parts.trailing_zeros(),
        names,
    )
}

/// Join the already contiguous P1 ranges for each final person. Link their
/// descriptors, rather than copying their payloads into another P2 buffer.
pub(super) fn finish_people(
    packets: Vec<Packet>,
    data: &[EventData],
    window: Window,
    names: Option<&Names>,
    fused: &crate::aggregate::FusedCampaigns,
    ids: &crate::records::ClickIds,
    partition_bits: u32,
) -> SessionBatch {
    let blocks: Vec<_> = packets
        .into_iter()
        .map(|p| match p {
            Packet::Groups(block) => block,
            Packet::Rows(_) => unreachable!("person fusion requires P1 group descriptors"),
        })
        .collect();
    const END: u64 = u64::MAX;
    // Representatives are dense IDs and their low bits select this very
    // partition. Bound storage by actual input for sparse/exceptional IDs.
    let n: usize = blocks.iter().map(|b| b.groups().len()).sum();
    let mut prior = Vec::with_capacity(n);
    let mut link_bases = Vec::with_capacity(blocks.len());
    let dense_limit = n.saturating_mul(4).max(64);
    let mut heads = Vec::<u64>::new();
    let mut sparse = AHashMap::<u32, u64>::default();
    let mask = (1u32 << partition_bits) - 1;
    let part = blocks
        .iter()
        .find_map(|b| b.groups().first())
        .map_or(0, |g| g.person & mask);
    for (block_idx, block) in blocks.iter().enumerate() {
        link_bases.push(prior.len());
        for (i, g) in block.groups().iter().enumerate() {
            debug_assert_eq!(g.person & mask, part);
            let slot = (g.person >> partition_bits) as usize;
            let head = if slot < dense_limit {
                if heads.len() <= slot {
                    heads.resize((slot + 1).next_power_of_two().min(dense_limit), END);
                }
                &mut heads[slot]
            } else {
                sparse.entry(g.person).or_insert(END)
            };
            let position = (u64::from(index(block_idx)) << 32) | u64::from(index(i));
            prior.push(std::mem::replace(head, position));
        }
    }
    fused.with_people(names, ids, |kernel| {
        let dense = heads.into_iter().enumerate().filter_map(|(slot, head)| {
            (head != END).then_some((part | ((slot as u32) << partition_bits), head))
        });
        for (person, mut head) in dense.chain(sparse) {
            kernel.compute_fragments(person, |rows| {
                while head != END {
                    let block_idx = (head >> 32) as usize;
                    let block = &blocks[block_idx];
                    let i = head as u32 as usize;
                    let descriptors = block.groups();
                    let g = &descriptors[i];
                    head = prior[link_bases[block_idx] + i];
                    let prev = i.checked_sub(1).map(|j| &descriptors[j]);
                    let events = prev.map_or(0, |g| g.event_end) as usize..g.event_end as usize;
                    let clicks = prev.map_or(0, |g| g.click_end) as usize..g.click_end as usize;
                    for &word in &block.events()[events] {
                        let (time, purchase) = if word < super::SIDE {
                            (window.start + i64::from(word), None)
                        } else {
                            let [chunk, stamp] = block.event(word);
                            data[chunk as usize].decode(window.start, stamp)
                        };
                        rows.event(time, purchase);
                    }
                    for (&(time, id), &campaign) in block.clicks()[clicks.clone()]
                        .iter()
                        .zip(&block.campaigns()[clicks])
                    {
                        rows.click(Click {
                            person,
                            time,
                            id,
                            campaign,
                        });
                    }
                }
            });
        }
    })
}

fn merge(
    blocks: Vec<Block>,
    data: &[EventData],
    window: Window,
    flat: bool,
    bits: u32,
    names: Option<&Names>,
) -> (SessionBatch, Vec<Click>) {
    let mut slots = AHashMap::default();
    let mut groups = Vec::<(u32, Counts)>::new();
    let mut event_count = 0;
    // P2 obtains all event/click/purchase counts from descriptors. It never
    // scans individual rows for counts or resolves an identity from the key.
    for block in &blocks {
        event_count += block.events().len();
        let (mut prev_event, mut prev_click) = (0, 0);
        for descriptor in block.groups() {
            let person = descriptor.person;
            let slot = if flat {
                let slot = (person >> bits) as usize;
                if groups.len() <= slot {
                    groups.resize(slot + 1, (0, Counts::default()));
                }
                groups[slot].0 = person;
                slot
            } else {
                *slots.entry(person).or_insert_with(|| {
                    let slot = groups.len();
                    groups.push((person, Counts::default()));
                    slot
                })
            };
            groups[slot].1.add(Counts {
                events: (descriptor.event_end - prev_event) as usize,
                sales: descriptor.sales as usize,
                clicks: (descriptor.click_end - prev_click) as usize,
            });
            (prev_event, prev_click) = (descriptor.event_end, descriptor.click_end);
        }
    }
    let slot_for = |p: u32| if flat { p >> bits } else { index(slots[&p]) };
    let events = blocks.iter().flat_map(|block| {
        let mut begin = 0;
        block.groups().iter().flat_map(move |group| {
            let end = group.event_end as usize;
            let rows = &block.events()[begin..end];
            begin = end;
            let person = slot_for(group.person);
            rows.iter().map(move |&word| {
                let [chunk, stamp] = block.event(word);
                PersonEvent {
                    person,
                    chunk,
                    stamp,
                }
            })
        })
    });
    let clicks = blocks.iter().flat_map(|block| {
        let mut begin = 0;
        block.groups().iter().flat_map(move |group| {
            let end = group.click_end as usize;
            let rows = block.clicks()[begin..end]
                .iter()
                .zip(&block.campaigns()[begin..end]);
            begin = end;
            let person = slot_for(group.person);
            rows.map(move |(&(time, id), &campaign)| Click {
                person,
                time,
                id,
                campaign,
            })
        })
    });
    People::finish_groups(
        groups,
        events,
        clicks,
        event_count,
        flat,
        window,
        data,
        names,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::Purchase;

    #[test]
    fn different_destinations_share_allocations_and_use_local_source_offsets() {
        let mut plan = Plan::new(0, false).unwrap();
        plan.user_parts = 8;
        plan.forward_blocks = true;
        let mut data = [EventData::default(), EventData::default()];
        let a = data[0].stamp(
            0,
            3,
            Some(Purchase {
                category: 1,
                cents: 5,
            }),
        );
        let b = data[1].stamp(0, i64::from(u32::MAX) + 19, None);
        let mut builder = Builder::new(plan);
        let second = builder.slot(5);
        let first = builder.slot(1);
        let same_part = builder.slot(9);
        builder.event(
            PersonEvent {
                person: second,
                chunk: 1,
                stamp: b,
            },
            &data,
        );
        builder.event(
            PersonEvent {
                person: same_part,
                chunk: 0,
                stamp: a,
            },
            &data,
        );
        builder.event(
            PersonEvent {
                person: first,
                chunk: 0,
                stamp: 17,
            },
            &data,
        );
        builder.click(Click {
            person: first,
            time: -5,
            id: u64::MAX - 7,
            campaign: 12,
        });
        builder.click(Click {
            person: second,
            time: 33,
            id: 1u64 << 63,
            campaign: 35,
        });
        let packets = builder.finish();
        assert_eq!(packets.len(), 2);
        assert_eq!((packets[0].0, packets[1].0), (1, 5));
        let (Packet::Groups(one), Packet::Groups(two)) = (&packets[0].1, &packets[1].1) else {
            panic!()
        };
        assert!(Arc::ptr_eq(&one.payload, &two.payload));
        assert_eq!(
            one.groups().iter().map(|g| g.person).collect::<Vec<_>>(),
            [1, 9]
        );
        assert_eq!(two.groups()[0].person, 5);
        assert_eq!(one.events(), [17, super::super::SIDE]);
        assert_eq!(two.events(), [super::super::SIDE]);
        assert_eq!(one.event(one.events()[1]), [0, a]);
        assert_eq!(two.event(two.events()[0]), [1, b]);
        assert_eq!(one.clicks(), [(-5, u64::MAX - 7)]);
        assert_eq!(two.clicks(), [(33, 1u64 << 63)]);
        assert_eq!(one.campaigns(), [12]);
        assert_eq!(two.campaigns(), [35]);
        let mut work = Work::default();
        for (_, p) in packets {
            p.account(&mut work);
        }
        assert_eq!(work.producer_payload_buffers, 5);
        assert_eq!(
            work.producer_payload_capacity,
            work.descriptor_bytes + work.event_bytes + work.click_bytes
        );
        assert_eq!(work.block_view_bytes, 2 * size_of::<Block>());
    }

    #[test]
    fn fused_forward_joins_sparse_and_dense_heads_without_copying_their_ranges() {
        use crate::aggregate::FusedCampaigns;
        use crate::records::ClickIds;
        let mut plan = Plan::new(0, false).unwrap();
        plan.forward_blocks = true;
        plan.user_parts = 8;
        let window = Window {
            start: -9,
            end: 5_000_000,
        };
        let mut data = [EventData::default(), EventData::default()];
        let stamp = data[1].stamp(
            window.start,
            1,
            Some(Purchase {
                category: 7,
                cents: 1234,
            }),
        );
        let mut packets = Vec::new();
        let sparse = u32::MAX - 5;
        for (chunk, stamp) in [(0, 0), (1, stamp)] {
            let mut builder = Builder::new(plan);
            for person in [2, 10, sparse] {
                let group = builder.slot(person);
                builder.event(
                    PersonEvent {
                        person: group,
                        chunk,
                        stamp,
                    },
                    &data,
                );
                if chunk == 0 {
                    builder.click(Click {
                        person: group,
                        time: window.start,
                        id: 9,
                        campaign: 1,
                    });
                }
            }
            packets.extend(builder.finish().into_iter().map(|(_, p)| p));
        }
        let fused = FusedCampaigns::new(2, window);
        let ids = ClickIds::new(Names::default(), Vec::new());
        let actual = finish_people(packets, &data, window, None, &fused, &ids, 3);
        let mut people = actual.daily.person;
        people.sort_unstable();
        assert_eq!(people, [2, 10, sparse]);
        assert_eq!(actual.daily.events, [2; 3]);
        assert_eq!(actual.daily.revenue, [1234; 3]);
        assert_eq!(actual.daily.category, [Some(7); 3]);
        let campaigns: Vec<_> = fused.finish(&[]).into_iter().collect();
        assert_eq!(campaigns.len(), 1);
        assert_eq!(campaigns[0].0, (1, -1));
        assert_eq!(campaigns[0].1.assisted_sessions, 3);
    }

    #[test]
    fn representative_ranges_join_across_p1_and_preserve_full_clicks_and_sales() {
        for flat in [false, true] {
            for wide in [false, true] {
                let mut plan = Plan::new(0, false).unwrap();
                plan.forward_blocks = true;
                plan.flat = flat;
                plan.user_parts = 8;
                let start = -9123;
                let end = if wide {
                    start + i64::from(u32::MAX) + 2500
                } else {
                    start + 3_000_000
                };
                let window = Window { start, end };
                let mut data = [EventData::default(), EventData::default()];
                let sale = data[1].stamp(
                    start,
                    end - 100,
                    Some(Purchase {
                        category: 9,
                        cents: 333,
                    }),
                );
                let late = data[1].stamp(start, end - 1, None);
                let mut one = Builder::new(plan);
                let mut two = Builder::new(plan);
                // Two raw spellings from one P1 return the same destination slot.
                let a = one.slot(10);
                assert_eq!(a, one.slot(10));
                let b = one.slot(2);
                let only = one.slot(18);
                let other = two.slot(10);
                let later = two.slot(2);
                one.event(
                    PersonEvent {
                        person: a,
                        chunk: 0,
                        stamp: 3,
                    },
                    &data,
                );
                two.event(
                    PersonEvent {
                        person: other,
                        chunk: 1,
                        stamp: sale,
                    },
                    &data,
                );
                two.event(
                    PersonEvent {
                        person: later,
                        chunk: 1,
                        stamp: late,
                    },
                    &data,
                );
                let keys = [u64::MAX - 50, 1u64 << 63, 1, 17];
                for (slot, id) in [(a, keys[0]), (b, keys[1]), (only, keys[2])] {
                    one.click(Click {
                        person: slot,
                        id,
                        time: start + 1,
                        campaign: 6,
                    });
                }
                two.click(Click {
                    person: other,
                    id: keys[3],
                    time: start + 1,
                    campaign: 8,
                });
                let packets: Vec<_> = one
                    .finish()
                    .into_iter()
                    .chain(two.finish())
                    .map(|(dest, p)| {
                        assert_eq!(dest, 2);
                        p
                    })
                    .collect();
                let mut work = Work::default();
                for p in &packets {
                    p.account(&mut work);
                }
                assert_eq!((work.groups, work.events, work.clicks), (5, 3, 4));
                assert_eq!(
                    (work.descriptor_bytes, work.event_bytes, work.click_bytes),
                    (5 * 16, 3 * 4 + (1 + usize::from(wide)) * 8, 4 * 20)
                );
                let (actual, actual_clicks) = finish(packets, &data, window, plan, None);

                let mut reference = People::default();
                let a = reference.group(10);
                let b = reference.group(2);
                reference.event(
                    PersonEvent {
                        person: a,
                        chunk: 0,
                        stamp: 3,
                    },
                    &data,
                );
                reference.event(
                    PersonEvent {
                        person: a,
                        chunk: 1,
                        stamp: sale,
                    },
                    &data,
                );
                reference.event(
                    PersonEvent {
                        person: b,
                        chunk: 1,
                        stamp: late,
                    },
                    &data,
                );
                reference.click(Click {
                    person: a,
                    id: keys[0],
                    time: start + 1,
                    campaign: 6,
                });
                reference.click(Click {
                    person: b,
                    id: keys[1],
                    time: start + 1,
                    campaign: 6,
                });
                reference.click(Click {
                    person: a,
                    id: keys[3],
                    time: start + 1,
                    campaign: 8,
                });
                let (expected, expected_clicks) = reference.finish(window, &data, None);
                assert!(actual_clicks == expected_clicks);
                assert_eq!(actual.daily.person, expected.daily.person);
                assert_eq!(actual.daily.day, expected.daily.day);
                assert_eq!(actual.daily.sessions, expected.daily.sessions);
                assert_eq!(actual.daily.events, expected.daily.events);
                assert_eq!(actual.daily.revenue, expected.daily.revenue);
                assert_eq!(actual.daily.category, expected.daily.category);
            }
        }
    }
}
