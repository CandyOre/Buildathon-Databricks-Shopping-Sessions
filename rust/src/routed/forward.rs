//! Group a P1 worker's outbound rows by their resolved person. Each person
//! appears once per destination block, irrespective of its raw spellings.
use super::{Counts, Destinations, EventData, Forward, People, PersonEvent, Plan};
use crate::aggregate::SessionBatch;
use crate::input::{Names, Window};
use crate::records::Click;
use ahash::AHashMap;
use std::mem::size_of;

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
}

impl Packet {
    pub fn account(&self, work: &mut Work) {
        match self {
            Self::Rows(rows) => {
                work.events += rows.events.len();
                work.clicks += rows.clicks.len();
                work.event_bytes += rows.events.len() * size_of::<PersonEvent>();
                work.click_bytes += rows.clicks.len() * size_of::<Click>();
            }
            Self::Groups(block) => {
                work.events += block.events.len();
                work.clicks += block.clicks.len();
                work.groups += block.groups.len();
                work.descriptor_bytes += block.groups.len() * size_of::<Descriptor>();
                work.event_bytes += block.events.len() * size_of::<[u32; 2]>();
                work.click_bytes +=
                    block.clicks.len() * (size_of::<(i64, u64)>() + size_of::<u32>());
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Descriptor {
    person: u32,
    event_end: u32,
    click_end: u32,
    sales: u32,
}

#[derive(Default)]
pub(super) struct Block {
    groups: Vec<Descriptor>,
    // Event chunk indexes preserve full purchase/time sidecar coordinates.
    events: Vec<[u32; 2]>,
    // Splitting campaign avoids four padding bytes per key/timestamp pair.
    clicks: Vec<(i64, u64)>,
    campaigns: Vec<u32>,
}

impl Block {
    fn prefix(&self) -> (usize, usize) {
        self.groups
            .last()
            .map_or((0, 0), |g| (g.event_end as usize, g.click_end as usize))
    }
}

enum Building {
    Rows(Destinations),
    Groups {
        slots: AHashMap<u32, u32>,
        rows: People,
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
            Building::Groups { slots, rows } => {
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
            Building::Groups { rows, .. } => rows.event(event, data),
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
            Building::Groups { rows, .. } => pack(rows, self.mask),
        }
    }
}

fn index(n: usize) -> u32 {
    u32::try_from(n).expect("forwarded person block exceeds u32 range")
}

fn pack(mut source: People, mask: usize) -> Vec<(usize, Packet)> {
    let mut blocks: Vec<_> = (0..=mask).map(|_| Block::default()).collect();
    for (person, counts) in &mut source.groups {
        let block = &mut blocks[*person as usize & mask];
        let (event_begin, click_begin) = block.prefix();
        block.groups.push(Descriptor {
            person: *person,
            event_end: index(event_begin + counts.events),
            click_end: index(click_begin + counts.clicks),
            sales: index(counts.sales),
        });
        counts.events = event_begin;
        counts.clicks = click_begin;
    }
    for block in &mut blocks {
        let (events, clicks) = block.prefix();
        block.events.resize(events, [0; 2]);
        block.clicks.resize(clicks, (0, 0));
        block.campaigns.resize(clicks, 0);
    }
    for e in source.events {
        let (person, heads) = &mut source.groups[e.person as usize];
        let dst = &mut blocks[*person as usize & mask];
        dst.events[heads.events] = [e.chunk, e.stamp];
        heads.events += 1;
    }
    for c in source.clicks {
        let (person, heads) = &mut source.groups[c.person as usize];
        let dst = &mut blocks[*person as usize & mask];
        dst.clicks[heads.clicks] = (c.time, c.id);
        dst.campaigns[heads.clicks] = c.campaign;
        heads.clicks += 1;
    }
    blocks
        .into_iter()
        .enumerate()
        .filter(|(_, b)| !b.groups.is_empty())
        .map(|(part, block)| (part, Packet::Groups(block)))
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

fn merge(
    mut blocks: Vec<Block>,
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
    for block in &mut blocks {
        event_count += block.events.len();
        let (mut prev_event, mut prev_click) = (0, 0);
        for descriptor in &mut block.groups {
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
            descriptor.person = index(slot);
        }
    }
    drop(slots);
    let events = blocks.iter().flat_map(|block| {
        let mut begin = 0;
        block.groups.iter().flat_map(move |group| {
            let end = group.event_end as usize;
            let rows = &block.events[begin..end];
            begin = end;
            rows.iter().map(move |&[chunk, stamp]| PersonEvent {
                person: group.person,
                chunk,
                stamp,
            })
        })
    });
    let clicks = blocks.iter().flat_map(|block| {
        let mut begin = 0;
        block.groups.iter().flat_map(move |group| {
            let end = group.click_end as usize;
            let rows = block.clicks[begin..end]
                .iter()
                .zip(&block.campaigns[begin..end]);
            begin = end;
            rows.map(move |(&(time, id), &campaign)| Click {
                person: group.person,
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
    fn representative_ranges_join_across_p1_and_preserve_full_clicks_and_sales() {
        for flat in [false, true] {
            for wide in [false, true] {
                let mut plan = Plan::new(0).unwrap();
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
                    (5 * 16, 3 * 8, 4 * 20)
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
