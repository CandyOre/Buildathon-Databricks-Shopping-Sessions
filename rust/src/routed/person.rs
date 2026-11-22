//! Consume all of a person's input before reusing the worker's session/click
//! scratch. A dense group slot is a build-time link, never an external ID.
use super::{EventData, People, PersonEvent};
use crate::aggregate::{FusedCampaigns, SessionBatch};
use crate::input::{Names, Window};
use crate::records::{Click, ClickIds};

const END: u32 = u32::MAX;

#[derive(Default)]
pub(super) struct Lists {
    heads: Vec<(u32, u32, u32)>,
    events: Vec<PersonEvent>,
    clicks: Vec<Click>,
}

pub(super) enum Local {
    Counts(People),
    Lists(Lists),
}

impl Local {
    pub fn new(fused: bool) -> Self {
        if fused {
            Self::Lists(Lists::default())
        } else {
            Self::Counts(People::default())
        }
    }

    pub fn group(&mut self, person: u32) -> u32 {
        match self {
            Self::Counts(rows) => rows.group(person),
            Self::Lists(rows) => {
                let slot = u32::try_from(rows.heads.len()).expect("too many local people");
                rows.heads.push((person, END, END));
                slot
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Counts(r) => r.groups.len(),
            Self::Lists(r) => r.heads.len(),
        }
    }

    #[inline]
    pub fn event(&mut self, mut e: PersonEvent, data: &[EventData]) {
        match self {
            Self::Counts(rows) => rows.event(e, data),
            Self::Lists(rows) => {
                let index = rows.events.len();
                assert!(index < END as usize);
                e.person = std::mem::replace(&mut rows.heads[e.person as usize].1, index as u32);
                rows.events.push(e);
            }
        }
    }

    #[inline]
    pub fn click(&mut self, mut c: Click) {
        match self {
            Self::Counts(rows) => rows.click(c),
            Self::Lists(rows) => {
                let index = rows.clicks.len();
                assert!(index < END as usize);
                c.person = std::mem::replace(&mut rows.heads[c.person as usize].2, index as u32);
                rows.clicks.push(c);
            }
        }
    }
}

pub(super) fn finish(
    rows: Lists,
    data: &[EventData],
    window: Window,
    names: Option<&Names>,
    fused: &FusedCampaigns,
    ids: &ClickIds,
) -> SessionBatch {
    // The producer recorded the predecessor with each initial append.
    fused.with_people(names, ids, |kernel| {
        for (person, mut next, click_head) in rows.heads {
            if next == END {
                continue;
            }
            let events = std::iter::from_fn(|| {
                if next == END {
                    return None;
                }
                let e = &rows.events[next as usize];
                next = e.person;
                Some(data[e.chunk as usize].decode(window.start, e.stamp))
            });
            let mut next = click_head;
            let clicks = std::iter::from_fn(|| {
                if next == END {
                    return None;
                }
                let mut c = rows.clicks[next as usize];
                next = c.person;
                c.person = person;
                Some(c)
            });
            kernel.compute(person, events, clicks);
        }
    })
}
