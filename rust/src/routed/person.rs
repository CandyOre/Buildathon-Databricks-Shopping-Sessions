//! Consume all of a person's input before reusing the worker's session/click
//! scratch. A dense group slot is a build-time link, never an external ID.
use super::{EventData, People};
use crate::aggregate::{FusedCampaigns, SessionBatch};
use crate::input::{Names, Window};
use crate::records::ClickIds;

const END: u32 = u32::MAX;

pub(super) fn finish(
    mut rows: People,
    data: &[EventData],
    window: Window,
    names: Option<&Names>,
    fused: &FusedCampaigns,
    ids: &ClickIds,
) -> SessionBatch {
    // Every row already belongs to a known group. Reuse the slot field as
    // the next link; this avoids a second partition-wide array of clicks or
    // resolved events. Zero-event groups never enter the consumer.
    for (_, heads) in &mut rows.groups {
        heads.events = END as usize;
        heads.clicks = END as usize;
    }
    for (i, e) in rows.events.iter_mut().enumerate() {
        let head = &mut rows.groups[e.person as usize].1.events;
        e.person = std::mem::replace(head, i) as u32;
    }
    for (i, c) in rows.clicks.iter_mut().enumerate() {
        let head = &mut rows.groups[c.person as usize].1.clicks;
        c.person = std::mem::replace(head, i) as u32;
    }
    assert!(rows.events.len() <= END as usize && rows.clicks.len() <= END as usize);
    fused.with_people(names, ids, |kernel| {
        for (person, heads) in rows.groups {
            if heads.events == END as usize {
                continue;
            }
            let mut next = heads.events as u32;
            let events = std::iter::from_fn(|| {
                if next == END {
                    return None;
                }
                let e = &rows.events[next as usize];
                next = e.person;
                Some(data[e.chunk as usize].decode(window.start, e.stamp))
            });
            let mut next = heads.clicks as u32;
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
