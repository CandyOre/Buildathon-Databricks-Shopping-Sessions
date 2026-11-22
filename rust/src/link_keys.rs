//! Intern canonical 128-bit link endpoints by full value within each input
//! chunk. Global insertion is batched by shard and only a newly registered
//! endpoint constructs its original canonical text for lexical union.
use super::{EXTERNAL_NAME, NameShard, Names, SHARDS, id_for, packed_name, routed_hex, text};
use crate::csv::{Chunk, fields};
use ahash::AHashMap;
use anyhow::Result;
use std::sync::Arc;

#[derive(Default)]
pub(super) struct Work {
    pub occurrences: usize,
    pub local_unique: usize,
    pub locks: usize,
}

fn intern_words(state: &mut NameShard, shard: usize, key: [u64; 2]) -> u32 {
    let NameShard {
        wide,
        offset,
        names,
        keys,
        ..
    } = state;
    *wide
        .get_or_insert_with(AHashMap::default)
        .entry(key)
        .or_insert_with(|| {
            let id = id_for(shard, *offset + keys.len());
            let mut spelling = [b'u'; 33];
            const HEX: &[u8; 16] = b"0123456789abcdef";
            for (dst, byte) in spelling[1..]
                .chunks_exact_mut(2)
                .zip(key.into_iter().flat_map(u64::to_be_bytes))
            {
                dst[0] = HEX[(byte >> 4) as usize];
                dst[1] = HEX[(byte & 15) as usize];
            }
            keys.push(EXTERNAL_NAME | names.len() as u64);
            names.push(Arc::from(text(&spelling)));
            id
        })
}

pub(super) fn read(chunk: Chunk, external: &Names) -> Result<(Vec<(u64, u64)>, Work)> {
    let mut edges = Vec::new();
    let mut slots = AHashMap::default();
    // Only these positions use temporary local slot numbers. No valid user
    // spelling or packed graph key needs to be reserved as a tag.
    let mut positions = Vec::new();
    let mut endpoint = |bytes: &[u8], position: usize| {
        if let Some(short) = packed_name(bytes) {
            return short;
        }
        if let Some(key) = routed_hex(bytes, b'u') {
            let next = u32::try_from(slots.len()).expect("too many local wide endpoints");
            let slot = *slots.entry(key).or_insert(next);
            positions.push(u32::try_from(position).expect("link chunk exceeds endpoint index"));
            return u64::from(slot);
        }
        let shard = external.hash.hash_one(bytes) as usize % SHARDS;
        EXTERNAL_NAME | u64::from(external.intern_in(shard, text(bytes)))
    };
    chunk.scan(|row| {
        if let Some([left, right]) = fields(row) {
            if !left.is_empty() && !right.is_empty() && left != right {
                let position = 2 * edges.len();
                edges.push((endpoint(left, position), endpoint(right, position + 1)));
            }
        }
    })?;
    let mut work = Work {
        occurrences: positions.len(),
        local_unique: slots.len(),
        locks: 0,
    };
    if slots.is_empty() {
        return Ok((edges, work));
    }
    let mut ids = vec![0; slots.len()];
    let mut shards: Vec<Vec<([u64; 2], u32)>> = (0..SHARDS).map(|_| Vec::new()).collect();
    for (key, slot) in slots {
        shards[external.hash.hash_one(key) as usize % SHARDS].push((key, slot));
    }
    for (shard, batch) in shards.into_iter().enumerate() {
        if batch.is_empty() {
            continue;
        }
        work.locks += 1;
        let mut state = external.lock_for_intern(shard);
        for (key, slot) in batch {
            ids[slot as usize] = intern_words(&mut state, shard, key);
        }
    }
    for position in positions {
        let edge = &mut edges[position as usize / 2];
        let word = if position & 1 == 0 {
            &mut edge.0
        } else {
            &mut edge.1
        };
        *word = EXTERNAL_NAME | u64::from(ids[*word as usize]);
    }
    Ok((edges, work))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn repeated_full_width_edges_resolve_across_files_and_keep_lexical_roots() -> Result<()> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!("link-words-{}-{nonce}", std::process::id()));
        let links = path.join("links");
        fs::create_dir_all(&links)?;
        let a = "u00000000000000000000000000000001";
        let hi = "u10000000000000000000000000000001";
        let lo = "u00000000000000000000000000000002";
        let caps = "u0000000000000000000000000000000A";
        let self_only = "u00000000000000000000000000000003";
        let header = "user_id_a,user_id_b\n";
        let repeated = format!("\"{a}\",{hi}\n").repeat(64);
        fs::write(
            links.join("a.csv"),
            format!("{header}{repeated}{a},\n{self_only},{self_only}\n"),
        )?;
        let external = Names {
            wide_users: true,
            ..Names::default()
        };
        let chunk = crate::csv::chunks(&links)?.pop().unwrap();
        let (edges, work) = read(chunk, &external)?;
        assert_eq!(edges.len(), 64);
        assert!(
            edges
                .iter()
                .all(|pair| *pair == edges[0] && pair.0 != pair.1)
        );
        assert_eq!((work.occurrences, work.local_unique), (128, 2));
        assert!((1..=2).contains(&work.locks));

        fs::write(
            links.join("b.csv"),
            format!("{header}{hi},{lo}\n{caps},zzzz uncommon user\n"),
        )?;
        let mut users = Names::default();
        let (aliases, _) = super::super::read_numeric_aliases(&path, &mut users, |_| None)?;
        let linked = aliases
            .linked_wide(&users, routed_hex(a.as_bytes(), b'u').unwrap())
            .unwrap();
        users.with_spelling(linked, |name| assert_eq!(name, a));
        for member in [hi, lo] {
            assert_eq!(
                aliases.linked_wide(&users, routed_hex(member.as_bytes(), b'u').unwrap()),
                Some(linked)
            );
        }
        assert_eq!(
            aliases.linked_wide(&users, routed_hex(self_only.as_bytes(), b'u').unwrap()),
            None
        );
        let upper = aliases.linked_other(&users, caps.as_bytes()).unwrap();
        assert_ne!(upper, linked);
        users.with_spelling(upper, |name| assert_eq!(name, caps));
        assert_eq!(
            aliases.linked_other(&users, b"zzzz uncommon user"),
            Some(upper)
        );
        fs::remove_file(links.join("a.csv"))?;
        fs::remove_file(links.join("b.csv"))?;
        fs::remove_dir(links)?;
        fs::remove_dir(path)?;
        Ok(())
    }
}
