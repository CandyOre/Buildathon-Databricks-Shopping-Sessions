//! One exact short user key and its resolved destination per 64-bit slot.
use super::{Target, mixed_bits};
use ahash::AHashMap;

const KEY_BITS: u32 = 42;
pub(super) const KEY_MASK: u64 = (1 << KEY_BITS) - 1;
const MAX_GROUP: u32 = (1 << (63 - KEY_BITS)) - 2;
const OVERFLOW: u64 = 1;

pub(super) struct NarrowTargets {
    slots: Vec<u64>,
    used: usize,
    bucket_bits: u32,
    large_groups: AHashMap<u64, Target>,
}

impl NarrowTargets {
    pub fn new(bucket_bits: u32) -> Self {
        Self {
            slots: Vec::new(),
            used: 0,
            bucket_bits,
            large_groups: AHashMap::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.used
    }

    pub fn slots(&self) -> usize {
        self.slots.len()
    }

    pub fn large_groups(&self) -> usize {
        self.large_groups.len()
    }

    #[inline]
    fn find(slots: &[u64], key: u64, hash: u64) -> usize {
        let mask = slots.len() - 1;
        let mut i = hash as usize & mask;
        while slots[i] != 0 && slots[i] & KEY_MASK != key {
            i = (i + 1) & mask;
        }
        i
    }

    #[cold]
    fn grow(&mut self) {
        let mut larger = vec![0; (self.slots.len() * 2).max(32)];
        for &word in &self.slots {
            if word != 0 {
                let key = word & KEY_MASK;
                let i = Self::find(&larger, key, mixed_bits(key) >> self.bucket_bits);
                larger[i] = word;
            }
        }
        self.slots = larger;
    }

    #[inline]
    pub fn get_or_insert_with(&mut self, key: u64, resolve: impl FnOnce() -> Target) -> Target {
        debug_assert!(key <= KEY_MASK);
        if self.slots.is_empty() {
            self.grow();
        }
        let hash = mixed_bits(key) >> self.bucket_bits;
        let mut i = Self::find(&self.slots, key, hash);
        let code = self.slots[i] >> KEY_BITS;
        if code >= 2 {
            let group = ((code >> 1) - 1) as u32;
            return if code & 1 == 0 {
                Target::Here(group)
            } else {
                Target::Forward(group)
            };
        }
        if code == OVERFLOW {
            return self.large_groups[&key];
        }
        // Grow on actual insertion, so frequent reads of an occupied table
        // do not require a speculative resize or touch a second cache.
        if self.used >= self.slots.len() - self.slots.len() / 4 {
            self.grow();
            i = Self::find(&self.slots, key, hash);
        }
        let target = resolve();
        let (group, forward) = match target {
            Target::Here(group) => (group, 0),
            Target::Forward(group) => (group, 1),
        };
        let code = if group <= MAX_GROUP {
            ((u64::from(group) + 1) << 1) | forward
        } else {
            self.large_groups.insert(key, target);
            OVERFLOW
        };
        self.slots[i] = (code << KEY_BITS) | key;
        self.used += 1;
        target
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_keys_survive_collisions_growth_and_overflowing_destinations() {
        let mut targets = NarrowTargets::new(6);
        let mut keys = vec![0, KEY_MASK];
        // All these distinct keys reach the same P1 partition and the same
        // first 32-slot table position; resizing must preserve full equality.
        keys.extend((1..).filter(|&k| mixed_bits(k) & 2047 == 0).take(120));
        let expected: Vec<_> = (0..keys.len())
            .map(|i| match i % 4 {
                0 => Target::Here(i as u32),
                1 => Target::Forward(MAX_GROUP),
                2 => Target::Forward(MAX_GROUP + 1),
                _ => Target::Here(u32::MAX),
            })
            .collect();
        for (&key, &target) in keys.iter().zip(&expected) {
            assert_eq!(targets.get_or_insert_with(key, || target), target);
        }
        for (i, key) in keys.into_iter().enumerate().rev() {
            assert_eq!(
                targets.get_or_insert_with(key, || unreachable!()),
                expected[i]
            );
        }
        assert_eq!(targets.len(), expected.len());
        assert!(targets.slots.len() > 32);
    }
}
