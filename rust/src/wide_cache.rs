use std::sync::OnceLock;

#[derive(Clone, Copy)]
struct Entry {
    key: [u64; 2],
    person: u32,
}

// A lock-free read cache in front of the exact shared wide-name registry.
// Each cell is published once. A collision can only cause another probe or
// a registry lookup. No reader can observe half of a published 128-bit key.
pub(crate) struct WideCache {
    cells: Box<[OnceLock<Entry>]>,
}

impl WideCache {
    pub(crate) fn new(linked: usize) -> Self {
        let len = linked
            .saturating_mul(4)
            .clamp(4096, 1 << 22)
            .next_power_of_two();
        let cells = (0..len).map(|_| OnceLock::new()).collect();
        Self { cells }
    }

    #[inline]
    fn home(&self, [hi, lo]: [u64; 2]) -> usize {
        let x = (hi.rotate_left(23) ^ lo).wrapping_mul(0xa24b_aed4_963e_e407);
        ((x ^ (x >> 33)) as usize) & (self.cells.len() - 1)
    }

    #[inline]
    pub(crate) fn resolve(&self, key: [u64; 2], lookup: impl Fn() -> u32) -> u32 {
        let home = self.home(key);
        for offset in 0..4 {
            let cell = &self.cells[(home + offset) & (self.cells.len() - 1)];
            let entry = cell.get_or_init(|| Entry {
                key,
                person: lookup(),
            });
            if entry.key == key {
                return entry.person;
            }
        }
        lookup()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_full_keys_survive_one_shared_hash_bucket() {
        let cache = WideCache::new(0);
        for _ in 0..2 {
            for hi in 0..15u64 {
                let key = [hi, hi.rotate_left(23)];
                assert_eq!(cache.home(key), 0);
                assert_eq!(cache.resolve(key, || hi as u32 + 1), hi as u32 + 1);
            }
        }
    }
}
