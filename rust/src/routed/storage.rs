use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// One contiguous backing for a reader's adaptive event buckets. Only the
/// observed prefix of a bucket is initialized. The caller stores overflow
/// records in its existing exact lane, so capacity estimates affect speed only.
pub(super) struct Stripe<T:Copy> {
    slots: Vec<MaybeUninit<T>>,
    used: Vec<u32>,
    stride:usize,
}

impl<T:Copy> Stripe<T> {
    pub fn new(parts:usize, stride:usize)->Self {
        let count=stride.checked_mul(parts).expect("bucket stripe capacity");
        let mut slots=Vec::with_capacity(count);
        slots.resize_with(count,MaybeUninit::uninit);
        crate::runtime::huge_pages(&slots);
        Self{slots,used:vec![0;parts],stride}
    }
    #[inline]
    pub fn push(&mut self,part:usize,row:T)->bool {
        let used=self.used[part] as usize;
        if used==self.stride {return false;}
        self.slots[part*self.stride+used].write(row);
        self.used[part]+=1;
        true
    }
    pub fn prefix(&self,part:usize)->&[T] {
        let start=part*self.stride;
        let src=&self.slots[start..start+self.used[part] as usize];
        // push initializes entries in order before increasing the visible length.
        unsafe {std::slice::from_raw_parts(src.as_ptr().cast::<T>(),src.len())}
    }
    pub fn capacity_bytes(&self)->usize {self.slots.capacity()*size_of::<T>()}
}

const PAGE: usize = 4096;
const REGION: usize = 2 << 20;

struct Region(NonNull<u8>);

// Storage stays at its allocated address and is released only by its owner.
// All assignment/reclamation of the nonoverlapping pages is under Pool's lock.
unsafe impl Send for Region {}

impl Region {
    fn new() -> Self {
        let layout = Layout::from_size_align(REGION, REGION).unwrap();
        let base =
            NonNull::new(unsafe { alloc(layout) }).unwrap_or_else(|| handle_alloc_error(layout));
        let uninit =
            unsafe { std::slice::from_raw_parts(base.as_ptr().cast::<MaybeUninit<u8>>(), REGION) };
        crate::runtime::huge_pages(uninit);
        Self(base)
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.0.as_ptr(),
                Layout::from_size_align(REGION, REGION).unwrap(),
            )
        }
    }
}

#[derive(Clone, Copy)]
struct Token {
    region: usize,
    page: usize,
}

#[derive(Default)]
struct State {
    regions: Vec<Region>,
    unused: Vec<Token>,
    recycled: Vec<Token>,
}

#[derive(Default)]
pub(super) struct PoolStats {
    pub regions: AtomicUsize,
    pub pages: AtomicUsize,
    pub reused: AtomicUsize,
}

struct Pool {
    state: Mutex<State>,
    stats: Arc<PoolStats>,
}

impl Pool {
    fn claim(&self) -> (Token, NonNull<u8>) {
        let mut state = self.state.lock().unwrap();
        let token = if let Some(t) = state.recycled.pop() {
            self.stats.reused.fetch_add(1, Ordering::Relaxed);
            t
        } else {
            if state.unused.is_empty() {
                let region = state.regions.len();
                state.regions.push(Region::new());
                self.stats.regions.fetch_add(1, Ordering::Relaxed);
                state
                    .unused
                    .extend((0..REGION / PAGE).rev().map(|page| Token { region, page }));
            }
            state.unused.pop().unwrap()
        };
        let ptr = unsafe {
            NonNull::new_unchecked(
                state.regions[token.region]
                    .0
                    .as_ptr()
                    .add(token.page * PAGE),
            )
        };
        self.stats.pages.fetch_add(1, Ordering::Relaxed);
        (token, ptr)
    }
}

pub(super) struct Arena {
    shared: Option<Arc<Pool>>,
    pub stats: Arc<PoolStats>,
}

impl Arena {
    pub fn new(reuse: bool) -> Self {
        let stats = Arc::new(PoolStats::default());
        let shared = reuse.then(|| {
            Arc::new(Pool {
                state: Mutex::default(),
                stats: Arc::clone(&stats),
            })
        });
        Self { shared, stats }
    }

    pub fn pack<T: Copy>(&self, rows: impl IntoIterator<Item = T>) -> Pages<T> {
        let pool = self.shared.clone().unwrap_or_else(|| {
            Arc::new(Pool {
                state: Mutex::default(),
                stats: Arc::clone(&self.stats),
            })
        });
        let cap = PAGE / std::mem::size_of::<T>();
        assert!(cap > 0 && std::mem::align_of::<T>() <= PAGE);
        let mut pages = Pages {
            pool,
            pieces: Vec::new(),
        };
        for row in rows {
            if pages.pieces.last().is_none_or(|(_, _, n)| *n == cap) {
                let (key, ptr) = pages.pool.claim();
                pages.pieces.push((key, ptr.cast::<T>(), 0));
            }
            let (_, dest, n) = pages.pieces.last_mut().unwrap();
            // Each page belongs to this builder until Pages is dropped. The
            // last partial page exposes its initialized prefix only.
            unsafe {
                dest.as_ptr().add(*n).write(row);
            }
            *n += 1;
        }
        pages
    }
}

pub(super) struct Pages<T: Copy> {
    pool: Arc<Pool>,
    pieces: Vec<(Token, NonNull<T>, usize)>,
}

impl<T: Copy> Pages<T> {
    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.pieces.iter().flat_map(|(_, ptr, n)| {
            unsafe { std::slice::from_raw_parts(ptr.as_ptr(), *n) }
                .iter()
                .copied()
        })
    }
}

impl<T: Copy> Drop for Pages<T> {
    fn drop(&mut self) {
        let mut state = self.pool.state.lock().unwrap();
        state
            .recycled
            .extend(self.pieces.drain(..).map(|(key, _, _)| key));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimated_stripes_expose_only_written_rows_and_signal_overflow() {
        let mut s=Stripe::new(4,2);
        assert!(s.push(1,10));
        assert!(s.push(3,30));
        assert!(s.push(1,11));
        assert!(!s.push(1,12));
        assert_eq!(s.prefix(0),&[]);
        assert_eq!(s.prefix(1),&[10,11]);
        assert_eq!(s.prefix(3),&[30]);
    }

    #[test]
    fn released_pages_can_hold_other_records_without_touching_live_ranges() {
        let arena = Arena::new(true);
        let live = arena.pack(0..4000u32);
        let old = arena.pack((0..10000u64).map(|n| [n; 2]));
        assert_eq!(old.iter().last(), Some([9999; 2]));
        drop(old);
        let next = arena.pack((0..10000u64).map(|n| [n; 3]));
        assert!(arena.stats.reused.load(Ordering::Relaxed) > 0);
        assert!(next.iter().enumerate().all(|(i, row)| row == [i as u64; 3]));
        assert!(live.iter().eq(0..4000u32));
    }
}
