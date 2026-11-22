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
    region:u32,
    page:u16,
    last_input:u8,
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
    pub input_pages:AtomicUsize,
    pub input_bytes:AtomicUsize,
    pub streaming_bytes:AtomicUsize,
    pub event_pages_used_for_clicks:AtomicUsize,
}

struct Pool {
    state: Mutex<State>,
    stats: Arc<PoolStats>,
}

impl Pool {
    fn claim(&self) -> (Token, NonNull<u8>) {
        let mut state = self.state.lock().unwrap();
        self.claim_locked(&mut state)
    }
    fn claim_locked(&self,state:&mut State) -> (Token, NonNull<u8>) {
        let token = if let Some(t) = state.recycled.pop() {
            self.stats.reused.fetch_add(1, Ordering::Relaxed);
            t
        } else {
            if state.unused.is_empty() {
                let region = u32::try_from(state.regions.len()).expect("too many 2 MiB regions");
                state.regions.push(Region::new());
                self.stats.regions.fetch_add(1, Ordering::Relaxed);
                state
                    .unused
                    .extend((0..REGION / PAGE).rev().map(|page| Token { region, page:page as u16,last_input:0 }));
            }
            state.unused.pop().unwrap()
        };
        let ptr = unsafe {
            NonNull::new_unchecked(
                state.regions[token.region as usize]
                    .0
                    .as_ptr()
                    .add(token.page as usize * PAGE),
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

    fn pool(&self)->Arc<Pool> {
        self.shared.clone().unwrap_or_else(|| {
            Arc::new(Pool {
                state: Mutex::default(),
                stats: Arc::clone(&self.stats),
            })
        })
    }
    pub fn pack<T: Copy>(&self, rows: impl IntoIterator<Item = T>) -> Pages<T> {
        let pool = self.pool();
        let cap = PAGE / std::mem::size_of::<T>();
        assert!(cap > 0 && std::mem::align_of::<T>() <= PAGE);
        let mut pages = Pages {
            pool,
            pieces: Vec::new(),
            len:0,
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
            pages.len+=1;
        }
        pages
    }
}

pub(super) struct Pages<T: Copy> {
    pool: Arc<Pool>,
    pieces: Vec<(Token, NonNull<T>, usize)>,
    len:usize,
}

pub(super) struct OwnedRows<T:Copy> {
    pages:Pages<T>,
    page:usize,
    offset:usize,
}

impl<T:Copy> IntoIterator for Pages<T> {
    type Item=T;
    type IntoIter=OwnedRows<T>;
    fn into_iter(self)->Self::IntoIter {OwnedRows{pages:self,page:0,offset:0}}
}

impl<T:Copy> Iterator for OwnedRows<T> {
    type Item=T;
    fn next(&mut self)->Option<T> {
        let (_,base,len)=self.pages.pieces.get(self.page)?;
        let result=unsafe {base.as_ptr().add(self.offset).read()};
        self.offset+=1;
        if self.offset==*len { self.page+=1;self.offset=0;}
        Some(result)
    }
}

// Each token grants exclusive ownership of one nonmoving page. A finished
// Pages only exposes shared reads; recycling requires ownership via Drop.
unsafe impl<T: Copy+Send> Send for Pages<T> {}
unsafe impl<T: Copy+Sync> Sync for Pages<T> {}

impl<T: Copy> Pages<T> {
    pub fn len(&self)->usize {self.len}
    pub fn capacity_bytes(&self)->usize {self.pieces.len()*PAGE}
    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.pieces.iter().flat_map(|(_, ptr, n)| {
            unsafe { std::slice::from_raw_parts(ptr.as_ptr(), *n) }
                .iter()
                .copied()
        })
    }
}

/// Reader-owned 4 KiB pages. Only the reader writes before finish; Rayon
/// hands each bucket to one reducer. Streaming stores are fenced before
/// publication (or unwinding) and never target unaligned memory.
// SIMD stores copy every byte, including bytes at record field boundaries.
// Implementors have no uninitialized padding; the compiler checks the size
// sums for the private record types in the parent module.
pub(super) unsafe trait PageRecord:Copy {}

#[derive(Copy,Clone,PartialEq)]
pub(super) enum InputKind { Event=1,Click=2 }

pub(super) struct InputWriter<T:PageRecord> {
    pool: Arc<Pool>,
    rows:Vec<Option<Pages<T>>>,
    spare:Vec<(Token,NonNull<u8>)>,
    staging:Vec<[MaybeUninit<T>;8]>,
    used:Vec<u8>,
    mode:super::Writes,
    streamed:bool,
    nt_bytes:usize,
    allocated:usize,
    kind:InputKind,
    event_to_click:usize,
}

impl<T:PageRecord> InputWriter<T> {
    pub fn new(arena:&Arena,parts:usize,mode:super::Writes,kind:InputKind)->Self {
        assert!(size_of::<T>()>0 && size_of::<T>()<=PAGE && align_of::<T>()<=PAGE);
        let nb=if mode==super::Writes::Direct {0} else {parts};
        Self{pool:arena.pool(),rows:(0..parts).map(|_|None).collect(),spare:Vec::new(),
            staging:vec![[MaybeUninit::uninit();8];nb],used:vec![0;nb],mode,streamed:false,nt_bytes:0,allocated:0,kind,event_to_click:0}
    }
    fn new_page(&mut self,part:usize) {
        if self.spare.is_empty() {
            let mut state=self.pool.state.lock().unwrap();
            for _ in 0..16 {self.spare.push(self.pool.claim_locked(&mut state));}
        }
        let (mut key,ptr)=self.spare.pop().unwrap();
        self.event_to_click+=usize::from(self.kind==InputKind::Click&&key.last_input==InputKind::Event as u8);
        key.last_input=self.kind as u8;
        self.allocated+=1;
        self.rows[part].get_or_insert_with(||Pages{pool:Arc::clone(&self.pool),pieces:Vec::new(),len:0})
            .pieces.push((key,ptr.cast::<T>(),0));
    }
    #[inline]
    fn destination(&mut self,part:usize)->&mut Pages<T> {
        if self.rows[part].as_ref().and_then(|p|p.pieces.last()).is_none_or(|p|p.2==PAGE/size_of::<T>()) {
            self.new_page(part);
        }
        self.rows[part].as_mut().unwrap()
    }
    #[inline]
    pub fn push(&mut self,part:usize,row:T) {
        if self.mode==super::Writes::Direct {
            let pages=self.destination(part);
            let (_,ptr,n)=pages.pieces.last_mut().unwrap();
            unsafe {ptr.as_ptr().add(*n).write(row);}
            *n+=1;
            pages.len+=1;
            return;
        }
        let used=self.used[part] as usize;
        self.staging[part][used].write(row);
        self.used[part]+=1;
        if used==7 {self.flush(part);}
    }
    fn flush(&mut self,part:usize) {
        let total=std::mem::take(&mut self.used[part]) as usize;
        if total==0 {return;}
        let src=self.staging[part].as_ptr().cast::<T>();
        let mut copied=0;
        while copied<total {
            let stream=self.mode==super::Writes::Stream;
            let pages=self.destination(part);
            let (_,ptr,n)=pages.pieces.last_mut().unwrap();
            let count=(total-copied).min(PAGE/size_of::<T>()-*n);
            let dst=unsafe {ptr.as_ptr().add(*n)};
            let source=unsafe {src.add(copied)};
            let streaming=stream && (dst as usize)%16==0 && (count*size_of::<T>())%16==0;
            #[cfg(target_arch="x86_64")]
            unsafe {
                if streaming {
                    use std::arch::x86_64::*;
                    for lane in 0..count*size_of::<T>()/16 {
                        _mm_stream_si128(dst.cast::<__m128i>().add(lane),_mm_loadu_si128(source.cast::<__m128i>().add(lane)));
                    }
                } else {std::ptr::copy_nonoverlapping(source,dst,count);}
            }
            #[cfg(not(target_arch="x86_64"))]
            unsafe { std::ptr::copy_nonoverlapping(source,dst,count); }
            *n+=count;
            pages.len+=count;
            self.streamed|=streaming;
            #[cfg(target_arch="x86_64")]
            if streaming {self.nt_bytes+=count*size_of::<T>();}
            copied+=count;
        }
    }
    pub fn finish(mut self)->Vec<Option<Pages<T>>> {
        for part in 0..self.used.len() {self.flush(part);}
        let bytes=self.rows.iter().flatten().map(|p|p.len()*size_of::<T>()).sum();
        self.pool.stats.input_bytes.fetch_add(bytes,Ordering::Relaxed);
        self.pool.stats.input_pages.fetch_add(self.allocated,Ordering::Relaxed);
        self.pool.stats.streaming_bytes.fetch_add(self.nt_bytes,Ordering::Relaxed);
        self.pool.stats.event_pages_used_for_clicks.fetch_add(self.event_to_click,Ordering::Relaxed);
        std::mem::take(&mut self.rows)
    }
}

impl<T:PageRecord> Drop for InputWriter<T> {
    fn drop(&mut self) {
        #[cfg(target_arch="x86_64")]
        if self.streamed {unsafe {std::arch::x86_64::_mm_sfence();}}
        if !self.spare.is_empty() {
            let mut state=self.pool.state.lock().unwrap();
            state.recycled.extend(self.spare.drain(..).map(|(key,_)|key));
        }
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

    unsafe impl PageRecord for u64 {}

    #[test]
    fn native_reader_modes_preserve_all_rows_across_page_and_batch_tails() {
        let arena=Arena::new(true);
        for (mode,kind) in [(super::super::Writes::Direct,InputKind::Event),(super::super::Writes::Buffered,InputKind::Click),(super::super::Writes::Stream,InputKind::Click)] {
            let mut writer=InputWriter::new(&arena,3,mode,kind);
            for i in 0..6751u64 {writer.push((i%3) as usize,i);}
            for (bucket,rows) in writer.finish().into_iter().enumerate() {
                let expected=(bucket as u64..6751).step_by(3);
                let rows=rows.unwrap();
                assert!(rows.iter().eq(expected.clone()));
                assert!(rows.into_iter().eq(expected));
            }
        }
        assert_eq!(arena.stats.input_bytes.load(Ordering::Relaxed),3*6751*8);
        #[cfg(target_arch="x86_64")]
        assert!(arena.stats.streaming_bytes.load(Ordering::Relaxed)>0);
        assert!(arena.stats.reused.load(Ordering::Relaxed)>0);
        assert!(arena.stats.event_pages_used_for_clicks.load(Ordering::Relaxed)>0);
    }

    #[test]
    fn estimated_stripes_expose_only_written_rows_and_signal_overflow() {
        let mut s=Stripe::new(4,2);
        assert!(s.push(1,10));
        assert!(s.push(3,30));
        assert!(s.push(1,11));
        assert!(!s.push(1,12));
        assert!(s.prefix(0).is_empty());
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
