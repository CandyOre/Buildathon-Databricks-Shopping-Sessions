use std::cell::Cell;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};
use std::thread;

use rayon::prelude::*;

type Task = Box<dyn FnOnce() + Send>;
struct Queued {
    scope_id: usize,
    work: Task,
}
pub type Work<R> = Box<dyn FnOnce() -> R + Send>;
type ScopedWork<'a, R> = Box<dyn FnOnce() -> R + Send + 'a>;

thread_local! {
    static SLOT:Cell<Option<usize>>=const {Cell::new(None)};
    static HELPING:Cell<bool>=const {Cell::new(false)};
}

pub fn worker_slot() -> Option<usize> {
    SLOT.get().or_else(rayon::current_thread_index)
}

fn stages() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("PERF_POOL").is_some_and(|v| v == "stages"))
}

struct Queue {
    tasks: Mutex<VecDeque<Queued>>,
    ready: Condvar,
    workers: usize,
    completed: std::sync::atomic::AtomicUsize,
}

impl Queue {
    fn worker(&self, index: usize, spins: usize) -> ! {
        SLOT.set(Some(index));
        loop {
            let mut next = None;
            for _ in 0..spins {
                if let Ok(mut tasks) = self.tasks.try_lock() {
                    next = tasks.pop_front();
                    if next.is_some() {
                        break;
                    }
                }
                std::hint::spin_loop();
            }
            let task = next.unwrap_or_else(|| {
                let tasks = self.tasks.lock().unwrap();
                self.ready
                    .wait_while(tasks, |q| q.is_empty())
                    .unwrap()
                    .pop_front()
                    .unwrap()
            });
            let _ = catch_unwind(AssertUnwindSafe(task.work));
            self.completed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

fn pool() -> &'static Queue {
    static POOL: OnceLock<Arc<Queue>> = OnceLock::new();
    POOL.get_or_init(|| {
        let threads = rayon::current_num_threads();
        let queue = Arc::new(Queue {
            tasks: Mutex::new(VecDeque::new()),
            ready: Condvar::new(),
            workers: threads,
            completed: Default::default(),
        });
        let spins = number("PERF_SPIN", 0).min(100_000);
        for index in 0..threads {
            let shared = Arc::clone(&queue);
            thread::Builder::new()
                .name(format!("stage-{index}"))
                .spawn(move || shared.worker(index, spins))
                .expect("start stage worker");
        }
        queue
    })
}

pub fn worker_slots() -> usize {
    if stages() {
        pool().workers
    } else {
        rayon::current_num_threads()
    }
}

pub fn run<R: Send + 'static>(jobs: Vec<Work<R>>) -> Vec<R> {
    let grain = number("PERF_GRAIN", 1).clamp(1, jobs.len().max(1));
    if !stages() && !std::env::var_os("PERF_POOL").is_some_and(|v| v == "spin") {
        return jobs
            .into_par_iter()
            .with_min_len(grain)
            .map(|work| work())
            .collect();
    }
    map_on_pool(jobs, grain, || (), |_, work| work())
}

#[derive(Default)]
struct Countdown {
    remaining: Mutex<usize>,
    done: Condvar,
}

struct Ticket(Arc<Countdown>);
impl Drop for Ticket {
    fn drop(&mut self) {
        let mut count = self.0.remaining.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            self.0.done.notify_all();
        }
    }
}

struct Scope<'p> {
    queue: &'p Queue,
    countdown: Arc<Countdown>,
}
impl Scope<'_> {
    fn ticket(&self) -> Ticket {
        *self.countdown.remaining.lock().unwrap() += 1;
        Ticket(Arc::clone(&self.countdown))
    }
    fn wait(&self) {
        self.queue.ready.notify_all();
        let mut completed = self
            .queue
            .completed
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut last_progress = std::time::Instant::now();
        loop {
            let state = &self.countdown;
            let count = state.remaining.lock().unwrap();
            if *count == 0 {
                return;
            }
            if rayon::current_thread_index().is_none() {
                drop(state.done.wait_while(count, |n| *n != 0).unwrap());
                return;
            }
            // An operation on our worker may itself use a Rayon parallel
            // sort. Hand the waiting Rayon worker back to its own scheduler.
            drop(count);
            if matches!(rayon::yield_now(), Some(rayon::Yield::Executed)) {
                continue;
            }
            let now = self
                .queue
                .completed
                .load(std::sync::atomic::Ordering::Relaxed);
            if now != completed {
                completed = now;
                last_progress = std::time::Instant::now();
            } else if last_progress.elapsed() >= std::time::Duration::from_millis(50) {
                // A Rayon callback can itself submit a stage while every
                // stage worker waits for nested Rayon work. Break that cycle
                // after a prolonged lack of progress from the dedicated pool.
                let scope_id = Arc::as_ptr(&self.countdown) as usize;
                let task = {
                    let mut queued = self.queue.tasks.lock().unwrap();
                    queued
                        .iter()
                        .position(|t| t.scope_id == scope_id)
                        .and_then(|i| queued.remove(i))
                };
                if let Some(task) = task {
                    let was_helping = HELPING.replace(true);
                    let _ = catch_unwind(AssertUnwindSafe(task.work));
                    HELPING.set(was_helping);
                    self.queue
                        .completed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
            }
            let count = state.remaining.lock().unwrap();
            if *count != 0 {
                drop(
                    state
                        .done
                        .wait_timeout(count, std::time::Duration::from_micros(100))
                        .unwrap(),
                );
            }
        }
    }
}
impl Drop for Scope<'_> {
    fn drop(&mut self) {
        self.wait();
    }
}

fn scoped_jobs<'a, R: Send + 'a>(jobs: Vec<ScopedWork<'a, R>>) -> Vec<R> {
    if SLOT.get().is_some() || HELPING.get() {
        return jobs.into_iter().map(|job| job()).collect();
    }
    let scope = Scope {
        queue: pool(),
        countdown: Arc::default(),
    };
    // Declared after scope so any jobs not published yet drop their ticket
    // before the scope destructor waits, even if queue submission unwinds.
    let mut ready = Vec::with_capacity(jobs.len());
    let mut replies = Vec::with_capacity(jobs.len());
    for job in jobs {
        let (send, receive) = mpsc::sync_channel(1);
        let ticket = scope.ticket();
        let task: Box<dyn FnOnce() + Send + 'a> = Box::new(move || {
            let completion = ticket;
            let result = catch_unwind(AssertUnwindSafe(job));
            let _ = send.send(result);
            drop(send);
            drop(completion);
        });
        ready.push(task);
        replies.push(receive);
    }
    {
        let mut queue = scope.queue.tasks.lock().unwrap();
        queue.reserve(ready.len());
        for job in ready {
            // SAFETY: only the queue sees the widened lifetime. The Scope
            // waits for every job (including panicking jobs) to finish using
            // its captures before caller-owned borrows or result channels end.
            let job: Task =
                unsafe { std::mem::transmute::<Box<dyn FnOnce() + Send + 'a>, Task>(job) };
            queue.push_back(Queued {
                scope_id: Arc::as_ptr(&scope.countdown) as usize,
                work: job,
            });
        }
    }
    scope.wait();
    replies
        .into_iter()
        .map(|r| {
            r.recv()
                .expect("stage worker result")
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        })
        .collect()
}

fn map_on_pool<I, R, S, Init, F>(items: Vec<I>, grain: usize, init: Init, task: F) -> Vec<R>
where
    I: Send,
    R: Send,
    Init: Fn() -> S + Sync,
    F: Fn(&mut S, I) -> R + Sync,
{
    if items.is_empty() {
        return Vec::new();
    }
    let groups = items.len().div_ceil(grain);
    let mut jobs: Vec<ScopedWork<'_, Vec<R>>> = Vec::with_capacity(groups);
    let (init, task) = (&init, &task);
    let mut items = items.into_iter();
    for _ in 0..groups {
        let group: Vec<_> = items.by_ref().take(grain).collect();
        jobs.push(Box::new(move || {
            let mut scratch = init();
            group.into_iter().map(|i| task(&mut scratch, i)).collect()
        }));
    }
    scoped_jobs(jobs).into_iter().flatten().collect()
}

pub fn stage_map_init<I, R, S, Init, F>(
    phase: &str,
    items: Vec<I>,
    min_grain: usize,
    init: Init,
    task: F,
) -> Vec<R>
where
    I: Send,
    R: Send,
    Init: Fn() -> S + Sync + Send,
    F: Fn(&mut S, I) -> R + Sync + Send,
{
    if !stages() {
        return items
            .into_par_iter()
            .with_min_len(min_grain)
            .map_init(init, task)
            .collect();
    }
    let balanced = items.len().div_ceil(pool().workers * 4).max(min_grain);
    let grain = number("PERF_GRAIN", balanced).clamp(1, items.len().max(1));
    if flag("PROFILE") {
        use std::sync::atomic::{AtomicU64, Ordering};
        let slots = AtomicU64::new(0);
        let count = items.len();
        let start = std::time::Instant::now();
        let result = map_on_pool(items, grain, init, |state, item| {
            if let Some(i) = SLOT.get() {
                if i < 64 {
                    slots.fetch_or(1 << i, Ordering::Relaxed);
                }
            }
            task(state, item)
        });
        eprintln!(
            "profile {}",
            serde_json::json!({"stage":"stage_workers","phase":phase,"items":count,
            "grain":grain,"workers":slots.load(Ordering::Relaxed).count_ones(),"elapsed_s":start.elapsed().as_secs_f64()})
        );
        return result;
    }
    map_on_pool(items, grain, init, task)
}

pub fn stage_map<I: Send, R: Send>(
    phase: &str,
    items: Vec<I>,
    task: impl Fn(I) -> R + Sync + Send,
) -> Vec<R> {
    stage_map_init(phase, items, 1, || (), |_, item| task(item))
}

pub fn number(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

pub fn flag(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| value == "1")
}

#[cfg(test)]
mod scoped_pool_tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn borrowed_ranges_keep_input_order_and_propagate_item_errors() {
        let mut numbers = [1usize, 2, 3, 4, 5, 6];
        let input = numbers.iter_mut().enumerate().collect();
        let got = map_on_pool(
            input,
            1,
            || (),
            |_, (i, n): (usize, &mut usize)| {
                *n *= 2;
                if i == 2 { Err(i) } else { Ok(*n) }
            },
        );
        assert_eq!(got, [Ok(2), Ok(4), Err(2), Ok(8), Ok(10), Ok(12)]);
        assert_eq!(numbers, [2, 4, 6, 8, 10, 12]);
    }

    #[test]
    fn persistent_workers_run_in_parallel_and_nested_stages_finish_inline() {
        let n = pool().workers.min(2);
        let gate = Barrier::new(n);
        let result = map_on_pool(
            (0..n).collect(),
            1,
            || (),
            |_, i| {
                gate.wait();
                let owner = SLOT.get().unwrap();
                let nested = map_on_pool(
                    vec![i, 2 * i],
                    1,
                    || (),
                    |_, j| (SLOT.get().unwrap(), j + 1),
                );
                (owner, nested)
            },
        );
        for (i, (slot, nested)) in result.iter().enumerate() {
            assert_eq!(nested, &[(*slot, i + 1), (*slot, 2 * i + 1)]);
        }
        if n > 1 {
            assert_ne!(result[0].0, result[1].0);
        }
    }

    #[test]
    fn panic_reaches_caller_only_after_other_borrowers_finish() {
        let finished = AtomicUsize::new(0);
        let result = catch_unwind(AssertUnwindSafe(|| {
            map_on_pool(
                (0..16).collect(),
                1,
                || (),
                |_, i| {
                    if i == 0 {
                        panic!("scoped failure");
                    }
                    finished.fetch_add(1, Ordering::SeqCst);
                },
            )
        }));
        assert!(result.is_err());
        assert_eq!(finished.load(Ordering::SeqCst), 15);
        assert_eq!(map_on_pool(vec![7], 1, || (), |_, n| n + 1), [8]);
    }

    #[test]
    fn nested_rayon_and_stage_callbacks_can_complete_each_other() {
        let values = map_on_pool(
            vec![3, 5],
            1,
            || (),
            |_, i| {
                let (a, b) = rayon::join(
                    || map_on_pool(vec![i], 1, || (), |_, v| v * 2)[0],
                    || map_on_pool(vec![i], 1, || (), |_, v| v + 1)[0],
                );
                a + b
            },
        );
        assert_eq!(values, [10, 16]);
    }
}

/// Advise only pages entirely owned by this allocation. The kernel may
/// decline promotion depending on the host's transparent huge page policy.
pub fn huge_pages<T>(data: &[T]) {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    if !*ENABLED.get_or_init(|| flag("PERF_THP")) {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        const HUGE: usize = 2 << 20;
        let lo = data.as_ptr() as usize;
        let first = lo.next_multiple_of(HUGE);
        let end = (lo + std::mem::size_of_val(data)) & !(HUGE - 1);
        if end > first {
            // madvise changes only kernel paging hints; no Rust memory is accessed.
            unsafe {
                libc::madvise(first as *mut libc::c_void, end - first, libc::MADV_HUGEPAGE);
            }
        }
    }
}
