use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};
use std::thread;

use rayon::prelude::*;

type Task = Box<dyn FnOnce() + Send>;
pub type Work<R> = Box<dyn FnOnce() -> R + Send>;

struct Queue {
    tasks: Mutex<VecDeque<Task>>,
    ready: Condvar,
}

impl Queue {
    fn worker(&self, spins: usize) -> ! {
        loop {
            let mut next = None;
            for _ in 0..spins {
                if let Ok(mut tasks) = self.tasks.try_lock() {
                    next = tasks.pop_front();
                    if next.is_some() { break; }
                }
                std::hint::spin_loop();
            }
            let task = next.unwrap_or_else(|| {
                let tasks = self.tasks.lock().unwrap();
                self.ready.wait_while(tasks, |q| q.is_empty()).unwrap().pop_front().unwrap()
            });
            task();
        }
    }
}

fn pool() -> &'static Queue {
    static POOL: OnceLock<Arc<Queue>> = OnceLock::new();
    POOL.get_or_init(|| {
        let queue = Arc::new(Queue { tasks: Mutex::new(VecDeque::new()), ready: Condvar::new() });
        let threads = rayon::current_num_threads();
        let spins = number("PERF_SPIN", 0).min(100_000);
        for index in 0..threads {
            let shared = Arc::clone(&queue);
            thread::Builder::new().name(format!("stage-{index}"))
                .spawn(move || shared.worker(spins)).expect("start stage worker");
        }
        queue
    })
}

pub fn run<R: Send + 'static>(jobs: Vec<Work<R>>) -> Vec<R> {
    let grain = number("PERF_GRAIN", 1).clamp(1, jobs.len().max(1));
    if !std::env::var_os("PERF_POOL").is_some_and(|v| v == "spin") {
        return jobs.into_par_iter().with_min_len(grain).map(|work| work()).collect();
    }
    let pool = pool();
    let mut results = Vec::with_capacity(jobs.len());
    let mut tasks = pool.tasks.lock().unwrap();
    let mut jobs = jobs.into_iter();
    while jobs.len() > 0 {
        let group: Vec<_> = jobs.by_ref().take(grain).collect();
        let (tx, rx) = mpsc::sync_channel(1);
        tasks.push_back(Box::new(move || {
            let value = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                group.into_iter().map(|job| job()).collect::<Vec<_>>()
            }));
            let _ = tx.send(value);
        }));
        results.push(rx);
    }
    drop(tasks);
    pool.ready.notify_all();
    results.into_iter().map(|rx| match rx.recv().expect("stage worker exited") {
        Ok(result) => result,
        Err(error) => std::panic::resume_unwind(error),
    }).flatten().collect()
}

pub fn number(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

pub fn flag(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| value == "1")
}

/// Advise only pages entirely owned by this allocation. The kernel may
/// decline promotion depending on the host's transparent huge page policy.
pub fn huge_pages<T>(data: &[T]) {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    if !*ENABLED.get_or_init(|| flag("PERF_THP")) { return; }
    #[cfg(target_os = "linux")]
    {
        const HUGE: usize = 2 << 20;
        let lo = data.as_ptr() as usize;
        let first = lo.next_multiple_of(HUGE);
        let end = (lo + std::mem::size_of_val(data)) & !(HUGE - 1);
        if end > first {
            // madvise changes only kernel paging hints; no Rust memory is accessed.
            unsafe { libc::madvise(first as *mut libc::c_void, end-first, libc::MADV_HUGEPAGE); }
        }
    }
}
