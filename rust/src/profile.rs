use std::fs;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde_json::json;

fn status_kib(field: &str) -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix(field)?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

fn cgroup_peak_bytes() -> Option<u64> {
    fs::read_to_string("/sys/fs/cgroup/memory.peak")
        .ok()?
        .trim()
        .parse()
        .ok()
}

struct Sample {
    started: Instant,
    start_rss_kib: Option<u64>,
    peak_rss_kib: Option<u64>,
    stop: bool,
}

impl Sample {
    fn observe(&mut self) -> Option<u64> {
        let rss = status_kib("VmRSS:");
        self.peak_rss_kib = self.peak_rss_kib.max(rss);
        rss
    }
}

pub struct Profiler {
    total: Instant,
    sample: Option<Arc<Mutex<Sample>>>,
    thread: Option<JoinHandle<()>>,
}

impl Profiler {
    pub fn new(enabled: bool) -> Self {
        let total = Instant::now();
        let mut profile = Self {
            total,
            sample: None,
            thread: None,
        };
        if enabled {
            let current = status_kib("VmRSS:");
            let sample = Arc::new(Mutex::new(Sample {
                started: total,
                start_rss_kib: current,
                peak_rss_kib: current,
                stop: false,
            }));
            let sampler = Arc::clone(&sample);
            profile.thread = Some(thread::spawn(move || {
                loop {
                    thread::park_timeout(Duration::from_millis(5));
                    let mut value = sampler.lock().unwrap();
                    if value.stop {
                        break;
                    }
                    value.observe();
                }
            }));
            profile.sample = Some(sample);
        }
        profile
    }

    pub fn stage(&mut self, stage: &str) {
        if let Some(sample) = &self.sample {
            let mut sample = sample.lock().unwrap();
            let end = Instant::now();
            let end_rss = sample.observe();
            let data = json!({
                "stage": stage, "elapsed_s": end.duration_since(sample.started).as_secs_f64(),
                "start_rss_kib": sample.start_rss_kib, "end_rss_kib": end_rss, "peak_rss_kib": sample.peak_rss_kib,
            });
            eprintln!("profile {data}");
            sample.started = end;
            sample.start_rss_kib = end_rss;
            sample.peak_rss_kib = end_rss;
        }
    }

    pub fn finish(&mut self) {
        if self.sample.is_some() {
            let data = json!({
                "stage": "total", "elapsed_s": self.total.elapsed().as_secs_f64(),
                "peak_rss_kib": status_kib("VmHWM:"), "container_peak_bytes": cgroup_peak_bytes(),
            });
            eprintln!("profile {data}");
            self.stop();
        }
    }

    fn stop(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.sample.as_ref().unwrap().lock().unwrap().stop = true;
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

impl Drop for Profiler {
    fn drop(&mut self) {
        self.stop();
    }
}
