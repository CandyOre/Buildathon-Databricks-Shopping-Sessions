use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
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
    system: SystemSample,
}

#[derive(Default)]
struct SystemSample {
    counts: BTreeMap<String, u64>,
    threads: BTreeMap<String, u64>,
    running_ns: BTreeMap<String, u64>,
    runnable_wait_ns: BTreeMap<String, u64>,
    last_cpu: BTreeMap<String, u64>,
}

fn ticks(line: &str) -> Option<(u64, u64, u64, u64)> {
    // comm is parenthesized and may itself contain spaces or parentheses.
    let mut rest = line.rsplit_once(") ")?.1.split_whitespace().skip(7);
    let minor = rest.next()?.parse().ok()?;
    let major = rest.nth(1)?.parse().ok()?;
    let user = rest.nth(1)?.parse().ok()?;
    let system = rest.next()?.parse().ok()?;
    Some((minor, major, user, system))
}

impl SystemSample {
    fn read() -> Self {
        let mut out = Self::default();
        if let Ok(io) = fs::read_to_string("/proc/self/io") {
            for (key, value) in io.lines().filter_map(|line| line.split_once(": ")) {
                if let Ok(v) = value.parse() {
                    out.counts.insert(key.to_owned(), v);
                }
            }
        }
        if let Some((minor, major, user, kernel)) = fs::read_to_string("/proc/self/stat")
            .ok()
            .and_then(|s| ticks(&s))
        {
            for (key, value) in [
                ("minor_faults", minor),
                ("major_faults", major),
                ("user_ticks", user),
                ("system_ticks", kernel),
            ] {
                out.counts.insert(key.to_owned(), value);
            }
        }
        if let Ok(tasks) = fs::read_dir("/proc/self/task") {
            for entry in tasks.flatten() {
                let tid = entry.file_name().to_string_lossy().into_owned();
                if let Ok(stat) = fs::read_to_string(entry.path().join("stat")) {
                    if let Some((_, _, u, s)) = ticks(&stat) {
                        out.threads.insert(tid.clone(), u + s);
                    }
                    // /proc/<pid>/stat field 39 is the last processor number.
                    if let Some(cpu) = stat
                        .rsplit_once(") ")
                        .and_then(|(_, s)| s.split_whitespace().nth(36))
                        .and_then(|s| s.parse().ok())
                    {
                        out.last_cpu.insert(tid.clone(), cpu);
                    }
                }
                if let Ok(stat) = fs::read_to_string(entry.path().join("schedstat")) {
                    let mut values = stat
                        .split_whitespace()
                        .filter_map(|s| s.parse::<u64>().ok());
                    if let (Some(run), Some(wait)) = (values.next(), values.next()) {
                        out.running_ns.insert(tid.clone(), run);
                        out.runnable_wait_ns.insert(tid, wait);
                    }
                }
            }
        }
        out
    }

    fn delta(new: &BTreeMap<String, u64>, old: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
        new.iter()
            .map(|(k, v)| (k.clone(), v.saturating_sub(*old.get(k).unwrap_or(&0))))
            .collect()
    }
}

struct HardwareCounter {
    file: Option<fs::File>,
    errno: Option<i32>,
}

impl HardwareCounter {
    #[cfg(target_os = "linux")]
    fn open(cache: u64) -> Self {
        use std::os::fd::FromRawFd;
        // PERF_ATTR_SIZE_VER0. Count user-mode read misses and inherit on the
        // Rayon workers which will be created after the profiler starts.
        #[repr(C)]
        struct Attr {
            kind: u32,
            size: u32,
            config: u64,
            period: u64,
            sample: u64,
            format: u64,
            flags: u64,
            wakeup: u32,
            bp_type: u32,
            config1: u64,
        }
        let attr = Attr {
            kind: 3,
            size: 64,
            config: cache | (1 << 16),
            period: 0,
            sample: 0,
            format: 0,
            flags: (1 << 1) | (1 << 5) | (1 << 6),
            wakeup: 0,
            bp_type: 0,
            config1: 0,
        };
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &attr as *const Attr,
                0,
                -1,
                -1,
                8,
            )
        };
        if fd < 0 {
            Self {
                file: None,
                errno: std::io::Error::last_os_error().raw_os_error(),
            }
        } else {
            Self {
                file: Some(unsafe { fs::File::from_raw_fd(fd as i32) }),
                errno: None,
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    fn open(_: u64) -> Self {
        Self {
            file: None,
            errno: None,
        }
    }
    fn value(&self) -> Option<u64> {
        let mut file = self.file.as_ref()?;
        let mut bytes = [0; 8];
        file.read_exact(&mut bytes).ok()?;
        Some(u64::from_ne_bytes(bytes))
    }
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
    hardware: Option<[HardwareCounter; 2]>,
}

impl Profiler {
    pub fn new(enabled: bool) -> Self {
        let total = Instant::now();
        let mut profile = Self {
            total,
            sample: None,
            thread: None,
            hardware: None,
        };
        if enabled {
            profile.hardware = Some([HardwareCounter::open(2), HardwareCounter::open(3)]);
            let current = status_kib("VmRSS:");
            let sample = Arc::new(Mutex::new(Sample {
                started: total,
                start_rss_kib: current,
                peak_rss_kib: current,
                stop: false,
                system: SystemSample::read(),
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
            let observation = SystemSample::read();
            let data = json!({
                "stage": stage, "elapsed_s": end.duration_since(sample.started).as_secs_f64(),
                "epoch_us": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_micros() as u64),
                "start_rss_kib": sample.start_rss_kib, "end_rss_kib": end_rss, "peak_rss_kib": sample.peak_rss_kib,
                "os_delta":SystemSample::delta(&observation.counts,&sample.system.counts),
                "thread_cpu_ticks":SystemSample::delta(&observation.threads,&sample.system.threads),
                "thread_running_ns":SystemSample::delta(&observation.running_ns,&sample.system.running_ns),
                "thread_runnable_wait_ns":SystemSample::delta(&observation.runnable_wait_ns,&sample.system.runnable_wait_ns),
                "thread_last_cpu":observation.last_cpu,
            });
            sample.system = observation;
            eprintln!("profile {data}");
            sample.started = end;
            sample.start_rss_kib = end_rss;
            sample.peak_rss_kib = end_rss;
        }
    }

    pub fn finish(&mut self) {
        if self.sample.is_some() {
            let h = self.hardware.as_ref().unwrap();
            let huge = fs::read_to_string("/proc/self/smaps_rollup")
                .ok()
                .and_then(|s| {
                    s.lines().find_map(|l| {
                        l.strip_prefix("AnonHugePages:")?
                            .split_whitespace()
                            .next()?
                            .parse::<u64>()
                            .ok()
                    })
                });
            let data = json!({
                "stage": "total", "elapsed_s": self.total.elapsed().as_secs_f64(),
                "peak_rss_kib": status_kib("VmHWM:"), "container_peak_bytes": cgroup_peak_bytes(),
                "anonymous_huge_kib":huge, "llc_read_misses":h[0].value(), "dtlb_read_misses":h[1].value(),
                "perf_errno":[h[0].errno,h[1].errno],
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
