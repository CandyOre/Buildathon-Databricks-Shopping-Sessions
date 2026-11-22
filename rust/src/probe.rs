use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, bail};
use rayon::prelude::*;

use crate::csv;
use crate::input::{Calendar, Dimensions, Window, hex_id};

// Sampling only selects an implementation. All rows, including the sampled
// ones, pass through the normal exact parser in the selected implementation.
pub fn use_routing(data: &Path, window: Window) -> Result<bool> {
    match std::env::var("PERF_ROUTE").as_deref() {
        Ok("1") => return Ok(true),
        Ok("auto") => (),
        _ => return Ok(false),
    }
    let start = Instant::now();
    let mut lines = 0usize;
    let mut regular = 0usize;
    let mut bytes = 0;
    let mut sample = Vec::with_capacity(64 << 10);
    for path in csv::parts(&data.join("events"))?.into_iter().take(4) {
        sample.clear();
        File::open(path)?.take(64 << 10).read_to_end(&mut sample)?;
        bytes += sample.len();
        for line in sample.split_inclusive(|&b| b == b'\n').skip(1) {
            let Some(line) = line.strip_suffix(b"\n") else {
                continue;
            };
            lines += 1;
            if let Some(f) = csv::fields::<7>(line) {
                let key =
                    |s: &[u8], p| s.first() == Some(&p) && (s.len() <= 9 || hex_id(s, p).is_some());
                regular += usize::from(key(f[0], b'e') && key(f[1], b'u'));
            }
        }
    }
    let short_window = window.end.saturating_sub(window.start) <= i64::from(i32::MAX);
    let routed = short_window && lines >= 64 && regular * 100 >= lines * 95;
    if crate::runtime::flag("PROFILE") {
        eprintln!(
            "profile {}",
            serde_json::json!({"stage":"route_sample", "bytes":bytes,
            "lines":lines,"regular":regular,"short_window":short_window,"routed":routed,
            "elapsed_s":start.elapsed().as_secs_f64()})
        );
    }
    Ok(routed)
}

#[derive(Default)]
struct Prefix {
    bytes: usize,
    rows: usize,
    checksum: u64,
    buckets: Vec<Vec<[u64; 3]>>,
}

fn word(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0u64, |h, &b| h.rotate_left(7) ^ u64::from(b))
}

pub fn run(data: &Path, window: Window, dim: &Dimensions, mode: &str) -> Result<()> {
    if !matches!(mode, "read" | "lines" | "fields" | "decode" | "bucket") {
        bail!("unknown PERF_PROBE mode {mode}");
    }
    let chunks = csv::chunks(&data.join("events"))?;
    let calendar = Calendar::new(window);
    let start = Instant::now();
    let mut parts = chunks
        .par_iter()
        .map(|chunk| -> Result<Prefix> {
            let mut result = Prefix {
                bytes: chunk.byte_len(),
                ..Default::default()
            };
            match mode {
                "read" => result.checksum = chunk.probe_read()?,
                "lines" => chunk.scan(|row| {
                    result.rows += 1;
                    result.checksum += row.len() as u64;
                })?,
                "fields" => chunk.scan_parsed(|f: [&[u8]; 7]| {
                    result.rows += 1;
                    result.checksum += f.iter().map(|field| field.len() as u64).sum::<u64>();
                })?,
                _ => {
                    let store = mode == "bucket";
                    if store {
                        result.buckets.resize_with(128, Vec::new);
                    }
                    chunk.scan_events(&calendar, window, dim, |id, user, ingest, payload| {
                        let [hi, lo] = hex_id(id, b'e').unwrap_or_else(|| [0, word(id)]);
                        let value = payload.map_or(0, |(t, purchase)| {
                            (t as u64)
                                ^ purchase
                                    .map_or(0, |p| u64::from(p.category) << 32 | u64::from(p.cents))
                        }) ^ ingest as u64
                            ^ word(user);
                        let digest = hi.rotate_left(29) ^ lo ^ value;
                        result.rows += 1;
                        if store {
                            result.buckets[digest as usize & 127].push([hi, lo, value]);
                        } else {
                            result.checksum = result.checksum.wrapping_add(digest);
                        }
                    })?;
                }
            }
            Ok(result)
        })
        .collect::<Result<Vec<_>>>()?;
    // Consume complete stored tuples. decode and bucket must give the same sum.
    let count: usize = parts.iter().map(|p| p.rows).sum();
    let bytes: usize = parts.iter().map(|p| p.bytes).sum();
    let sum = parts
        .par_iter_mut()
        .map(|part| {
            part.buckets
                .iter()
                .flatten()
                .fold(part.checksum, |s, &[hi, lo, v]| {
                    s.wrapping_add(hi.rotate_left(29) ^ lo ^ v)
                })
        })
        .reduce(|| 0, u64::wrapping_add);
    eprintln!(
        "profile {}",
        serde_json::json!({"stage":"prefix","mode":mode,"bytes":bytes,
        "rows":count,"checksum":sum,"bucket_bytes":if mode=="bucket" {24*count} else {0},
        "elapsed_s":start.elapsed().as_secs_f64()})
    );
    Ok(())
}
