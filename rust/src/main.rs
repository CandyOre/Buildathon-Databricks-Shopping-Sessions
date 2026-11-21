mod aggregate;
mod input;
mod output;
mod profile;
mod records;

use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result};
use rayon::prelude::*;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<()> {
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    let out = PathBuf::from(env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into()));
    let threads = match env::var("NCPU") {
        Ok(value) => value.parse().context("NCPU must be a positive integer")?,
        Err(_) => std::thread::available_parallelism().map_or(1, usize::from),
    };
    anyhow::ensure!(threads > 0, "NCPU must be a positive integer");

    let mut profile = profile::Profiler::new(env::var_os("PROFILE").is_some_and(|p| p == "1"));
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()?;
    let window = input::Window::read(&data)?;
    let dimensions = input::Dimensions::read(&data)?;
    let users = input::Names::default();
    let aliases = input::read_aliases(&data, &users)?;
    profile.stage("load_dimensions_and_links");

    let candidates = records::read_events(&data, window, &dimensions, &users, &aliases)?;
    profile.stage("parse_events");
    let (mut events, event_count) = records::dedup_events(candidates);
    profile.stage("dedup_events");
    events.par_iter_mut().for_each(|batch| {
        batch
            .rows
            .par_sort_unstable_by_key(|event| (event.person, event.time));
    });
    profile.stage("sort_events");

    let mut session_batches: Vec<_> = events.into_par_iter().map(aggregate::sessionize).collect();
    profile.stage("sessionize_and_user_daily");

    let (clicks, click_ids) = records::read_clicks(&data, window, &dimensions, &users, &aliases)?;
    drop(aliases);
    let user_names = users.into_names();
    profile.stage("parse_and_sort_clicks");

    let campaign_daily = aggregate::campaign_daily(&mut session_batches, clicks, click_ids);
    profile.stage("attribution_and_assists");
    output::write(
        &out,
        session_batches,
        campaign_daily,
        &user_names,
        &dimensions,
    )?;
    profile.stage("write_parquet");
    profile.finish();
    eprintln!("Wrote user_daily and campaign_daily from {event_count} valid, deduplicated events");
    Ok(())
}
