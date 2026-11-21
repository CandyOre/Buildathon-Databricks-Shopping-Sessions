mod aggregate;
mod csv;
mod input;
mod output;
mod polars_campaign;
mod polars_sessions;
mod profile;
mod records;

use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result};
use rayon::prelude::*;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone, Copy, PartialEq)]
enum Pipeline {
    Native,
    Dedup,
    Full,
}

fn main() -> Result<()> {
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    let out = PathBuf::from(env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into()));
    let threads = match env::var("NCPU") {
        Ok(value) => value.parse().context("NCPU must be a positive integer")?,
        Err(_) => std::thread::available_parallelism().map_or(1, usize::from),
    };
    anyhow::ensure!(threads > 0, "NCPU must be a positive integer");
    let pipeline = match env::var("PIPELINE").as_deref() {
        Ok("native") => Pipeline::Native,
        Ok("dedup") => Pipeline::Dedup,
        Ok("full") | Err(env::VarError::NotPresent) => Pipeline::Full,
        _ => anyhow::bail!("PIPELINE must be native, dedup or full"),
    };
    // SAFETY: this runs at startup, before the profiler, Rayon and Polars start threads.
    unsafe { env::set_var("POLARS_MAX_THREADS", threads.to_string()) };
    let mut profile = profile::Profiler::new(env::var_os("PROFILE").is_some_and(|p| p == "1"));
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()?;
    let window = input::Window::read(&data)?;
    let dimensions = input::Dimensions::read(&data)?;
    let mut users = input::Names::default();
    let aliases = input::read_aliases(&data, &mut users)?;
    profile.stage("load_dimensions_and_links");

    let (mut events, event_count) = if pipeline == Pipeline::Native {
        let candidates = records::read_events(&data, window, &dimensions, &users, &aliases)?;
        profile.stage("parse_events");
        records::dedup_events(candidates)
    } else {
        let candidates = records::read_polars_events(&data, window, &dimensions, &users, &aliases)?;
        profile.stage("parse_events");
        let frames = records::prepare_polars_events(candidates)?;
        profile.stage("prepare_dedup_columns");
        records::dedup_polars_events(frames)?
    };
    profile.stage("dedup_events");

    let mut sessions = if pipeline == Pipeline::Full {
        polars_sessions::sessionize(events, &mut profile)?
    } else {
        events.par_iter_mut().for_each(|batch| {
            batch
                .rows
                .par_sort_unstable_by_key(|event| (event.person, event.time));
        });
        profile.stage("sort_events");
        let batches = events.into_par_iter().map(aggregate::sessionize).collect();
        profile.stage("sessionize_and_user_daily");
        batches
    };

    let (clicks, click_ids) = records::read_clicks(&data, window, &dimensions, &users, &aliases)?;
    drop(aliases);
    let user_names = users.into_names();
    profile.stage("parse_and_sort_clicks");

    let campaigns = if pipeline == Pipeline::Full {
        polars_campaign::campaign_daily(&mut sessions, clicks, click_ids, &mut profile)?
    } else {
        let totals = aggregate::campaign_daily(&mut sessions, clicks, click_ids);
        profile.stage("attribution_and_assists");
        totals
    };
    output::write(&out, sessions, campaigns, &user_names, &dimensions)?;
    profile.stage("write_parquet");
    profile.finish();
    eprintln!("Wrote user_daily and campaign_daily from {event_count} valid, deduplicated events");
    Ok(())
}
