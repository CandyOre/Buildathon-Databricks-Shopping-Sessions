mod aggregate;
mod csv;
mod input;
mod output;
mod probe;
mod profile;
mod records;
mod routed;
mod runtime;
mod wide_cache;

use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result};

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
    let (window, dimensions) = if runtime::flag("PERF_PARALLEL_SETUP") {
        enum Setup { Window(input::Window), Tables(input::Dimensions) }
        let first = data.clone();
        let second = data.clone();
        let mut jobs = runtime::run::<Result<Setup>>(vec![
            Box::new(move || input::Window::read(&first).map(Setup::Window)),
            Box::new(move || input::Dimensions::read(&second).map(Setup::Tables)),
        ]).into_iter();
        match (jobs.next().unwrap()?, jobs.next().unwrap()?) {
            (Setup::Window(w), Setup::Tables(d)) => (w, d),
            _ => unreachable!(),
        }
    } else {
        (input::Window::read(&data)?, input::Dimensions::read(&data)?)
    };
    if let Ok(mode) = env::var("PERF_PROBE") {
        probe::run(&data, window, &dimensions, &mode)?;
        profile.stage("event_prefix");
        profile.finish();
        return Ok(());
    }
    let (mut session_batches, campaigns, user_names, event_count) =
        if probe::use_routing(&data, window)? {
            profile.stage("load_dimensions");
            let rows = routed::run(&data, window, &dimensions)?;
            profile.stage("route_and_sessionize");
            rows
        } else {
            let mut users = input::Names::default();
            let aliases = input::read_aliases(&data, &mut users)?;
            profile.stage("load_dimensions_and_links");
            let (sessions, count, clicks, ids) = if runtime::flag("PERF_OVERLAP_STREAMS") {
                let (events, clicks) = rayon::join(
                    || -> Result<_> {
                        let candidates = records::read_events(&data, window, &dimensions, &users, &aliases)?;
                        Ok(records::dedup_and_sessionize(candidates, users.person_slots_per_shard()))
                    },
                    || records::read_clicks(&data, window, &dimensions, &users, &aliases),
                );
                let (sessions, count) = events?;
                let (clicks, ids) = clicks?;
                profile.stage("overlap_events_and_clicks");
                (sessions, count, clicks, ids)
            } else {
                let candidates = records::read_events(&data, window, &dimensions, &users, &aliases)?;
                profile.stage("parse_events");
                let (sessions, count) = records::dedup_and_sessionize(candidates, users.person_slots_per_shard());
                profile.stage("dedup_and_sessionize");
                let (clicks, ids) = records::read_clicks(&data, window, &dimensions, &users, &aliases)?;
                profile.stage("parse_and_sort_clicks");
                (sessions, count, clicks, ids)
            };
            drop(aliases);
            let names = users.into_people();
            (sessions, routed::Campaigns::Pending(clicks,ids), names, count)
        };

    let dictionaries = output::prepare_names(&session_batches, &user_names)?;
    let campaign_daily = match campaigns {
        routed::Campaigns::Ready(days) => {
            profile.stage("prepare_output_after_fused_attribution");
            days
        }
        routed::Campaigns::Pending(clicks,ids) => {
            let days=aggregate::campaign_daily(&mut session_batches,clicks,ids,dimensions.campaigns.len(),window);
            profile.stage("attribution_and_assists");
            days
        }
    };
    output::write(
        &out,
        session_batches,
        campaign_daily,
        user_names,
        dimensions,
        dictionaries,
    )?;
    profile.stage("write_parquet");
    csv::print_work();
    profile.finish();
    eprintln!("Wrote user_daily and campaign_daily from {event_count} valid, deduplicated events");
    if runtime::flag("PERF_EXIT_AFTER_CLOSE") {
        std::process::exit(0);
    }
    Ok(())
}
