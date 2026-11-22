"""Add event-key counters or an isolated dedup timer to an exported Rust source tree."""

import argparse
from hashlib import sha256
from pathlib import Path


BASELINE_RECORDS = "d9de10d7a4f784fcd01c6e98b53d0900e3cbbb77f2fe192eee582ca7d30a2f1c"
FALLBACK = "    EXTERNAL_ID | u64::from(fallback.intern(text(id)))"
SCAN = "            chunk.scan_parsed(|[id, user, ts, ingested, kind, product, price]| {"
LOSSES = "    let losses: Vec<_> = (0..SHARDS)"
AFTER_LOSSES = "        .collect();\n    let mut people: Vec<_> = candidates"


def replace_once(source, old, new):
    if source.count(old) != 1:
        raise ValueError(f"expected exactly one source anchor: {old!r}")
    return source.replace(old, new)


COUNT_HELPERS = """\
// Injected in the counter-only build. Each worker merges once per input chunk.
std::thread_local! {
    static EVENT_INTERN_CALLS: std::cell::Cell<[u64; 3]> =
        const { std::cell::Cell::new([0; 3]) };
}

fn diagnostic_strict_event_id(id: &[u8]) -> bool {
    id.len() == 33 && id[0] == b'e'
        && id[1..].iter().all(|&b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

"""


def add_counts(source):
    source = replace_once(source, "fn id_for(id: &[u8]", COUNT_HELPERS + "fn id_for(id: &[u8]")
    source = replace_once(source, FALLBACK, """\
    if prefix == b'e' {
        EVENT_INTERN_CALLS.with(|counter| {
            let mut counts = counter.get();
            counts[0] += 1;
            if diagnostic_strict_event_id(id) {
                counts[1] += 1;
            } else if id.len() > 9 {
                counts[2] += 1;
            }
            counter.set(counts);
        });
    }
""" + FALLBACK)
    start = "    let parsed: Vec<_> = chunks(&data.join(\"events\"))?"
    source = replace_once(source, start, "    let event_totals = std::sync::Mutex::new([0_u64; 4]);\n" + start)
    source = replace_once(source, SCAN, """\
            let mut strict_seen = 0u64;
            EVENT_INTERN_CALLS.with(|counter| counter.set([0; 3]));
""" + SCAN + "\n                strict_seen += u64::from(diagnostic_strict_event_id(id));")
    finish = "            Ok::<_, anyhow::Error>((id_rows, people_rows))"
    source = replace_once(source, finish, """\
            let calls = EVENT_INTERN_CALLS.with(|counter| counter.replace([0; 3]));
            let mut totals = event_totals.lock().unwrap();
            totals[0] += strict_seen;
            for (total, count) in totals[1..].iter_mut().zip(calls) {
                *total += count;
            }
""" + finish)
    result = "    Ok(Candidates {"
    return replace_once(source, result, """\
    let [seen, all_calls, strict_calls, other_long] = event_totals.into_inner().unwrap();
    eprintln!("profile {}", serde_json::json!({
        "stage": "event_id_dictionary",
        "strict_seen": seen,
        "all_event_intern_calls": all_calls,
        "strict_intern_calls": strict_calls,
        "other_long_intern_calls": other_long,
    }));
""" + result)


def add_timer(source):
    # 'losses' finishes all ID-shard work before the next loop touches person rows.
    source = replace_once(source, LOSSES, "    let dedup_started = std::time::Instant::now();\n" + LOSSES)
    return replace_once(source, AFTER_LOSSES, """\
        .collect();
    let dedup_seconds = dedup_started.elapsed().as_secs_f64();
    eprintln!("profile {}", serde_json::json!({
        "stage": "event_id_dedup", "elapsed_s": dedup_seconds,
    }));
    let mut people: Vec<_> = candidates""")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context", type=Path, help="directory extracted from git archive")
    parser.add_argument("--variant", required=True, choices=("A", "B"))
    parser.add_argument("--mode", required=True, choices=("count", "timing"))
    args = parser.parse_args()

    root = args.context.resolve()
    if (root / ".git").exists() or root == Path(__file__).resolve().parents[1]:
        parser.error("provide an exported build context")
    path = root / "rust/src/records.rs"
    source = path.read_text(encoding="utf-8")
    if '"event_id_dictionary"' in source or '"event_id_dedup"' in source:
        parser.error("the source already contains diagnostics")
    if (sha256(source.encode()).hexdigest() == BASELINE_RECORDS) != (args.variant == "A"):
        parser.error("variant does not match the baseline records.rs fingerprint")
    candidate_anchors = ("fn hex_event_id(", "EventId::parse(", "id_for(id, b'e',")
    if args.variant == "B" and not all(anchor in source for anchor in candidate_anchors):
        parser.error("candidate long-key or shared fallback path has changed")
    if (source.count(FALLBACK) != 1 or source.count(".intern(") != 1
            or "pub fn dedup_and_sessionize(" not in source):
        parser.error("the dictionary or dedup path has changed")
    updated = (add_counts if args.mode == "count" else add_timer)(source)
    path.write_bytes(updated.encode())


if __name__ == "__main__":
    main()
