"""Check v5 event-key A/B benchmark logs and summarize whole-container measurements."""

import argparse
import json
import math
from pathlib import Path
from statistics import median

from v5_event_key_study import CPUSET


CASES = ("s83-20m", "s37-20m", "s83-60m")
ORDER = {"times": "abbaab", "count": "ab", "timing": "abbaab"}
COUNTERS = ("strict_seen", "strict_intern_calls", "all_event_intern_calls", "other_long_intern_calls")
STAGES = ("parse_events", "event_id_dedup", "dedup_and_sessionize")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def measure(rows, field, stage=None, scale=1):
    values = []
    for row in rows:
        record = row["stages"][stage] if stage else row
        number = record[field]
        require(isinstance(number, (float, int)) and math.isfinite(number) and number > 0,
                f"invalid {stage or 'container'}.{field}: {number!r}")
        values.append(number / scale)
    return values


def median_and_max(values):
    return {"median": median(values), "max": max(values)}


def summarize(work):
    volume = None
    images = {}
    results = {}
    for case in CASES:
        pages = None
        runs = {}
        for mode, order in ORDER.items():
            path = work / f"{mode}-{case}.jsonl"
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            require(len(rows) == len(order), f"{path}: expected {len(order)} records, found {len(rows)}")
            for index, (row, role) in enumerate(zip(rows, order), 1):
                ctx = f"{path.name}:{index}"
                if volume is None:
                    volume = row["volume"]
                require(row["volume"] == volume and row["dataset"] == case and row["cpuset"] == CPUSET,
                        f"{ctx}: volume, dataset or CPU set changed")
                if pages is None:
                    pages = row["input_page_bytes"]
                require(row["cache"] == "warm" and pages > 0 and
                        row["resident_input_bytes"] == row["input_page_bytes"] == pages,
                        f"{ctx}: expected identical fully resident input pages")
                tag = role if mode == "times" else f"{role}-{mode}"
                require(row["image"] == f"{volume}:{tag}", f"{ctx}: expected {volume}:{tag}")
                image_id = row["image_id"]
                require(isinstance(image_id, str) and image_id.startswith("sha256:") and
                        len(image_id) == 71 and all(c in "0123456789abcdef" for c in image_id[7:]),
                        f"{ctx}: invalid image ID")
                require(images.setdefault(tag, image_id) == image_id, f"{ctx}: image changed for {tag}")
                require(row["profile"] is (mode != "times"), f"{ctx}: incorrect profile flag")
                stages = row["stages"]
                require((mode != "times") == bool(stages), f"{ctx}: unexpected profiler data")
                row["stages"] = {s["stage"]: s for s in stages}
                require(len(row["stages"]) == len(stages), f"{ctx}: duplicate profiler stage")
                runs.setdefault((mode, role), []).append(row)

        output = {"input_page_bytes": pages}
        for role in "ab":
            seconds = measure(runs["times", role], "seconds")
            counts = runs["count", role][0]["stages"]["event_id_dictionary"]
            require(all(type(counts[key]) is int and counts[key] >= 0 for key in COUNTERS),
                    f"{case} {role}: invalid dictionary counters")
            require(counts["strict_intern_calls"] <= counts["strict_seen"] and
                    counts["strict_intern_calls"] + counts["other_long_intern_calls"] <= counts["all_event_intern_calls"],
                    f"{case} {role}: inconsistent dictionary counters")
            times = runs["timing", role]
            rss = measure(times, "peak_rss_kib", "total", scale=1024)
            cgroup = measure(times, "container_peak_bytes", "total", scale=1024**2)
            output[role.upper()] = {
                "container_seconds": seconds,
                "median_container_seconds": median(seconds),
                "dictionary": {key: counts[key] for key in COUNTERS},
                "median_stage_seconds": {stage: median(measure(times, "elapsed_s", stage)) for stage in STAGES},
                "process_peak_rss_mib": median_and_max(rss),
                "cgroup_peak_mib": median_and_max(cgroup),
            }
        require(output["A"]["dictionary"]["strict_seen"] == output["B"]["dictionary"]["strict_seen"],
                f"{case}: candidate and baseline observed different strict key counts")
        a = output["A"]["median_container_seconds"]
        b = output["B"]["median_container_seconds"]
        output.update(saved_seconds=a-b, saved_percent=100*(a-b)/a, speedup=a/b)
        results[case] = output
    require(len(set(images.values())) == len(images), "different A/B or diagnostic tags have identical images")
    return {"volume": volume, "cpuset": CPUSET, "image_ids": images, "datasets": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        result = summarize(args.work)
    except (ValueError, KeyError, OSError) as error:
        parser.exit(1, f"{parser.prog}: {error}\n")
    report = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if args.output:
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report, end="")


if __name__ == "__main__":
    main()
