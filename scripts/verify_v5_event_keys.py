"""Verify both event-key images and their exported A/B study tables."""
import argparse
from contextlib import redirect_stdout
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import traceback

import duckdb

from check_event_key_tables import check
from run_fixtures import SCHEMAS
from v5_event_key_study import CASES, LIMITS, ROOT


def container_args(image, data, output, binary):
    args = [
        "docker", "run", "--rm", "--pull=never", "--platform", "linux/amd64",
        "--network", "none", "--read-only", "--user", "1000:1000", *LIMITS,
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "4096",
        "--tmpfs", "/tmp:rw,size=64m,mode=1777",
        "--mount", f"type=bind,source={data},target=/data,readonly",
        "--mount", f"type=bind,source={output},target=/output",
        "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output", "-e", "NCPU=8", "-e", "MEM_GB=24",
    ]
    if binary:
        args.extend(["--entrypoint", "/usr/local/bin/" + binary])
    return [*args, image]


def record(directory, label, action):
    log = directory / (label + ".log")
    with log.open("w", encoding="utf-8") as out, redirect_stdout(out):
        try:
            passed = action(out)
        except Exception:
            traceback.print_exc(file=out)
            passed = False
        print("MATCH" if passed else "DIFFER")
    with (directory / "results.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"check": label, "passed": passed, "log": log.name}) + "\n")
    print(f"{'ok' if passed else 'FAIL'} {label}", flush=True)
    return passed


def compare(con, left, right, csv_answer):
    results = [check(con, left, right, table, csv_answer) for table in SCHEMAS]
    return all(results)


def run_case(con, directory, label, image, case, binary=None):
    def run(out):
        data = case / "input" if (case / "input").is_dir() else case / "data"
        results = directory / "outputs" / label
        results.mkdir(parents=True)
        command = container_args(image, data, results, binary)
        print(json.dumps(command), flush=True)
        subprocess.run(command, check=True, stdout=out, stderr=subprocess.STDOUT)
        return compare(con, results, case / "expected", True)

    return record(directory, label, run)


def verify(work):
    volume = (work / "volume").read_text(encoding="ascii").strip()
    official = work / "starter/starter-duckdb/fixtures"
    cases = sorted(p for p in official.iterdir() if p.is_dir() and (p / "expected").is_dir())
    if len(cases) != 36:
        raise ValueError(f"Expected 36 official v5 fixtures in {official}, found {len(cases)}")
    images = {
        side: subprocess.check_output(
            ["docker", "image", "inspect", "--format", "{{.Id}}", f"{volume}:{side}"], text=True
        ).strip()
        for side in ("a", "b")
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    directory = work / "verification" / stamp
    directory.mkdir(parents=True)
    (directory / "images.json").write_text(json.dumps(images, indent=2) + "\n", encoding="utf-8")
    print(f"Verification logs: {directory}", flush=True)
    custom = directory / "fixtures"
    subprocess.run([sys.executable, ROOT / "tests/event-keys/make_case.py", custom], check=True)
    edge = custom / "long_event_keys"
    results = []
    with tempfile.TemporaryDirectory() as spill, duckdb.connect(config={
        "threads": "2", "memory_limit": "3GiB", "temp_directory": spill,
        "preserve_insertion_order": "false",
    }) as con:
        for side, image in images.items():
            for case in cases:
                results.append(run_case(con, directory, f"{side}-official-{case.name}", image, case))
            results.append(run_case(con, directory, f"{side}-event-keys", image, edge))
        for binary in ("portable", "large"):
            results.append(run_case(con, directory, "b-event-keys-" + binary, images["b"], edge, binary))
        for ds in CASES:
            inputs = work / "tables" / ds
            results.append(record(directory, "table-ab-" + ds, lambda _: compare(
                con, inputs / "a", inputs / "b", False
            )))
        for side in images:
            results.append(record(directory, "official-dev-" + side, lambda _: compare(
                con, work / "tables/dev" / side, work / "answer/dev-answer", True
            )))
    summary = {"passed": sum(results), "total": len(results)}
    (directory / "summary.json").write_text(json.dumps(summary) + "\n", encoding="utf-8")
    print(f"{summary['passed']}/{summary['total']} checks passed. Logs: {directory}", flush=True)
    return all(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    raise SystemExit(0 if verify(parser.parse_args().work.resolve()) else 1)
