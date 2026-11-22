"""Check the v5 official fixtures, dev answer, design fixtures and optional large tables."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

import duckdb

from verify_v5_event_keys import compare, record
from v5_event_key_study import LIMITS, ROOT


def setting(text):
    key, separator, value = text.partition("=")
    if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", key):
        raise argparse.ArgumentTypeError("use KEY=VALUE")
    if key in {"DATA_DIR", "OUTPUT_DIR", "NCPU", "MEM_GB"}:
        raise argparse.ArgumentTypeError(f"{key} is set by the competition harness")
    return key, value


def bind(path, target, readonly=False):
    mount = f"type=bind,source={path.resolve()},target={target}"
    return mount + (",readonly" if readonly else "")


def fixtures_in(directory):
    return sorted(p for p in directory.iterdir() if (p / "expected").is_dir())


def run(options):
    directory = options.work.resolve() / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    directory.mkdir(parents=True)
    suites = set(options.suite or ["official", "dev", "custom"])
    fixtures = []
    if "official" in suites:
        root = options.official_fixtures or options.assets / "starter/starter-duckdb/fixtures"
        official = fixtures_in(root)
        if len(official) != 36:
            raise ValueError(f"Expected 36 official fixtures at {root}, found {len(official)}")
        fixtures.extend(("official-" + p.name, p) for p in official)
    if "custom" in suites:
        generated = directory / "fixtures"
        for generator in ("tests/event-keys/make_case.py", "tests/performance-design/make_cases.py"):
            subprocess.run([sys.executable, ROOT / generator, generated], check=True)
        fixtures.extend(("design-" + p.name, p) for p in fixtures_in(generated))

    jobs = []
    for name, fixture in fixtures:
        data = fixture / "input" if (fixture / "input").is_dir() else fixture / "data"
        jobs.append((name, bind(data, "/data", True), fixture / "expected"))
    if "dev" in suites:
        answer = options.dev_answer or options.assets / "answer/dev-answer"
        if options.dev_input:
            dev_mount = bind(options.dev_input, "/data", True)
        else:
            volume = options.volume or (options.assets / "volume").read_text(encoding="ascii").strip()
            dev_mount = f"type=volume,source={volume},volume-subpath=dev,target=/data,readonly"
        jobs.append(("official-dev-1m", dev_mount, answer))
    for name, _, answer in jobs:
        for table in ("campaign_daily", "user_daily"):
            if not (answer / (table + ".csv")).is_file():
                raise FileNotFoundError(f"{name}: missing {table}.csv in {answer}")

    settings = dict(options.setting)
    digest = subprocess.check_output(
        ["docker", "image", "inspect", "--format", "{{.Id}}", options.image], text=True
    ).strip()
    metadata = dict(image=options.image, digest=digest, settings=settings, limits=LIMITS,
                    jobs=[(name, mount, str(answer.resolve())) for name, mount, answer in jobs],
                    table_pairs=[list(map(str, pair)) for pair in options.tables])
    (directory / "config.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"Verification logs: {directory}", flush=True)

    def verify_case(out, name, data_mount, expected):
        output = directory / "outputs" / name
        scratch = directory / "scratch" / name
        for dest in (output, scratch):
            dest.mkdir(parents=True)
            dest.chmod(0o777)  # The scorer runs as uid 1000 on native Linux too.
        command = ["docker", "run", "--rm", "--pull=never", "--platform", "linux/amd64",
                   "--network", "none", "--read-only", "--user", "1000:1000", *LIMITS,
                   "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "4096",
                   "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--mount", data_mount,
                   "--mount", bind(output, "/output"), "--mount", bind(scratch, "/scratch"),
                   "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output", "-e", "NCPU=8", "-e", "MEM_GB=24"]
        for key, value in settings.items():
            command.extend(["-e", f"{key}={value}"])
        command.append(digest)  # No sh, cp, or other command is needed inside the submitted image.
        print(json.dumps(command), flush=True)
        subprocess.run(command, check=True, stdout=out, stderr=subprocess.STDOUT)
        return compare(con, output, expected, True)

    results = []
    with tempfile.TemporaryDirectory() as spill, duckdb.connect(config={
        "threads": "2", "memory_limit": "3GiB", "temp_directory": spill,
        "preserve_insertion_order": "false",
    }) as con:
        for job in jobs:
            results.append(record(directory, job[0], lambda out: verify_case(out, *job)))
        for i, (left, right) in enumerate(options.tables, 1):
            results.append(record(directory, f"large-tables-{i}", lambda _: compare(con, left, right, False)))
    summary = {"passed": sum(results), "total": len(results)}
    (directory / "summary.json").write_text(json.dumps(summary) + "\n", encoding="utf-8")
    print(f"{sum(results)}/{len(results)} checks passed. Logs: {directory}", flush=True)
    return all(results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--setting", type=setting, action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--assets", type=Path, default=ROOT / ".buildathon/event-key-v5-a635381",
                        help="v5_event_key_study.py's workspace with official assets and volume name")
    parser.add_argument("--work", type=Path, default=ROOT / ".buildathon/design-verification")
    parser.add_argument("--suite", action="append", choices=["official", "dev", "custom"],
                        help="repeat to select suites; all run by default")
    parser.add_argument("--official-fixtures", type=Path, help="override the 36 official case directory")
    parser.add_argument("--dev-answer", type=Path, help="override the extracted 1M official CSV answer directory")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--dev-input", type=Path, help="host directory containing generated dev inputs")
    source.add_argument("--volume", help="Docker volume containing the generated dev subdirectory")
    parser.add_argument("--tables", type=Path, nargs=2, action="append", default=[],
                        metavar=("A", "B"), help="also compare an exported pair of large Parquet result roots")
    return 0 if run(parser.parse_args()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
