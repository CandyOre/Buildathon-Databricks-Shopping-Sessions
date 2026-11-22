"""Build fixed-row-count stress inputs, publish them to a fresh volume directory, and A/B test them."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import median
import subprocess
import sys
import tempfile

from design_bench import BENCH, ROOT
from verify_performance_design import setting


CASES = ("low-dup", "high-dup", "small-files", "long-lines")
START = datetime(2028, 2, 28, tzinfo=timezone.utc)
BASE = int(START.timestamp()) * 1000
EVENT = b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
CLICK = b"click_id,user_id,campaign_id,ts\n"
LINK = b"user_id_a,user_id_b\n"
HELPER = "debian:bookworm-slim"


def timestamp(time):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ")


def report(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")


def command(*args, **kwargs):
    return subprocess.run(list(map(str, args)), check=True, **kwargs)


def generate_case(root, case, args):
    output = root / case
    for group in ("events", "links", "clicks"):
        (output / group).mkdir(parents=True)
    report(output / "manifest.json", {"window_start": timestamp(START), "window_end": timestamp(START + timedelta(days=3))})
    (output / "products.csv").write_bytes(b"product_id,category\np1,books\np2,music\n")
    (output / "campaigns.csv").write_bytes(b"campaign_id,channel\n" + b"".join(
        f"c_{i},channel-{i % 3}\n".encode() for i in range(1, 9)))
    users = max(3, args.events // 20)
    def person(i):
        return f"u{i % users:07x}"
    with (output / "links/part.csv").open("wb") as links:
        links.write(LINK)
        for i in range(0, users - 1, 5):
            links.write(f"{person(i)},{person(i + 1)}\n".encode())
    with (output / "clicks/part.csv").open("wb") as clicks:
        clicks.write(CLICK)
        for i in range(args.clicks):
            who = person(i * 313 + args.seed)
            clicks.write(f"k{i:032x},{who},c_{1+i%8},{BASE + i * 113 % 86_400_000}\n".encode())

    n = args.events
    high_distinct = max(1, n // 20)
    distinct = high_distinct if case == "high-dup" else n - n // 100
    key_digits = args.key_bytes - 1
    mask = (1 << (key_digits * 4)) - 1
    # Invertible in the fixed-width hex namespace, including all 32-digit IDs.
    multiplier = 0xd6e8feb86659fd93
    step = args.seed * 2 + 1
    while math.gcd(step, n) != 1:
        step += 2
    ingests = [timestamp(START + timedelta(seconds=rank)).encode() for rank in range((n-1)//high_distinct + 1)]
    long_product = b"x" * args.long_bytes if case == "long-lines" else b""
    files = args.small_files if case == "small-files" else args.files
    row_hash, other_hash = hashlib.sha256(), hashlib.sha256()
    max_line = event_bytes = 0
    for file in range(files):
        with (output / "events" / f"part-{file:06}.csv").open("wb") as stream:
            stream.write(EVENT)
            for position in range(file*n//files, (file+1)*n//files):
                # Rotate and permute the order so late copies also occur first on disk.
                j = (step*position + args.seed) % n
                value = (multiplier * (j % distinct) + args.seed * 3571) & mask
                key = f"e{value:0{key_digits}x}".encode()
                who = person(((j ^ (j >> 9))*0x9e3779b1 + args.seed) & 0xffffffff).encode()
                ts = b"bad" if j % 307 == 0 else str(BASE + (j * 1021 % 172_800_000)).encode()
                ingest = ingests[j // high_distinct]  # Unique per duplicate key in both rate settings.
                sale = j % 7 == 0
                kind = b"purchase" if sale else b"view"
                product = (b"p2" if j % 3 == 0 else b"p1")
                if long_product and j in (1, 2):
                    product = long_product  # A view ignores the product's category and price.
                price = str(j % 50_000 + 1).encode() if sale else b""
                body = b",".join((who, ts, ingest, kind, product, price)) + b"\n"
                line = key + b"," + body
                stream.write(line)
                row_hash.update(line)
                other_hash.update(body)
                event_bytes += len(line)
                max_line = max(max_line, len(line)-1)
    return {"event_rows": n, "unique_event_keys": distinct, "duplicate_rows": n-distinct,
            "event_key_bytes": args.key_bytes, "click_rows": args.clicks, "event_files": files,
            "event_payload_bytes": event_bytes, "max_event_line_bytes": max_line,
            "event_rows_sha256": row_hash.hexdigest(), "event_rows_without_id_sha256": other_hash.hexdigest()}


def generate(args):
    if args.events < 3 or args.clicks < 0 or not 2 <= args.key_bytes <= 257:
        raise ValueError("use >=3 events, >=0 clicks and key width in [2,257]")
    if args.events > 1 << (4*(args.key_bytes - 1)):
        raise ValueError("requested event IDs do not fit in the fixed width")
    if not 1 <= args.files < args.small_files or args.long_bytes <= 4 << 20:
        raise ValueError("require 1 <= files < small-files and long-bytes > 4 MiB")
    args.root.parent.mkdir(parents=True, exist_ok=True)
    if args.root.exists():
        raise FileExistsError(args.root)
    with tempfile.TemporaryDirectory(dir=args.root.parent) as staging:
        data = Path(staging) / "bundle"
        stats = {case: generate_case(data, case, args) for case in CASES}
        a, b, small = (stats[k] for k in CASES[:3])
        assert a["event_rows_without_id_sha256"] == b["event_rows_without_id_sha256"]
        assert a["event_payload_bytes"] == b["event_payload_bytes"]
        assert a["event_rows_sha256"] == small["event_rows_sha256"]
        meta = {k: getattr(args, k) for k in ("seed","events","clicks","key_bytes","files","small_files","long_bytes")}
        report(data / "datasets.json", {"parameters": meta, "cases": stats})
        with (data / "SHA256SUMS").open("w", encoding="ascii", newline="\n") as digests:
            for path in sorted(data.rglob("*")):
                if path.is_file() and path != data / "SHA256SUMS":
                    with path.open("rb") as source:
                        digest = hashlib.file_digest(source, "sha256").hexdigest()
                    print(digest, path.relative_to(data).as_posix(), sep="  ", file=digests)
        data.rename(args.root)
    print(json.dumps(stats, indent=2), flush=True)


def safe_component(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", value):
        raise argparse.ArgumentTypeError("use a directory name containing letters, digits, '_' or '-'")
    return value


def install(args):
    root = args.root.resolve()
    digest = hashlib.sha256((root / "SHA256SUMS").read_bytes()).hexdigest()
    command("docker", "volume", "inspect", args.volume, stdout=subprocess.DEVNULL)
    # Check all files before publication. A second install cannot replace a
    # prior dataset; interrupted copies remain invisible to timed runs.
    copy = """
target=/study/$1
test ! -e "$target" || { echo "Destination already exists: $target" >&2; exit 1; }
stage=$(mktemp -d /study/.design-stress.XXXXXXXX)
trap 'rm -rf -- "$stage"' EXIT
cp -R /inputs/. "$stage/"
cd "$stage"
echo "$2  SHA256SUMS" | sha256sum -c --status
sha256sum -c --status SHA256SUMS
chmod -R a+rX "$stage"
mv -Tn -- "$stage" "$target"
test ! -e "$stage"
"""
    command("docker", "run", "--rm", "--pull=never", "--network", "none",
            "--mount", f"type=bind,source={root},target=/inputs,readonly",
            "--mount", f"type=volume,source={args.volume},target=/study",
            HELPER, "sh", "-eu", "-c", copy, "install", args.name, digest)
    print(f"Installed {args.volume}/{args.name} with SHA256SUMS {digest}")


def race(args):
    args.work.mkdir(parents=True)
    inputs = command("docker", "run", "--rm", "--pull=never", "--network", "none",
                     "--mount", f"type=volume,source={args.volume},volume-subpath={args.name},target=/inputs,readonly",
                     "-w", "/inputs", HELPER, "sh", "-ec",
                     "sha256sum -c --status SHA256SUMS && sha256sum SHA256SUMS && cat datasets.json",
                     text=True, capture_output=True).stdout
    checksum, datasets = inputs.split("\n", 1)
    report(args.work / "inputs.json", {"volume":args.volume, "subdirectory":args.name,
           "sha256sums_digest":checksum.split()[0], "datasets":json.loads(datasets)})
    variants = {}
    for side, tag, env in (("A", args.a, args.a_setting), ("B", args.b, args.b_setting)):
        digest = command("docker", "image", "inspect", "--format", "{{.Id}}", tag,
                         capture_output=True, text=True).stdout.strip()
        variants[side] = {"tag":tag, "image":digest, "env":dict(env)}
    conf = args.work / "variants.json"
    report(conf, variants)
    order = list("AB" if args.quick else "ABBAAB")
    summary = {}
    def compare(left, right, log):
        with log.open("w", encoding="utf-8") as out:
            command(sys.executable, ROOT / "scripts/check_event_key_tables.py", left, right,
                    stdout=out, stderr=subprocess.STDOUT)
    for case in args.cases:
        directory = args.work / case
        directory.mkdir()
        log = directory / "runs.jsonl"
        with (directory / "docker.log").open("w", encoding="utf-8") as out:
            command(sys.executable, BENCH, args.volume, f"{args.name}/{case}", *order,
                    "--variants", conf, "--log", log, "--verify-cache", stdout=out, stderr=subprocess.STDOUT)
        rows = [json.loads(s) for s in log.read_text(encoding="utf-8").splitlines()]
        assert [r["image"] for r in rows] == order
        for row in rows:
            assert row["resident_input_bytes"] == row["input_page_bytes"] > 0
            assert row["image_id"] == variants[row["image"]]["image"] and not row["profile"]
        for side in variants:
            row = next(row for row in reversed(rows) if row["image"] == side)
            if not re.fullmatch(r"/output/shopping-bench-[0-9a-f]{32}", row["output"]):
                raise ValueError(f"Unexpected harness output: {row['output']}")
            dest = (directory / side).resolve()
            dest.mkdir()
            command("docker", "run", "--rm", "--pull=never", "--network", "none",
                    "--mount", f"type=volume,source={args.volume},volume-subpath=out,target=/output,readonly",
                    "--mount", f"type=bind,source={dest},target=/export",
                    HELPER, "sh", "-ec", 'cp -R -- "$1"/. /export/', "export", row["output"])
        compare(directory / "A", directory / "B", directory / "compare.sql.log")
        times = {side: [r["seconds"] for r in rows if r["image"] == side] for side in variants}
        summary[case] = {**times, "median_A":median(times["A"]), "median_B":median(times["B"]),
                         "gain":1-median(times["B"])/median(times["A"])}
        print(case, summary[case], flush=True)
    for case in ("small-files", "long-lines"):
        if "low-dup" in summary and case in summary:
            compare(args.work / "low-dup/A", args.work / case / "A", args.work / f"layout-{case}.sql.log")
    report(args.work / "summary.json", summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    g = sub.add_parser("generate")
    g.add_argument("root", type=Path, help="new local input bundle")
    g.add_argument("--seed", type=int, default=83)
    g.add_argument("--events", type=int, default=100_000)
    g.add_argument("--clicks", type=int, default=25_000)
    g.add_argument("--key-bytes", type=int, default=33)
    g.add_argument("--files", type=int, default=4)
    g.add_argument("--small-files", type=int, default=256)
    g.add_argument("--long-bytes", type=int, default=(4 << 20) + 257)
    i = sub.add_parser("install")
    i.add_argument("root", type=Path)
    r = sub.add_parser("race")
    for p in (i,r):
        p.add_argument("volume")
        p.add_argument("name", type=safe_component, help="fresh subdirectory used for this bundle")
    r.add_argument("work", type=Path, help="new local results directory")
    r.add_argument("--a", required=True, help="baseline image tag or digest")
    r.add_argument("--b", required=True, help="candidate image tag or digest")
    r.add_argument("--a-setting", type=setting, action="append", default=[], metavar="KEY=VALUE")
    r.add_argument("--b-setting", type=setting, action="append", default=[], metavar="KEY=VALUE")
    r.add_argument("--cases", nargs="+", choices=CASES, default=CASES)
    r.add_argument("--quick", action="store_true", help="one hot A/B pair per case, otherwise ABBAAB")
    args = parser.parse_args()
    {"generate":generate,"install":install,"race":race}[args.action](args)
