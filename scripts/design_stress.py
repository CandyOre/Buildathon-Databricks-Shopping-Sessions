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
WINNER_CASES = ("winner-valid", "winner-invalid")
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


def generate_winner_events(output, case, args):
    distinct = args.events // 2
    file_pairs = args.files // 2
    digits = args.key_bytes - 1
    mask = (1 << (digits * 4)) - 1
    ingests = [timestamp(START + timedelta(seconds=s)).encode() for s in (0, 1)]
    all_rows, fixed_fields = hashlib.sha256(), hashlib.sha256()
    length = longest = 0
    for f in range(args.files):
        pair, side = divmod(f, 2)
        with (output / "events" / f"part-{f:06}.csv").open("wb") as stream:
            stream.write(EVENT)
            for i in range(pair * distinct // file_pairs, (pair + 1) * distinct // file_pairs):
                # Alternate the order of earliest/latest copies across file pairs.
                rank = (i + side) % 2
                value = (i * 0xd6e8feb86659fd93 + args.seed * 3571) & mask
                key = f"e{value:0{digits}x}".encode()
                person = f"{'early' if rank == 0 else 'later'}{i // 16:07x}".encode()
                # Odd keys have one valid copy and one just outside the window.
                # Swapping equal-width timestamps leaves all other bytes fixed.
                bad_rank = 1 if case == "winner-valid" else 0
                millis = BASE + i % 1_200_000
                if i % 2 and rank == bad_rank:
                    millis -= 86_400_000
                head = key + b"," + person + b","
                tail = b"," + ingests[rank] + b",view,p1,\n"
                line = head + str(millis).encode() + tail
                stream.write(line)
                all_rows.update(line)
                fixed_fields.update(head + tail)
                length += len(line)
                longest = max(longest, len(line) - 1)
    return {"event_rows": args.events, "unique_event_keys": distinct, "duplicate_rows": distinct,
            "event_key_bytes": args.key_bytes, "click_rows": args.clicks, "event_files": args.files,
            "event_payload_bytes": length, "max_event_line_bytes": longest,
            "invalid_input_rows": distinct // 2, "swapped_pairs": distinct // 2,
            "expected_winning_events": distinct if case == "winner-valid" else (distinct + 1) // 2,
            "event_rows_sha256": all_rows.hexdigest(), "event_rows_without_ts_sha256": fixed_fields.hexdigest()}


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

    if case in WINNER_CASES:
        return generate_winner_events(output, case, args)
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
    if args.winner_validity and (args.events % 2 or args.files % 2 or not 2 <= args.files <= args.events):
        raise ValueError("winner validity needs even events and an even file count between 2 and events")
    if not args.winner_validity and (not 1 <= args.files < args.small_files or args.long_bytes <= 4 << 20):
        raise ValueError("require 1 <= files < small-files and long-bytes > 4 MiB")
    args.root.parent.mkdir(parents=True, exist_ok=True)
    if args.root.exists():
        raise FileExistsError(args.root)
    with tempfile.TemporaryDirectory(dir=args.root.parent) as staging:
        data = Path(staging) / "bundle"
        cases = WINNER_CASES if args.winner_validity else CASES
        stats = {case: generate_case(data, case, args) for case in cases}
        a, b = (stats[k] for k in cases[:2])
        if args.winner_validity:
            assert a["event_rows_without_ts_sha256"] == b["event_rows_without_ts_sha256"]
            assert a["event_rows_sha256"] != b["event_rows_sha256"]
        else:
            assert a["event_rows_without_id_sha256"] == b["event_rows_without_id_sha256"]
            assert a["event_rows_sha256"] == stats["small-files"]["event_rows_sha256"]
        assert a["event_payload_bytes"] == b["event_payload_bytes"]
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


def write_winner_answer(root, case, distinct):
    # Each block of at most 16 keys has its own earliest user. Its surviving
    # views all fit inside one 20-minute interval. Clicks/link endpoints use
    # a disjoint namespace, so every session is organic.
    root.mkdir()
    day = START.date().isoformat()
    with (root / "user_daily.csv").open("w", encoding="ascii", newline="\n") as out:
        print("user_id,day,sessions,events,revenue_cents,top_category", file=out)
        for start in range(0, distinct, 16):
            count = min(16, distinct - start)
            if case == "winner-invalid":
                count = (count + 1) // 2
            print(f"early{start // 16:07x},{day},1,{count},0,", file=out)
    (root / "campaign_daily.csv").write_text(
        "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n"
        f"organic,organic,{day},{(distinct + 15) // 16},0,0,0\n", encoding="ascii", newline="\n")


def race(args):
    args.work.mkdir(parents=True)
    inputs = command("docker", "run", "--rm", "--pull=never", "--network", "none",
                     "--mount", f"type=volume,source={args.volume},volume-subpath={args.name},target=/inputs,readonly",
                     "-w", "/inputs", HELPER, "sh", "-ec",
                     "sha256sum -c --status SHA256SUMS && sha256sum SHA256SUMS && cat datasets.json",
                     text=True, capture_output=True).stdout
    checksum, datasets = inputs.split("\n", 1)
    installed = json.loads(datasets)
    if args.cases is None:
        args.cases = [case for case in (*CASES, *WINNER_CASES) if case in installed["cases"]]
    unavailable = set(args.cases) - installed["cases"].keys()
    if unavailable:
        raise ValueError(f"requested cases absent from bundle: {sorted(unavailable)}")
    report(args.work / "inputs.json", {"volume":args.volume, "subdirectory":args.name,
           "sha256sums_digest":checksum.split()[0], "datasets":installed})
    variants = {}
    for side, tag, env in (("A", args.a, args.a_setting), ("B", args.b, args.b_setting)):
        digest = command("docker", "image", "inspect", "--format", "{{.Id}}", tag,
                         capture_output=True, text=True).stdout.strip()
        variants[side] = {"tag":tag, "image":digest, "env":dict(env)}
    conf = args.work / "variants.json"
    report(conf, variants)
    order = list("AB" if args.quick else "ABBAAB")
    summary = {}
    def compare(left, right, log, expected=False):
        with log.open("w", encoding="utf-8") as out:
            command(sys.executable, ROOT / "scripts/check_event_key_tables.py", left, right,
                    *(["--expected-csv"] if expected else []),
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
        if case in WINNER_CASES:
            expected = directory / "expected"
            write_winner_answer(expected, case, installed["parameters"]["events"] // 2)
            for side in variants:
                compare(directory / side, expected, directory / f"{side}-answer.sql.log", expected=True)
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
    g.add_argument("--winner-validity", action="store_true",
                   help="only generate the matched cross-file earliest-validity pair")
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
    r.add_argument("--cases", nargs="+", choices=(*CASES, *WINNER_CASES),
                   help="default: all dataset cases in the installed bundle")
    r.add_argument("--quick", action="store_true", help="one hot A/B pair per case, otherwise ABBAAB")
    args = parser.parse_args()
    {"generate":generate,"install":install,"race":race}[args.action](args)
