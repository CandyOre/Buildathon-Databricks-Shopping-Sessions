"""Check Shopping Sessions Parquet outputs against CSV answers or another version.

Small checks require the Python packages in requirements.txt. The large checks
read the Docker Linux output volume inside an image built from Dockerfile.duckdb.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "benchmarks" / "polars" / "checks"
IMAGES = {
    "native": "shopping-sessions:polars-base-bfc2c1c",
    "dedup": "shopping-sessions:polars-dedup",
    "full": "shopping-sessions:polars-full",
}
COLUMNS = {
    "user_daily": ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
    "campaign_daily": ["campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                       "revenue_cents", "assisted_sessions"],
}
TEXT = {"user_id", "campaign_id", "channel", "top_category"}
BASE = ["docker", "run", "--rm", "--platform", "linux/amd64", "--network", "none",
        "--read-only", "--user", "1000:1000", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--cpuset-cpus", "0-7",
        "--cpus", "8", "--memory", "24g", "--memory-swap", "24g",
        "--tmpfs", "/tmp:rw,size=64m,mode=1777", "-e", "NCPU=8"]


def sql_type(col):
    return "VARCHAR" if col in TEXT else ("DATE" if col == "day" else "BIGINT")


def load_side(db, label, folder, table):
    import pyarrow as pa
    import pyarrow.parquet as pq

    cols = COLUMNS[table]
    csv = folder / f"{table}.csv"
    if csv.is_file():
        query = ", ".join(f'CAST("{c}" AS {sql_type(c)}) AS "{c}"' for c in cols)
        db.execute(f"CREATE OR REPLACE TEMP TABLE {label} AS SELECT {query} "
                   "FROM read_csv(?, header=true, all_varchar=true)", [str(csv)])
        return
    files = sorted(p for p in (folder / table).glob("*.parquet") if not p.name.startswith(("_", ".")))
    if not files:
        raise ValueError(f"No parquet files for {folder / table}")
    schema = pa.schema([pa.field(c, pa.string() if c in TEXT else
                                 (pa.date32() if c == "day" else pa.int64()),
                                 nullable=(c == "top_category")) for c in cols])
    for path in files:
        actual = pq.read_schema(path)
        if not actual.equals(schema):
            raise ValueError(f"{path}: schema {actual} differs from {schema}")
    db.execute(f"CREATE OR REPLACE TEMP TABLE {label} AS SELECT * FROM read_parquet(?)",
               [[str(p) for p in files]])


def compare(db, left, right):
    result = {}
    for table, cols in COLUMNS.items():
        key = cols[0]
        load_side(db, "a", left, table)
        load_side(db, "b", right, table)
        sides = []
        for label in ("a", "b"):
            nulls = ", ".join(f'COUNT(*) - COUNT("{c}")' for c in cols)
            rows, keys, *counts = db.execute(
                f'SELECT COUNT(*), COUNT(DISTINCT ("{key}", day)), {nulls} FROM {label}'
            ).fetchone()
            sides.append({"rows": rows, "unique_keys": keys, "nulls": dict(zip(cols, counts))})
        diffs = dict(zip(cols, db.execute(
            "SELECT " + ", ".join(f'COUNT(*) FILTER (WHERE a."{c}" IS DISTINCT FROM b."{c}")'
                                  for c in cols) +
            f' FROM a FULL OUTER JOIN b USING ("{key}", day)').fetchone()))
        only = [db.execute(f"SELECT COUNT(*) FROM (FROM {x} EXCEPT ALL FROM {y})").fetchone()[0]
                for x, y in (("a", "b"), ("b", "a"))]
        result[table] = {"left": sides[0], "right": sides[1], "column_mismatches": diffs,
                         "left_except_all_right": only[0], "right_except_all_left": only[1]}
        if (any(only) or any(diffs.values()) or sides[0] != sides[1]
                or any(s["unique_keys"] != s["rows"] for s in sides)
                or any(n for c, n in sides[1]["nulls"].items() if c != "top_category")):
            raise ValueError(f"{left} vs {right}: {json.dumps(result, indent=2)}")
        db.execute("DROP TABLE a")
        db.execute("DROP TABLE b")
    return result


def database():
    import duckdb

    db = duckdb.connect()
    db.execute("SET threads=4")
    db.execute("SET memory_limit='8GB'")
    return db


def targeted_fixture(folder):
    target = folder / "copy_and_click_ties"
    inp = target / "input"

    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding="utf-8", newline="\n")

    write(inp / "manifest.json", '{"window_start":"2026-10-01T00:00:00Z","window_end":"2026-10-15T00:00:00Z"}')
    write(inp / "products.csv", "product_id,category\np,books\n")
    write(inp / "campaigns.csv", "campaign_id,channel\nupper,email\nlower,social\n")
    h = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
    at = "2026-10-03T12:00:00Z"
    later = "2026-10-03T12:01:00Z"
    write(inp / "events" / "a.csv", h + f"bad,r,BAD,{at},view,p,\n" +
          f'malformed,r,{at},{at},v"iew,p,\n' + f"tie,r,{at},{at},purchase,p,7\n")
    write(inp / "events" / "b.csv", h + f"bad,s,{at},{later},purchase,p,50\n" +
          f"malformed,r,{at},{later},purchase,p,3\n" + f"tie,r,{at},{at},purchase,p,7\n")
    write(inp / "clicks" / "a.csv", "click_id,user_id,campaign_id,ts\n" +
          f"a,r,lower,{at}\nA,r,upper,{at}\n")
    exp = target / "expected"
    write(exp / "user_daily.csv", ",".join(COLUMNS["user_daily"]) + "\nr,2026-10-03,1,2,10,books\n")
    write(exp / "campaign_daily.csv", ",".join(COLUMNS["campaign_daily"]) +
          "\nupper,email,2026-10-03,1,1,10,1\nlower,social,2026-10-03,0,0,0,1\n")
    return target


def bind(path, target, readonly=True):
    return ["--mount", f"type=bind,source={path.resolve()},target={target}" +
            (",readonly" if readonly else "")]


def save(name, value):
    RESULTS.mkdir(parents=True, exist_ok=True)
    target = RESULTS / f"{name}.json"
    target.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    print(target, flush=True)


def small(args):
    image = IMAGES[args.variant]
    scratch = ROOT / ".buildathon" / "polars-check"
    scratch.mkdir(parents=True, exist_ok=True)
    image_id = subprocess.check_output(["docker", "image", "inspect", image, "-f", "{{.Id}}"],
                                       text=True).strip()
    db = database()
    with tempfile.TemporaryDirectory(dir=scratch, ignore_cleanup_errors=True) as tmp:
        work = Path(tmp)
        targeted = targeted_fixture(work / "targeted")
        output = work / "output"
        output.mkdir()
        output.chmod(0o777)
        fixtures = ROOT / "fixtures"
        cases = sorted(p for p in fixtures.iterdir() if p.is_dir())
        if len(cases) != 36:
            raise ValueError(f"Expected 36 official cases, got {len(cases)}")
        loop = """
        set -eu
        for case_dir in /fixtures/* /targeted/*; do
            [ -d "$case_dir" ] || continue
            DATA_DIR="$case_dir/input"
            if [ -d "$case_dir/data" ]; then DATA_DIR="$case_dir/data"; fi
            OUTPUT_DIR="/output/${case_dir##*/}"
            export DATA_DIR OUTPUT_DIR
            /usr/local/bin/shopping-sessions
        done
        """
        subprocess.run(BASE + bind(fixtures, "/fixtures") + bind(targeted.parent, "/targeted") +
                       bind(output, "/output", False) + ["--entrypoint", "/bin/sh", image, "-c", loop],
                       check=True)
        passed = []
        for case in cases + [targeted]:
            result = compare(db, case / "expected", output / case.name)
            passed.append({"case": case.name,
                           "rows": {t: r["right"]["rows"] for t, r in result.items()}})
        out_1m = output / "public-1m"
        out_1m.mkdir()
        out_1m.chmod(0o777)
        subprocess.run(BASE + ["--mount", f"type=volume,source={args.data_volume},target=/data,"
                                          "volume-subpath=events-1m,readonly"] +
                       bind(out_1m, "/output", False) + [image], check=True)
        public = compare(db, args.answer, out_1m)
        save(f"small-{args.variant}", {"variant": args.variant, "image": image_id,
                                       "official_fixtures_passed": len(cases),
                                       "fixtures": passed, "public_1m_seed_1": public})


def large(args):
    cmd = BASE + ["--mount", f"type=volume,source={args.out_volume},target=/results,readonly"]
    cmd += bind(Path(__file__), "/check.py")
    raw = subprocess.check_output(cmd + ["--entrypoint", "python", args.check_image,
                                        "/check.py", "worker", args.size, args.variant], text=True)
    save(f"{args.size}-{args.variant}", json.loads(raw))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    p = modes.add_parser("small")
    p.add_argument("variant", choices=IMAGES)
    p.add_argument("--answer", type=Path, default=ROOT / ".buildathon" / "dev-answer")
    p.add_argument("--data-volume", default="shopping-sessions-bench")
    for name in ("large", "worker"):
        p = modes.add_parser(name)
        p.add_argument("size", choices=["20m", "60m"])
        p.add_argument("variant", choices=["dedup", "full"])
        p.add_argument("--out-volume", default="shopping-sessions-bench-out")
        p.add_argument("--check-image", default="shopping-sessions:duckdb")
    args = parser.parse_args()
    if args.mode == "small":
        small(args)
    elif args.mode == "large":
        large(args)
    else:
        root = Path("/results")
        prefix = f"polars-{args.size}-"
        result = compare(database(), root / (prefix + "native"), root / (prefix + args.variant))
        print(json.dumps({"size": args.size, "baseline": "native", "candidate": args.variant,
                          "tables": result}))


if __name__ == "__main__":
    main()
