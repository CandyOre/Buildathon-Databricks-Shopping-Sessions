#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

import duckdb

from inputs import csv_rows, load_rows, user_aliases


def run(data, output, scratch):
    with duckdb.connect() as con:
        threads = int(os.environ.get("NCPU", min(8, os.cpu_count() or 1)))
        memory_gb = int(os.environ.get("MEM_GB", "8"))
        con.execute("SET threads = ?", [threads])
        con.execute("SET memory_limit = ?", [f"{max(1, int(memory_gb * 0.8))}GB"])
        con.execute("SET temp_directory = ?", [str(scratch)])
        con.execute("SET preserve_insertion_order = false")

        manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))
        con.execute("""
            CREATE TEMP TABLE run_window AS
            SELECT epoch_ms(CAST(? AS TIMESTAMPTZ)) AS start_ms,
                   epoch_ms(CAST(? AS TIMESTAMPTZ)) AS end_ms
        """, [manifest["window_start"], manifest["window_end"]])

        load_rows(con, "aliases", ("user_id", "person"), user_aliases((data / "links").glob("*.csv")))
        for table, columns in (
            ("products", ("product_id", "category")),
            ("campaigns", ("campaign_id", "channel")),
            ("event_rows", ("event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents")),
            ("click_rows", ("click_id", "user_id", "campaign_id", "ts")),
        ):
            parts = {"event_rows": "events", "click_rows": "clicks"}
            paths = (data / parts[table]).glob("*.csv") if table in parts else [data / f"{table}.csv"]
            load_rows(con, table, columns, csv_rows(paths, len(columns)))

        con.execute(Path(__file__).with_name("sessions.sql").read_text(encoding="utf-8"))

        for table in ("user_daily", "campaign_daily"):
            folder = output / table
            folder.mkdir(parents=True, exist_ok=True)
            con.execute(f"COPY {table} TO ? (FORMAT parquet)", [str(folder / "part-0.parquet")])
        print("Wrote user_daily and campaign_daily", file=sys.stderr)


def main():
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    scratch.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="shopping-sessions-", dir=scratch) as work:
        run(data, output, Path(work))


if __name__ == "__main__":
    main()
