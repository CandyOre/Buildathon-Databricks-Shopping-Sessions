"""Check two Shopping Sessions output roots with bidirectional SQL EXCEPT ALL.

Use --expected-csv for an official answer or handwritten fixture on the right.
Without it, both arguments must contain the two Parquet output directories.
"""
import argparse
import csv
from pathlib import Path
import tempfile

import duckdb

from run_fixtures import SCHEMAS


INTEGER_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
}


def load_table(con, root, table, csv_answer):
    schema = SCHEMAS[table]
    types = {name: "HUGEINT" if dtype.is_integer() else "DATE" if name == "day" else "VARCHAR"
             for name, dtype in schema.items()}
    if csv_answer:
        path = root / (table + ".csv")
        with path.open(encoding="utf-8", newline="") as src:
            header = next(csv.reader(src))
        if sorted(header) != sorted(schema):
            raise ValueError(f"Wrong CSV columns in {path}: {header}")
        return con.read_csv(str(path), columns={name: types[name] for name in header}, header=True)

    directory = root / table
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError(f"Expected Parquet output directory: {directory}")
    files = []
    for path in sorted(directory.glob("*.parquet")):
        if path.name.startswith((".", "_")) or path.is_symlink() or not path.is_file():
            continue
        part = con.read_parquet(str(path))
        if sorted(part.columns) != sorted(schema):
            raise ValueError(f"Wrong Parquet columns in {path}: {part.columns}")
        for name, dtype in zip(part.columns, part.types):
            actual = str(dtype)
            if actual != types[name] and not (types[name] == "HUGEINT" and actual in INTEGER_TYPES):
                raise ValueError(f"{path}/{name} is {actual}, expected {types[name]}")
        files.append(str(path))
    if not files:
        raise ValueError(f"No Parquet parts in {directory}")
    return con.read_parquet(files, union_by_name=True)


def subtract(columns, left, right):
    return f"SELECT {columns} FROM {left} EXCEPT ALL SELECT {columns} FROM {right}"


def check(con, left_root, right_root, table, csv_answer):
    load_table(con, left_root, table, False).create_view("side_a", replace=True)
    load_table(con, right_root, table, csv_answer).create_view("side_b", replace=True)
    keys = ["user_id" if table == "user_daily" else "campaign_id", "day"]
    columns = list(SCHEMAS[table])
    all_columns = ", ".join(columns)
    invalid = " OR ".join(name + " IS NULL" for name in columns if name != "top_category")
    ok = True
    for side in ("side_a", "side_b"):
        rows, duplicates, nulls = con.execute(f"""
            SELECT count(*), count(*) - count(DISTINCT ({', '.join(keys)})),
                   count(*) FILTER (WHERE {invalid}) FROM {side}
        """).fetchone()
        print(f"{table} {side}: rows={rows} duplicate_key_rows={duplicates} invalid_null_rows={nulls}")
        ok &= duplicates == 0 and nulls == 0

    for dest, left, right in [("a_only", "side_a", "side_b"), ("b_only", "side_b", "side_a")]:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {dest} AS " + subtract(all_columns, left, right))
    whole = con.execute("SELECT (SELECT count(*) FROM a_only), (SELECT count(*) FROM b_only)").fetchone()
    print(f"{table} full rows: A EXCEPT ALL B={whole[0]}, B EXCEPT ALL A={whole[1]}")
    ok &= whole == (0, 0)

    # Full-row subtraction cancelled the same multiset from each side. Any
    # projection of that common multiset also cancels, so the residuals give
    # exactly the original per-column EXCEPT ALL result, including duplicates.
    for name in columns:
        projected = ", ".join(keys if name in keys else [*keys, name])
        counts = [
            con.execute("SELECT count(*) FROM (" + subtract(projected, left, right) + ")").fetchone()[0]
            for left, right in [("a_only", "b_only"), ("b_only", "a_only")]
        ]
        print(f"  {name}: A EXCEPT ALL B={counts[0]}, B EXCEPT ALL A={counts[1]}")
        ok &= counts == [0, 0]
    return ok


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    parser.add_argument("--expected-csv", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as spill, duckdb.connect(config={
        "threads": "2", "memory_limit": "3GiB", "temp_directory": spill,
        "preserve_insertion_order": "false",
    }) as con:
        ok = all([check(con, args.left, args.right, table, args.expected_csv) for table in SCHEMAS])
    print("MATCH" if ok else "DIFFER")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
