from itertools import islice

import pyarrow as pa


def csv_rows(paths, width):
    """Parse the rules' line-oriented dialect, preserving spaces and CR bytes."""
    for path in paths:
        with path.open("rb") as source:
            next(source, None)  # header
            for line in source:
                fields = line.removesuffix(b"\n").split(b",")
                if len(fields) != width:
                    continue
                for index, field in enumerate(fields):
                    if b'"' in field:
                        if len(field) < 2 or field[:1] != b'"' or field[-1:] != b'"':
                            break
                        field = field[1:-1]
                        if b'"' in field:
                            break
                    fields[index] = field or None
                else:
                    yield fields


def load_rows(con, table, columns, rows):
    schema = pa.schema([(column, pa.string()) for column in columns])
    batches = iter(lambda: list(islice(rows, 100_000)), [])
    con.register("input_rows", pa.RecordBatchReader.from_batches(schema, (
        pa.RecordBatch.from_arrays([pa.array(values, type=pa.string()) for values in zip(*batch)], schema=schema)
        for batch in batches
    )))
    try:
        con.execute(f"CREATE TEMP TABLE {table} AS SELECT * FROM input_rows")
    finally:
        con.unregister("input_rows")


def user_aliases(paths):
    parents = {}
    sizes = {}
    names = {}

    def root(user_id):
        while user_id != parents[user_id]:
            parents[user_id] = parents[parents[user_id]]
            user_id = parents[user_id]
        return user_id

    for left, right in csv_rows(paths, 2):
        if left is None or right is None or left == right:
            continue
        for user_id in (left, right):
            if user_id not in parents:
                parents[user_id] = user_id
                sizes[user_id] = 1
                names[user_id] = user_id
        first, second = root(left), root(right)
        if first == second:
            continue
        if sizes[first] < sizes[second]:
            first, second = second, first
        parents[second] = first
        sizes[first] += sizes.pop(second)
        names[first] = min(names[first], names.pop(second))

    for user_id in parents:
        person = names[root(user_id)]
        if user_id != person:
            yield user_id, person
