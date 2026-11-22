"""Report physical Parquet pages and byte totals for a Shopping Sessions export."""

import argparse
from collections import Counter
import json
import mmap
from pathlib import Path

import pyarrow.parquet as pq


PAGE_TYPES = {0: "data_v1", 1: "index", 2: "dictionary", 3: "data_v2"}
BYTE_FIELDS = ("pages", "page_header_bytes", "uncompressed_payload_bytes",
               "compressed_payload_bytes", "uncompressed_chunk_bytes", "compressed_chunk_bytes")


def empty_counts():
    return Counter(dict.fromkeys(BYTE_FIELDS, 0))


class CompactHeader:
    """Read one Thrift CompactProtocol page header without touching its payload."""

    def __init__(self, data, start, limit):
        self.data = data
        self.pos = start
        self.limit = limit

    def advance(self, size):
        if size > self.limit - self.pos:
            raise ValueError("page header exceeds column chunk")
        self.pos += size

    def byte(self):
        self.advance(1)
        return self.data[self.pos - 1]

    def varint(self):
        value = 0
        for shift in range(0, 70, 7):
            byte = self.byte()
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError("invalid Thrift varint")

    def integer(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def fields(self):
        field = 0
        while tag := self.byte():
            delta, kind = tag >> 4, tag & 15
            field = field + delta if delta else self.integer()
            yield field, kind

    def skip(self, kind, field=True):
        if kind in (1, 2):
            if not field:
                self.advance(1)
        elif kind == 3:
            self.advance(1)
        elif kind in (4, 5, 6):
            self.varint()
        elif kind == 7:
            self.advance(8)
        elif kind == 8:
            self.advance(self.varint())
        elif kind in (9, 10):
            tag = self.byte()
            size, element = tag >> 4, tag & 15
            if size == 15:
                size = self.varint()
            for _ in range(size):
                self.skip(element, field=False)
        elif kind == 11:
            size = self.varint()
            if size:
                tag = self.byte()
                for _ in range(size):
                    self.skip(tag >> 4, field=False)
                    self.skip(tag & 15, field=False)
        elif kind == 12:
            for _, subkind in self.fields():
                self.skip(subkind)
        else:
            raise ValueError(f"unknown Thrift type {kind}")

    def page(self):
        values = {}
        for field, kind in self.fields():
            if field in (1, 2, 3) and kind == 5:
                values[field] = self.integer()
            else:
                self.skip(kind)
        if values.keys() != {1, 2, 3} or min(values[2], values[3]) < 0:
            raise ValueError("missing or invalid Parquet page sizes")
        return values[1], values[2], values[3]


def column_statistics(data, column):
    offsets = [column.data_page_offset]
    if column.has_dictionary_page:
        offsets.append(column.dictionary_page_offset)
    # A writer may emit only a dictionary page or no pages for an empty
    # column. In both cases it can put zero in the unused data-page offset.
    start = min((offset for offset in offsets if offset and offset >= 4), default=4)
    if column.total_compressed_size and not any(offset and offset >= 4 for offset in offsets):
        raise ValueError("column chunk with pages has no page offset")
    end = start + column.total_compressed_size
    if start < 4 or end > len(data):
        raise ValueError(f"invalid column offset {start}..{end}")
    counts = empty_counts()
    kinds = Counter()
    pos = start
    while pos < end:
        header = CompactHeader(data, pos, end)
        kind, raw, compressed = header.page()
        counts["page_header_bytes"] += header.pos - pos
        counts["uncompressed_payload_bytes"] += raw
        counts["compressed_payload_bytes"] += compressed
        kinds[PAGE_TYPES.get(kind, str(kind))] += 1
        pos = header.pos + compressed
        if pos > end:
            raise ValueError("page payload exceeds column chunk")
    counts["pages"] = sum(kinds.values())
    counts["compressed_chunk_bytes"] = counts["page_header_bytes"] + counts["compressed_payload_bytes"]
    counts["uncompressed_chunk_bytes"] = counts["page_header_bytes"] + counts["uncompressed_payload_bytes"]
    if counts["uncompressed_chunk_bytes"] != column.total_uncompressed_size:
        raise ValueError("page bytes disagree with footer total_uncompressed_size")
    return counts, kinds


def inspect_file(path, details=False):
    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    schema = [dict(name=f.name, type=str(f.type), nullable=f.nullable) for f in parquet_file.schema_arrow]
    totals, all_kinds, codecs = empty_counts(), Counter(), Counter()
    groups = []
    with path.open("rb") as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
        for index in range(metadata.num_row_groups):
            group = metadata.row_group(index)
            numbers, kinds, group_codecs = empty_counts(), Counter(), Counter()
            cols = []
            for j in range(group.num_columns):
                col = group.column(j)
                if col.file_path:
                    raise ValueError("external column-chunk files are unsupported")
                try:
                    counts, page_types = column_statistics(data, col)
                except ValueError as error:
                    raise ValueError(f"{path} group {index} column {col.path_in_schema}: {error}") from error
                group_codecs[col.compression] += 1
                numbers.update(counts)
                kinds.update(page_types)
                if details:
                    cols.append(dict(name=col.path_in_schema, codec=col.compression, **counts, page_types=page_types))
            totals.update(numbers)
            all_kinds.update(kinds)
            codecs.update(group_codecs)
            record = dict(index=index, rows=group.num_rows, **numbers, page_types=kinds, codec_chunks=group_codecs)
            if details:
                record["columns"] = cols
            groups.append(record)
        size = len(data)
    return dict(file=str(path), file_bytes=size, rows=metadata.num_rows, row_groups=len(groups),schema=schema,
                **totals, page_types=all_kinds, codec_chunks=codecs,
                non_chunk_file_bytes=size - totals["compressed_chunk_bytes"], groups=groups)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path, help="Export directories, table directories or Parquet files")
    parser.add_argument("--columns", action="store_true", help="Also include individual column chunks")
    args = parser.parse_args()
    paths = set()
    for target in args.paths:
        if target.is_dir():
            paths.update(target.rglob("*.parquet"))
        elif target.is_file():
            paths.add(target)
        else:
            parser.error(f"path does not exist: {target}")
    if not paths:
        parser.error("no Parquet files found")
    print(json.dumps([inspect_file(path, args.columns) for path in sorted(paths)], indent=2))


if __name__ == "__main__":
    main()
