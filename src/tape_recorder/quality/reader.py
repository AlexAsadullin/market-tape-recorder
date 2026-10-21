"""Reading the contract 6.2 tape tree back (complete parts only)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tape_recorder.schema import SCHEMAS, Channel
from tape_recorder.writer import PartitionKey

_DIR_RE = re.compile(r"^(venue|channel|symbol|date)=(.+)$")


def list_partitions(root: Path) -> list[PartitionKey]:
    keys: list[PartitionKey] = []
    for date_dir in sorted(root.glob("venue=*/channel=*/symbol=*/date=*")):
        if not any(date_dir.glob("part-*.parquet")):
            continue
        parts = {}
        for piece in date_dir.relative_to(root).parts:
            m = _DIR_RE.match(piece)
            if m:
                parts[m.group(1)] = m.group(2)
        try:
            channel = Channel(parts["channel"])
        except ValueError:
            continue
        keys.append(PartitionKey(parts["venue"], channel, parts["symbol"], parts["date"]))
    return keys


def part_files(root: Path, key: PartitionKey) -> list[Path]:
    directory = root / key.relative_dir()
    return sorted(directory.glob("part-*.parquet"), key=lambda p: int(p.stem.removeprefix("part-")))


def read_partition(root: Path, key: PartitionKey) -> pa.Table:
    """All rows of a partition in part order (the arrival order of a single writer)."""
    files = part_files(root, key)
    schema = SCHEMAS[key.channel]
    if not files:
        return schema.empty_table()
    return pa.concat_tables([pq.read_table(f, schema=schema) for f in files])


def instrument_days(
    root: Path, day: date | None = None, venue: str | None = None, symbol: str | None = None
) -> list[tuple[str, str, str]]:
    found = {
        (k.venue, k.symbol, k.date)
        for k in list_partitions(root)
        if (day is None or k.date == day.isoformat())
        and (venue is None or k.venue == venue)
        and (symbol is None or k.symbol == symbol)
    }
    return sorted(found)
