"""Buffered, partitioned Parquet writer with rotation and atomic finalisation.

Records are buffered per partition ``venue/channel/symbol/date`` (date of ``ts_local``, UTC) and
written as row groups into an in-progress file under ``staging/``. A part is finalised by closing
the Parquet footer, fsync-ing, and ``os.replace``-ing it into the tape tree, which is atomic on one
file system. Readers of ``tape/`` therefore only ever see complete ``part-<n>.parquet`` files.

One writer process per output root is assumed (part numbers are allocated by scanning the
partition directory).
"""

from __future__ import annotations

import os
import re
import uuid
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from tape_recorder.schema import SCHEMAS, Channel, Record, records_to_table
from tape_recorder.settings import WriterSettings
from tape_recorder.timeutil import NS_PER_S, Clock, ns_to_date

log = structlog.get_logger(__name__)

PART_RE = re.compile(r"^part-(\d+)\.parquet$")
IN_PROGRESS_SUFFIX = ".inprogress"


@dataclass(frozen=True, slots=True, order=True)
class PartitionKey:
    venue: str
    channel: Channel
    symbol: str
    date: str

    def relative_dir(self) -> Path:
        return (
            Path(f"venue={self.venue}")
            / f"channel={self.channel}"
            / f"symbol={self.symbol}"
            / f"date={self.date}"
        )


def partition_of(record: Record) -> PartitionKey:
    return PartitionKey(
        record.venue, record.channel, record.symbol, ns_to_date(record.ts_local).isoformat()
    )


def next_part_index(directory: Path, reserved: set[int]) -> int:
    existing = {
        int(m.group(1)) for p in directory.glob("part-*.parquet") if (m := PART_RE.match(p.name))
    }
    taken = existing | reserved
    return max(taken) + 1 if taken else 0


def fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write a small file (JSON, Markdown) atomically via a sibling temp file and rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}{IN_PROGRESS_SUFFIX}")
    with tmp.open("wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(path)
    fsync_dir(path.parent)


@dataclass(slots=True)
class _OpenPart:
    staging_path: Path
    final_path: Path
    writer: pq.ParquetWriter
    opened_at: int
    rows: int = 0


@dataclass(slots=True)
class _Buffer:
    records: list[Record] = field(default_factory=list)
    since: int = 0


class TapeWriter:
    """See module docstring. Not thread-safe; owned by one event loop."""

    def __init__(self, root: Path, staging: Path, settings: WriterSettings, clock: Clock) -> None:
        self.root = root
        self.staging = staging
        self.settings = settings
        self._clock = clock
        self._buffers: dict[PartitionKey, _Buffer] = defaultdict(_Buffer)
        self._open: dict[PartitionKey, _OpenPart] = {}
        self._reserved: dict[PartitionKey, set[int]] = defaultdict(set)
        self.finalised: list[Path] = []
        self.rows_written = 0
        self.staging.mkdir(parents=True, exist_ok=True)
        orphans = list(self.staging.glob(f"*{IN_PROGRESS_SUFFIX}"))
        if orphans:
            # Unfinished parts from a crashed process have no Parquet footer; keep them for
            # inspection but never move them into the tape tree.
            log.warning("staging_orphans", count=len(orphans), staging=str(self.staging))

    @property
    def buffered_rows(self) -> int:
        """Rows accepted but not yet written to a Parquet row group (the writer's backlog)."""
        return sum(len(b.records) for b in self._buffers.values())

    def write(self, records: Sequence[Record]) -> None:
        now = self._clock()
        for r in records:
            key = partition_of(r)
            buf = self._buffers[key]
            if not buf.records:
                buf.since = now
            buf.records.append(r)
            if len(buf.records) >= self.settings.flush_rows:
                self._flush(key)

    def tick(self) -> None:
        """Time-based flush and rotation; call periodically."""
        now = self._clock()
        flush_ns = round(self.settings.flush_interval_s * NS_PER_S)
        rotate_ns = round(self.settings.rotate_interval_s * NS_PER_S)
        for key, buf in list(self._buffers.items()):
            if buf.records and now - buf.since >= flush_ns:
                self._flush(key)
        for key, part in list(self._open.items()):
            if now - part.opened_at >= rotate_ns:
                self._finalise(key)

    def close(self) -> list[Path]:
        """Flush every buffer and finalise every open part; returns all finalised paths."""
        for key in list(self._buffers):
            if self._buffers[key].records:
                self._flush(key)
        for key in list(self._open):
            self._finalise(key)
        return self.finalised

    def _flush(self, key: PartitionKey) -> None:
        buf = self._buffers.pop(key)
        table = records_to_table(key.channel, buf.records)
        self._append(key, table)

    def write_table(self, key: PartitionKey, table: pa.Table) -> None:
        """Append an already-normalised table (used by merge-regions)."""
        self._append(key, table.cast(SCHEMAS[key.channel]))

    def _append(self, key: PartitionKey, table: pa.Table) -> None:
        part = self._open.get(key) or self._open_part(key)
        part.writer.write_table(table)
        part.rows += table.num_rows
        self.rows_written += table.num_rows
        if part.rows >= self.settings.rotate_rows:
            self._finalise(key)

    def _open_part(self, key: PartitionKey) -> _OpenPart:
        final_dir = self.root / key.relative_dir()
        final_dir.mkdir(parents=True, exist_ok=True)
        index = next_part_index(final_dir, self._reserved[key])
        self._reserved[key].add(index)
        staging_path = self.staging / f"{uuid.uuid4().hex}-part-{index}.parquet{IN_PROGRESS_SUFFIX}"
        writer = pq.ParquetWriter(
            staging_path, SCHEMAS[key.channel], compression=self.settings.compression
        )
        part = _OpenPart(staging_path, final_dir / f"part-{index}.parquet", writer, self._clock())
        self._open[key] = part
        return part

    def _finalise(self, key: PartitionKey) -> None:
        part = self._open.pop(key)
        part.writer.close()
        with part.staging_path.open("rb") as fh:
            os.fsync(fh.fileno())
        part.staging_path.replace(part.final_path)
        fsync_dir(part.final_path.parent)
        self.finalised.append(part.final_path)
        log.debug("part_finalised", path=str(part.final_path), rows=part.rows)
