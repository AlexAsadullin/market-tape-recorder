"""Merge two regional captures of the same streams into one tape, earliest arrival first.

Each row gets a message key that identifies the venue event independently of who received it:

=============== ==================================================
channel          key
=============== ==================================================
trades           ``trade_id``
book_delta       ``seq`` (all levels of one message share it)
book_snapshot    ``snapshot_id``
funding          ``ts_exchange``, ``rate``, ``mark_price``
liquidations     ``ts_exchange``, ``side``, ``price``, ``size``
open_interest    ``ts_exchange``, ``open_interest``
=============== ==================================================

For every key the region with the smallest ``ts_local`` wins and all its rows for that key are
kept, so a book message is never split across regions. Rows whose key is null (no exchange
timestamp, e.g. Hyperliquid asset contexts) cannot be matched and are taken from the primary
region. Per-message arrival times of both regions are written as latency distributions.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import BaseModel

from tape_recorder.quality.reader import list_partitions, read_partition
from tape_recorder.schema import SCHEMA_VERSION, SCHEMAS, Channel
from tape_recorder.settings import Settings
from tape_recorder.timeutil import NS_PER_MS
from tape_recorder.writer import IN_PROGRESS_SUFFIX, TapeWriter, atomic_write_bytes

KEY_COLUMNS: dict[Channel, list[str]] = {
    Channel.TRADES: ["trade_id"],
    Channel.BOOK_DELTA: ["seq"],
    Channel.BOOK_SNAPSHOT: ["snapshot_id"],
    Channel.FUNDING: ["ts_exchange", "rate", "mark_price"],
    Channel.LIQUIDATIONS: ["ts_exchange", "side", "price", "size"],
    Channel.OPEN_INTEREST: ["ts_exchange", "open_interest"],
}
_KEY = "__key"
_ROW = "__row"


class Distribution(BaseModel):
    n: int
    mean_ms: float | None
    quantiles_ms: dict[str, float]


class LatencyReport(BaseModel):
    schema_version: str = SCHEMA_VERSION
    venue: str
    symbol: str
    date: str
    regions: list[str]
    matched_messages: int
    win_share: dict[str, float]
    lead_ms: dict[str, Distribution]
    exchange_latency_ms: dict[str, Distribution]


@dataclass(slots=True)
class MergeResult:
    partitions: int
    rows_in: int
    rows_out: int
    latency_reports: list[Path]


def message_keys(table: pa.Table, channel: Channel) -> pa.ChunkedArray:
    parts = [pc.cast(table.column(c), pa.string()) for c in KEY_COLUMNS[channel]]
    if len(parts) == 1:
        return parts[0]
    return pc.binary_join_element_wise(*parts, "|")


def distribution(values_ms: np.ndarray, quantiles: list[float]) -> Distribution:
    if values_ms.size == 0:
        return Distribution(n=0, mean_ms=None, quantiles_ms={})
    qs = np.quantile(values_ms, quantiles)
    return Distribution(
        n=int(values_ms.size),
        mean_ms=float(values_ms.mean()),
        quantiles_ms={f"{q:g}": float(v) for q, v in zip(quantiles, qs, strict=True)},
    )


def merge_partition(
    a: pa.Table, b: pa.Table, channel: Channel, primary_region: str | None
) -> tuple[pa.Table, pa.Table]:
    """Return (merged rows, per-(key, region) first arrivals)."""
    both = pa.concat_tables([a, b])
    both = both.append_column(_KEY, message_keys(both, channel))
    both = both.append_column(_ROW, pa.array(np.arange(both.num_rows, dtype=np.int64)))
    keyed = both.filter(pc.is_valid(both.column(_KEY)))
    arrivals = keyed.group_by([_KEY, "capture_region"]).aggregate([("ts_local", "min")])
    ordered = arrivals.sort_by(
        [(_KEY, "ascending"), ("ts_local_min", "ascending"), ("capture_region", "ascending")]
    )
    winners = ordered.group_by(_KEY, use_threads=False).aggregate([("capture_region", "first")])
    winners = winners.rename_columns([_KEY, "capture_region"])
    kept = keyed.join(winners, keys=[_KEY, "capture_region"], join_type="inner")
    unkeyed = both.filter(pc.is_null(both.column(_KEY)))
    if unkeyed.num_rows:
        primary = primary_region or str(a.column("capture_region")[0] if a.num_rows else "")
        unkeyed = unkeyed.filter(pc.equal(unkeyed.column("capture_region"), primary))
        kept = pa.concat_tables(
            [kept.select(unkeyed.column_names), unkeyed], promote_options="permissive"
        )
    kept = kept.sort_by([("ts_local", "ascending"), (_ROW, "ascending")])
    return kept.select(SCHEMAS[channel].names).cast(SCHEMAS[channel]), arrivals


def _latency_report(
    venue: str,
    symbol: str,
    day: str,
    arrivals: list[pa.Table],
    rows: list[pa.Table],
    quantiles: list[float],
) -> tuple[LatencyReport, pa.Table]:
    first: dict[str, dict[str, int]] = defaultdict(dict)
    channel_of: dict[str, str] = {}
    for ch_table in arrivals:
        ch = ch_table.column("channel")[0].as_py() if ch_table.num_rows else ""
        cols = ch_table.select([_KEY, "capture_region", "ts_local_min"]).to_pydict()
        for key, region, ts in zip(*cols.values(), strict=True):
            k = f"{ch}|{key}"
            first[k][region] = ts
            channel_of[k] = ch
    regions = sorted({r for d in first.values() for r in d})
    matched = {k: d for k, d in first.items() if len(d) == len(regions) and len(regions) > 1}
    wins: dict[str, int] = dict.fromkeys(regions, 0)
    for d in matched.values():
        wins[min(d, key=lambda r: (d[r], r))] += 1
    lead: dict[str, Distribution] = {}
    for i, r1 in enumerate(regions):
        for r2 in regions[i + 1 :]:
            diff = np.array([(d[r1] - d[r2]) / NS_PER_MS for d in matched.values()])
            lead[f"{r1}-minus-{r2}"] = distribution(diff, quantiles)
    exch: dict[str, Distribution] = {}
    for region in regions:
        parts = []
        for t in rows:
            sel = t.filter(
                pc.and_(
                    pc.equal(t.column("capture_region"), region),
                    pc.is_valid(t.column("ts_exchange")),
                )
            )
            parts.append(
                sel.column("ts_local").to_numpy().astype(np.int64)
                - sel.column("ts_exchange").to_numpy().astype(np.int64)
            )
        values = np.concatenate(parts) / NS_PER_MS if parts else np.array([])
        exch[region] = distribution(values, quantiles)
    n = len(matched)
    report = LatencyReport(
        venue=venue,
        symbol=symbol,
        date=day,
        regions=regions,
        matched_messages=n,
        win_share={r: (wins[r] / n if n else 0.0) for r in regions},
        lead_ms=lead,
        exchange_latency_ms=exch,
    )
    long_rows = [
        (channel_of[k], k.split("|", 1)[1], r, ts) for k, d in first.items() for r, ts in d.items()
    ]
    per_message = pa.table(
        {
            "channel": [x[0] for x in long_rows],
            "msg_key": [x[1] for x in long_rows],
            "capture_region": [x[2] for x in long_rows],
            "ts_local": pa.array([x[3] for x in long_rows], pa.int64()),
        }
    )
    return report, per_message


def merge_regions(
    settings: Settings, root_a: Path, root_b: Path, out_root: Path, day: str | None = None
) -> MergeResult:
    """Merge two capture trees partition by partition; see the module docstring."""
    keys = sorted(
        {
            k
            for root in (root_a, root_b)
            for k in list_partitions(root)
            if day is None or k.date == day
        }
    )
    writer = TapeWriter(
        out_root, settings.data_path(settings.paths.staging), settings.writer, clock=lambda: 0
    )
    by_instrument: dict[tuple[str, str, str], tuple[list[pa.Table], list[pa.Table]]] = defaultdict(
        lambda: ([], [])
    )
    rows_in = rows_out = 0
    for key in keys:
        a, b = read_partition(root_a, key), read_partition(root_b, key)
        rows_in += a.num_rows + b.num_rows
        merged, arrivals = merge_partition(a, b, key.channel, settings.merge.primary_region)
        rows_out += merged.num_rows
        if merged.num_rows:
            writer.write_table(key, merged)
        arrivals = arrivals.append_column(
            "channel", pa.array([str(key.channel)] * arrivals.num_rows, pa.string())
        )
        bucket = by_instrument[(key.venue, key.symbol, key.date)]
        bucket[0].append(arrivals)
        bucket[1].append(pa.concat_tables([a, b]))
    writer.close()
    reports: list[Path] = []
    latency_root = settings.data_path(settings.paths.latency)
    for (venue, symbol, d), (arrivals, rows) in sorted(by_instrument.items()):
        report, per_message = _latency_report(
            venue, symbol, d, arrivals, rows, settings.merge.latency_quantiles
        )
        base = latency_root / f"venue={venue}" / f"symbol={symbol}"
        path = base / f"date={d}.json"
        atomic_write_bytes(path, report.model_dump_json(indent=2).encode())
        arrivals_path = base / f"date={d}.arrivals.parquet"
        tmp = arrivals_path.with_name(f".{arrivals_path.name}{IN_PROGRESS_SUFFIX}")
        pq.write_table(per_message, tmp)
        tmp.replace(arrivals_path)
        reports.append(path)
    return MergeResult(len(keys), rows_in, rows_out, reports)
