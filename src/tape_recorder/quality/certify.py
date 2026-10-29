"""Quality certificate per instrument-day (contract 6.2).

The certifier reads one venue/symbol/date from a tape tree and measures:

1. Sequence gaps in book deltas, with the per-venue rule of :mod:`tape_recorder.sequencing`.
   Deltas are replayed in venue-sequence order (not arrival order), so a tape merged from two
   regions with different latencies reconstructs correctly; arrival order is audited separately.
2. Out-of-order arrival: on one connection, a message whose sequence id is below one already seen.
3. Duplicates: the same venue event (delta ``seq`` or ``trade_id``) stored more than once.
4. Crossed (best bid > best ask) and locked (best bid == best ask) books after each applied message.
5. Snapshot vs reconstruction: when a snapshot with id S arrives while the book is in sync at S,
   its top N levels must equal the book rebuilt from deltas.
6. Re-subscription gaps: the union of per-connection activity spans ``[first, last]``; every hole
   in that union is time when no connection delivered anything.
7. Clock skew: the distribution of ``ts_local - ts_exchange`` over event channels only.
8. Coverage: the share of the expected window with live, in-sync data.
"""

from __future__ import annotations

import itertools
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from tape_recorder.book import OrderBook, levels_match
from tape_recorder.quality.certificate import (
    BookStats,
    Certificate,
    Check,
    ConnectionSpan,
    Gap,
    Resubscription,
    SkewStats,
    Status,
    write_certificate,
)
from tape_recorder.quality.intervals import Interval, clip, holes, runs, subtract, total, union
from tape_recorder.quality.reader import instrument_days, read_partition
from tape_recorder.schema import BookSide, Channel
from tape_recorder.sequencing import Continuity, DeltaIds, classify
from tape_recorder.settings import QualitySettings, Settings, VenueSettings
from tape_recorder.timeutil import NS_PER_MS, NS_PER_S, date_bounds_ns, window_bounds_ns
from tape_recorder.writer import PartitionKey

Level = tuple[float, float]

# Channels whose ts_exchange is the time of the event itself. Candles and Super Candles carry a
# bar time, which lags arrival by up to the bar length, so they would distort clock skew.
SKEW_CHANNELS = frozenset(
    {Channel.TRADES, Channel.BOOK_DELTA, Channel.BOOK_SNAPSHOT, Channel.TRADING_STATUS}
)


@dataclass(slots=True)
class DeltaMessage:
    seq: int
    prev_seq: int | None
    first_seq: int | None
    ts_local: int
    connection_id: str
    levels: list[tuple[str, float, float]]


@dataclass(slots=True)
class Snapshot:
    seq: int | None
    snapshot_id: str
    ts_local: int
    connection_id: str
    bids: list[Level]
    asks: list[Level]


@dataclass(slots=True)
class BookResult:
    stats: BookStats = field(default_factory=BookStats)
    gaps: list[Gap] = field(default_factory=list)
    unsynced: list[Interval] = field(default_factory=list)
    sequence_gaps: int = 0
    out_of_order: int = 0
    duplicates: int = 0


@dataclass(slots=True)
class TradeResult:
    gaps: list[Gap] = field(default_factory=list)
    gap_count: int = 0
    out_of_order: int = 0
    duplicates: int = 0


def delta_messages(table: pa.Table) -> list[DeltaMessage]:
    """Group consecutive delta rows that share (connection, seq, ts_local) into messages."""
    cols = table.select(
        ["seq", "prev_seq", "first_seq", "ts_local", "connection_id", "side", "price", "size"]
    ).to_pydict()
    out: list[DeltaMessage] = []
    cur: DeltaMessage | None = None
    for seq, prev, first, ts, conn, side, price, size in zip(*cols.values(), strict=True):
        if seq is None:
            continue
        if cur is None or (cur.seq, cur.ts_local, cur.connection_id) != (seq, ts, conn):
            cur = DeltaMessage(seq, prev, first, ts, conn, [])
            out.append(cur)
        cur.levels.append((side, price, size))
    return out


def snapshots(table: pa.Table) -> list[Snapshot]:
    cols = table.select(
        ["seq", "snapshot_id", "ts_local", "connection_id", "side", "price", "size", "level"]
    ).to_pydict()
    by_id: dict[tuple[str, str, int], Snapshot] = {}
    for seq, sid, ts, conn, side, price, size, _level in zip(*cols.values(), strict=True):
        key = (sid, conn, ts)
        snap = by_id.get(key)
        if snap is None:
            snap = by_id[key] = Snapshot(seq, sid, ts, conn, [], [])
        (snap.bids if side == BookSide.BID else snap.asks).append((price, size))
    for snap in by_id.values():
        snap.bids.sort(key=lambda lv: -lv[0])
        snap.asks.sort(key=lambda lv: lv[0])
    return sorted(by_id.values(), key=lambda s: s.ts_local)


def _check_touch(book: OrderBook, stats: BookStats) -> None:
    if book.is_crossed():
        stats.crossed_events += 1
    elif book.is_locked():
        stats.locked_events += 1


def _compare(book: OrderBook, snap: Snapshot, q: QualitySettings) -> bool:
    depth = q.book_compare_depth
    bids, asks = book.top(depth)
    want_b, want_a = snap.bids[:depth], snap.asks[:depth]
    return levels_match(
        bids[: len(want_b)], want_b, q.price_tolerance, q.size_rel_tolerance
    ) and levels_match(asks[: len(want_a)], want_a, q.price_tolerance, q.size_rel_tolerance)


def analyse_book(
    deltas: Sequence[DeltaMessage], snaps: Sequence[Snapshot], sequenced: bool, q: QualitySettings
) -> BookResult:
    """Reconstruct the book and audit it; see the module docstring for each measure."""
    res = BookResult()
    st = res.stats
    st.delta_messages, st.snapshots = len(deltas), len(snaps)
    book = OrderBook()
    if not sequenced:
        for snap in snaps:
            book.load(snap.bids, snap.asks)
            _check_touch(book, st)
        return res

    max_seen: dict[str, int] = {}
    for m in deltas:
        if m.seq < max_seen.get(m.connection_id, m.seq):
            res.out_of_order += 1
        max_seen[m.connection_id] = max(max_seen.get(m.connection_id, m.seq), m.seq)
    res.duplicates = sum(c - 1 for c in Counter(m.seq for m in deltas).values())

    events: list[tuple[int, int, int, DeltaMessage | Snapshot]] = [
        (m.seq, 0, m.ts_local, m) for m in deltas
    ]
    events.extend((s.seq, 1, s.ts_local, s) for s in snaps if s.seq is not None)
    events.sort(key=lambda e: (e[0], e[1], e[2]))

    last: int | None = None
    last_ts: int | None = None
    gap_start: int | None = None
    first_delta_ts = min((m.ts_local for m in deltas), default=None)
    first_sync_ts: int | None = None
    for _, _, _, ev in events:
        if isinstance(ev, Snapshot):
            assert ev.seq is not None
            if last is None:
                book.load(ev.bids, ev.asks)
                _check_touch(book, st)
                last, last_ts = ev.seq, ev.ts_local
                if first_sync_ts is None:
                    first_sync_ts = ev.ts_local
                if gap_start is not None:
                    end = max(gap_start, ev.ts_local)
                    res.unsynced.append((gap_start, end))
                    res.gaps.append(
                        Gap(
                            start_ns=gap_start,
                            end_ns=end,
                            kind="sequence",
                            channel=Channel.BOOK_DELTA,
                            detail=f"resync at {ev.seq}",
                        )
                    )
                    gap_start = None
            elif last == ev.seq:
                st.snapshots_compared += 1
                if not _compare(book, ev, q):
                    st.snapshot_mismatches += 1
            else:
                st.snapshots_uncomparable += 1
            continue
        if last is None:
            continue
        verdict = classify(last, DeltaIds(ev.seq, ev.prev_seq, ev.first_seq))
        if verdict is Continuity.STALE:
            st.stale_messages += 1
        elif verdict is Continuity.CONTIGUOUS:
            for side, price, size in ev.levels:
                book.apply(side, price, size)
            _check_touch(book, st)
            last, last_ts = ev.seq, ev.ts_local
        else:
            res.sequence_gaps += 1
            gap_start = last_ts if last_ts is not None else ev.ts_local
            last = None
            book.clear()
    if gap_start is not None:
        end = max([gap_start, *(m.ts_local for m in deltas)])
        res.unsynced.append((gap_start, end))
        res.gaps.append(
            Gap(
                start_ns=gap_start,
                end_ns=end,
                kind="sequence",
                channel=Channel.BOOK_DELTA,
                detail="never resynchronised",
            )
        )
    if first_delta_ts is not None:
        sync = first_sync_ts if first_sync_ts is not None else max(m.ts_local for m in deltas)
        if sync > first_delta_ts:
            res.unsynced.append((first_delta_ts, sync))
            res.gaps.append(
                Gap(
                    start_ns=first_delta_ts,
                    end_ns=sync,
                    kind="initial_sync",
                    channel=Channel.BOOK_DELTA,
                )
            )
    st.unsynced_ms = total(union(res.unsynced)) / NS_PER_MS
    return res


def analyse_trades(table: pa.Table, consecutive: bool) -> TradeResult:
    res = TradeResult()
    if table.num_rows == 0:
        return res
    cols = table.select(["trade_id", "ts_local", "connection_id"]).to_pydict()
    ids: list[str] = cols["trade_id"]
    res.duplicates = len(ids) - len(set(ids))
    if not consecutive:
        return res
    nums = [int(x) for x in ids]
    max_seen: dict[str, int] = {}
    for n, conn in zip(nums, cols["connection_id"], strict=True):
        if n < max_seen.get(conn, n):
            res.out_of_order += 1
        max_seen[conn] = max(max_seen.get(conn, n), n)
    first_ts: dict[int, int] = {}
    for n, ts in zip(nums, cols["ts_local"], strict=True):
        first_ts[n] = min(first_ts.get(n, ts), ts)
    ordered = sorted(first_ts)
    for a, b in itertools.pairwise(ordered):
        if b - a > 1:
            res.gap_count += 1
            res.gaps.append(
                Gap(
                    start_ns=min(first_ts[a], first_ts[b]),
                    end_ns=max(first_ts[a], first_ts[b]),
                    kind="trade_id",
                    channel=Channel.TRADES,
                    detail=f"missing ids {a + 1}..{b - 1}",
                )
            )
    return res


def skew_stats(skew_ns: np.ndarray) -> SkewStats | None:
    if skew_ns.size == 0:
        return None
    ms = skew_ns.astype(np.float64) / NS_PER_MS
    p50, p90, p99 = np.quantile(ms, [0.5, 0.9, 0.99])
    return SkewStats(
        n=int(ms.size),
        mean_ms=float(ms.mean()),
        std_ms=float(ms.std(ddof=0)),
        min_ms=float(ms.min()),
        p50_ms=float(p50),
        p90_ms=float(p90),
        p99_ms=float(p99),
        max_ms=float(ms.max()),
        negative_ratio=float((ms < 0).mean()),
    )


def _connections(tables: dict[Channel, pa.Table]) -> list[ConnectionSpan]:
    spans: dict[str, list[int | str]] = {}
    for t in tables.values():
        if t.num_rows == 0:
            continue
        cols = t.select(["connection_id", "capture_region", "ts_local"]).to_pydict()
        for conn, region, ts in zip(*cols.values(), strict=True):
            s = spans.get(conn)
            if s is None:
                spans[conn] = [region, ts, ts, 1]
            else:
                s[1], s[2], s[3] = min(int(s[1]), ts), max(int(s[2]), ts), int(s[3]) + 1
    return sorted(
        (
            ConnectionSpan(
                connection_id=c,
                capture_region=str(s[0]),
                first_ns=int(s[1]),
                last_ns=int(s[2]),
                rows=int(s[3]),
            )
            for c, s in spans.items()
        ),
        key=lambda c: (c.first_ns, c.connection_id),
    )


def _resubscription(spans: list[ConnectionSpan]) -> tuple[Resubscription, list[Gap]]:
    merged = union((s.first_ns, s.last_ns) for s in spans)
    hs = holes(merged)
    gaps = [Gap(start_ns=a, end_ns=b, kind="resubscription") for a, b in hs]
    overlaps = [
        (prev.last_ns - nxt.first_ns) / NS_PER_MS for prev, nxt in itertools.pairwise(spans)
    ]
    return (
        Resubscription(
            connections=len(spans),
            handovers=max(len(spans) - 1, 0),
            gap_ms_max=max((b - a for a, b in hs), default=0) / NS_PER_MS,
            gap_ms_total=total(hs) / NS_PER_MS,
            overlap_ms_min=min(overlaps) if overlaps else None,
        ),
        gaps,
    )


def _window(
    day: date, venue_cfg: VenueSettings | None, q: QualitySettings, all_ts: list[int]
) -> Interval:
    if q.coverage_window == "observed":
        return (all_ts[0], all_ts[-1]) if all_ts else date_bounds_ns(day)
    if venue_cfg is not None and venue_cfg.session_window is not None:
        w = venue_cfg.session_window
        return window_bounds_ns(day, w.start, w.end, w.timezone)
    return date_bounds_ns(day)


def _evaluate(
    values: dict[str, float], q: QualitySettings, venue: str
) -> tuple[Status, list[Check]]:
    checks: list[Check] = []
    status: Status = "certified"
    for name, spec in q.checks_for(venue).items():
        value = values.get(name, 0.0)
        passed = value <= spec.threshold if spec.direction == "max" else value >= spec.threshold
        checks.append(
            Check(
                name=name,
                passed=passed,
                value=value,
                threshold=spec.threshold,
                direction=spec.direction,
                severity=spec.severity,
            )
        )
        if not passed:
            status = "failed" if spec.severity == "hard" or status == "failed" else "degraded"
    return status, checks


def certify_instrument_day(
    tables: dict[Channel, pa.Table],
    venue: str,
    symbol: str,
    day: date,
    venue_cfg: VenueSettings | None,
    q: QualitySettings,
) -> Certificate:
    """Build the certificate for one instrument-day from its per-channel tables."""
    empty = pa.table({})
    deltas_t = tables.get(Channel.BOOK_DELTA, empty)
    snaps_t = tables.get(Channel.BOOK_SNAPSHOT, empty)
    trades_t = tables.get(Channel.TRADES, empty)
    sequenced = venue_cfg.book_sequenced if venue_cfg is not None else True
    consecutive = venue_cfg.trade_ids_consecutive if venue_cfg is not None else False

    book = analyse_book(
        delta_messages(deltas_t) if deltas_t.num_rows else [],
        snapshots(snaps_t) if snaps_t.num_rows else [],
        sequenced,
        q,
    )
    trades = analyse_trades(trades_t, consecutive)
    spans = _connections(tables)
    resub, resub_gaps = _resubscription(spans)

    gaps: list[Gap] = [*book.gaps, *trades.gaps, *resub_gaps]
    for channel, limit_s in q.silence_s.items():
        t = tables.get(channel)
        if t is None or t.num_rows == 0:
            continue
        ts_sorted = sorted(set(t.column("ts_local").to_pylist()))
        limit = round(limit_s * NS_PER_S)
        gaps.extend(
            Gap(start_ns=a, end_ns=b, kind="silence", channel=channel)
            for a, b in itertools.pairwise(ts_sorted)
            if b - a > limit
        )

    all_ts = sorted(
        {ts for t in tables.values() if t.num_rows for ts in t.column("ts_local").to_pylist()}
    )
    window = _window(day, venue_cfg, q, all_ts)
    poll = venue_cfg.poll_interval_s if venue_cfg is not None and venue_cfg.poll_interval_s else 0.0
    alive_gap = round(max(q.alive_gap_s, 2 * poll) * NS_PER_S)
    alive = union(clip(runs(all_ts, alive_gap), window))
    lost = union([*book.unsynced, *((g.start_ns, g.end_ns) for g in trades.gaps)])
    covered = subtract(alive, lost)
    span = window[1] - window[0]
    coverage = total(covered) / span if span > 0 else (1.0 if all_ts else 0.0)

    skew_parts: dict[str, list[np.ndarray]] = {}
    for channel, t in tables.items():
        if t.num_rows == 0 or channel not in SKEW_CHANNELS:
            continue
        valid = pc.is_valid(t.column("ts_exchange"))
        ex = pc.filter(t.column("ts_exchange"), valid).to_numpy().astype(np.int64)
        loc = pc.filter(t.column("ts_local"), valid).to_numpy().astype(np.int64)
        reg = np.asarray(pc.filter(t.column("capture_region"), valid).to_pylist())
        diff = loc - ex
        for region in np.unique(reg):
            skew_parts.setdefault(str(region), []).append(diff[reg == region])
    by_region = {
        r: s for r, parts in skew_parts.items() if (s := skew_stats(np.concatenate(parts)))
    }
    all_skew = [p for parts in skew_parts.values() for p in parts]
    skew = skew_stats(np.concatenate(all_skew)) if all_skew else None

    rows = {str(c): t.num_rows for c, t in tables.items() if t.num_rows}
    values = {
        "messages": float(sum(rows.values())),
        "sequence_gaps": float(book.sequence_gaps),
        "trade_id_gaps": float(trades.gap_count),
        "out_of_order_messages": float(book.out_of_order + trades.out_of_order),
        "duplicate_messages": float(book.duplicates + trades.duplicates),
        "crossed_book_events": float(book.stats.crossed_events),
        "locked_book_events": float(book.stats.locked_events),
        "snapshot_mismatches": float(book.stats.snapshot_mismatches),
        "resubscription_gap_ms_max": resub.gap_ms_max,
        "clock_skew_p50_abs_ms": abs(skew.p50_ms) if skew else 0.0,
        "clock_skew_negative_ratio": skew.negative_ratio if skew else 0.0,
        "coverage_ratio": coverage,
        "coverage_ratio_floor": coverage,
    }
    status, checks = _evaluate(values, q, venue)
    return Certificate(
        venue=venue,
        symbol=symbol,
        date=day.isoformat(),
        status=status,
        checks=checks,
        gaps=sorted(gaps, key=lambda g: (g.start_ns, g.kind)),
        coverage_ratio=coverage,
        generated_at=datetime.now(UTC).isoformat(),
        window_start_ns=window[0],
        window_end_ns=window[1],
        capture_regions=sorted({s.capture_region for s in spans}),
        rows=rows,
        connections=spans,
        resubscription=resub,
        book=book.stats,
        clock_skew=skew,
        clock_skew_by_region=by_region,
    )


def load_instrument_day(root: Path, venue: str, symbol: str, day: str) -> dict[Channel, pa.Table]:
    return {
        ch: t
        for ch in Channel
        if (t := read_partition(root, PartitionKey(venue, ch, symbol, day))).num_rows
    }


def certify_tree(
    settings: Settings,
    tape_root: Path,
    cert_root: Path,
    day: date | None = None,
    venue: str | None = None,
    symbol: str | None = None,
) -> list[tuple[Certificate, Path]]:
    """Certify every instrument-day found under ``tape_root`` and write the JSON files."""
    out: list[tuple[Certificate, Path]] = []
    for v, s, d in instrument_days(tape_root, day, venue, symbol):
        tables = load_instrument_day(tape_root, v, s, d)
        cert = certify_instrument_day(
            tables, v, s, date.fromisoformat(d), settings.venues.get(v), settings.quality
        )
        out.append((cert, write_certificate(cert_root, cert)))
    return out
