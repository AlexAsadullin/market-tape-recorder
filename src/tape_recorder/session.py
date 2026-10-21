"""Resilient venue sessions: reconnect with backoff, overlapping rotation, resynchronisation.

WebSocket venues run ``redundant_connections`` independent connection slots; their messages go
through one earliest-arrival dedup, so any single connection can drop without losing data.
Planned reconnects (connection age, daily rotation after midnight UTC, or a resync request) are
*overlapping*: the replacement connection is opened and must deliver data before the old one is
closed, which removes the re-subscription blind spot. A detected sequence gap triggers a REST
snapshot (venues that need one, e.g. Binance) or an overlapping reconnect (venues that send the
snapshot in-band on subscribe: Bybit, OKX).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta
from typing import Any

import structlog

from tape_recorder.adapters import VenueAdapter
from tape_recorder.frames import Frame, RestRequest
from tape_recorder.monitor import StreamMonitor
from tape_recorder.pipeline import Pipeline
from tape_recorder.schema import BookDelta, BookSnapshotLevel, Record
from tape_recorder.sequencing import Continuity, DeltaIds, SequenceTracker
from tape_recorder.settings import BackoffSettings, SessionSettings
from tape_recorder.timeutil import NS_PER_S, Clock, ns_to_datetime
from tape_recorder.transport import TRANSPORT_ERRORS, HttpFetcher, WsConnection, WsConnector

log = structlog.get_logger(__name__)

POLL_ERRORS: tuple[type[BaseException], ...] = (*TRANSPORT_ERRORS, ValueError)

Sleep = Callable[[float], Awaitable[None]]


class Backoff:
    """Exponential backoff with multiplicative jitter: ``min(max, initial * m**k) * (1 +- j)``."""

    def __init__(self, cfg: BackoffSettings, rng: random.Random) -> None:
        self._cfg = cfg
        self._rng = rng
        self._attempt = 0

    def reset(self) -> None:
        self._attempt = 0

    def next_delay(self) -> float:
        base = min(self._cfg.max_s, self._cfg.initial_s * self._cfg.multiplier**self._attempt)
        self._attempt += 1
        return base * (1 + self._rng.uniform(-self._cfg.jitter, self._cfg.jitter))


def next_daily_rotation(now_ns: int, hhmmss: str) -> int:
    """Next UTC instant at wall time ``hhmmss`` strictly after ``now_ns``."""
    now = ns_to_datetime(now_ns)
    at = time.fromisoformat(hhmmss).replace(tzinfo=UTC)
    candidate = datetime.combine(now.date(), at)
    if candidate <= now:
        candidate += timedelta(days=1)
    return int(candidate.timestamp()) * NS_PER_S


def counter_ids(prefix: str) -> Callable[[], str]:
    counter = itertools.count(1)
    return lambda: f"{prefix}-{next(counter)}"


@dataclass(eq=False)
class _Conn:
    connection_id: str
    ws: WsConnection
    opened_at: int
    first_data: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None


class WsVenueSession:
    def __init__(
        self,
        adapter: VenueAdapter,
        pipeline: Pipeline,
        connector: WsConnector,
        fetcher: HttpFetcher,
        settings: SessionSettings,
        clock: Clock,
        monitor: StreamMonitor | None,
        new_id: Callable[[], str],
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.adapter = adapter
        self.pipeline = pipeline
        self.connector = connector
        self.fetcher = fetcher
        self.settings = settings
        self.clock = clock
        self.monitor = monitor
        self.new_id = new_id
        self.sleep = sleep
        self.rng = random.Random(settings.seed)
        self.trackers: dict[str, SequenceTracker] = {}
        self.resync_requested = asyncio.Event()
        self.snapshot_lock = asyncio.Lock()
        self.rotations = 0
        self.resyncs = 0
        self.last_connection_id = ""

    async def run(self, stop: asyncio.Event) -> None:
        slots = [
            asyncio.create_task(self._slot(i, stop))
            for i in range(self.settings.redundant_connections)
        ]
        tasks = [*slots, asyncio.create_task(self._resnapshot_loop(stop))]
        try:
            await stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _open(self, backoff: Backoff, stop: asyncio.Event) -> _Conn | None:
        while not stop.is_set():
            conn_id = self.new_id()
            try:
                ws = await asyncio.wait_for(
                    self.connector(self.adapter.ws_url()), self.settings.connect_timeout_s
                )
                for msg in self.adapter.subscribe_messages():
                    await ws.send(msg)
            except TRANSPORT_ERRORS as exc:
                delay = backoff.next_delay()
                log.warning(
                    "connect_failed",
                    venue=self.adapter.venue,
                    error=repr(exc),
                    retry_in_s=round(delay, 3),
                )
                await self.sleep(delay)
                continue
            backoff.reset()
            conn = _Conn(conn_id, ws, self.clock())
            self.last_connection_id = conn_id
            conn.task = asyncio.create_task(self._read(conn))
            if self.monitor is not None:
                self.monitor.connection_opened(self.adapter.venue, conn_id)
            return conn
        return None

    async def _read(self, conn: _Conn) -> None:
        heartbeat = asyncio.create_task(self._heartbeat(conn))
        try:
            while True:
                payload = await conn.ws.recv()
                frame = Frame(self.adapter.venue, self.clock(), conn.connection_id, "ws", payload)
                events = self.pipeline.process(frame)
                if events and not conn.first_data.is_set():
                    conn.first_data.set()
                    await self._ensure_synced(conn.connection_id)
                self._track([r for e in events for r in e.records])
        except TRANSPORT_ERRORS as exc:
            log.info(
                "connection_closed",
                venue=self.adapter.venue,
                connection=conn.connection_id,
                reason=repr(exc),
            )
        finally:
            heartbeat.cancel()
            if self.monitor is not None:
                self.monitor.connection_closed(self.adapter.venue, conn.connection_id)

    async def _heartbeat(self, conn: _Conn) -> None:
        cfg = self.adapter.settings
        if cfg.heartbeat_interval_s is None or cfg.heartbeat_payload is None:
            return
        while True:
            await self.sleep(cfg.heartbeat_interval_s)
            await conn.ws.send(cfg.heartbeat_payload)

    def _track(self, events_records: Sequence[Record]) -> None:
        seen_delta: set[tuple[str, int]] = set()
        seen_snap: set[tuple[str, int]] = set()
        for r in events_records:
            if isinstance(r, BookSnapshotLevel) and r.seq is not None:
                if (r.symbol, r.seq) not in seen_snap:
                    seen_snap.add((r.symbol, r.seq))
                    self.trackers.setdefault(r.symbol, SequenceTracker()).on_snapshot(r.seq)
            elif isinstance(r, BookDelta) and r.seq is not None:
                if (r.symbol, r.seq) in seen_delta:
                    continue
                seen_delta.add((r.symbol, r.seq))
                tracker = self.trackers.setdefault(r.symbol, SequenceTracker())
                verdict = tracker.on_delta(DeltaIds(r.seq, r.prev_seq, r.first_seq))
                if verdict is Continuity.GAP:
                    log.warning("sequence_gap", venue=self.adapter.venue, symbol=r.symbol)
                    self.resync_requested.set()

    async def _ensure_synced(self, connection_id: str) -> None:
        requests = self.adapter.snapshot_requests()
        unsynced = not self.trackers or not all(t.synced for t in self.trackers.values())
        if requests and unsynced:
            await self._fetch_snapshots(requests, connection_id)

    async def _fetch_snapshots(self, requests: list[RestRequest], connection_id: str) -> None:
        async with self.snapshot_lock:
            for req in requests:
                try:
                    body = await self.fetcher(req)
                except TRANSPORT_ERRORS as exc:
                    log.warning("snapshot_failed", venue=self.adapter.venue, error=repr(exc))
                    continue
                # Stamped with the connection whose deltas this snapshot synchronises.
                frame = Frame(self.adapter.venue, self.clock(), connection_id, "rest", body, req)
                events = self.pipeline.process(frame)
                self._track([r for e in events for r in e.records])

    async def _resnapshot_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self.resync_requested.wait(), self.settings.resnapshot_interval_s
                )
            requests = self.adapter.snapshot_requests()
            if self.resync_requested.is_set():
                self.resyncs += 1
                if self.monitor is not None:
                    self.monitor.resync(self.adapter.venue)
            if requests:
                self.resync_requested.clear()
                await self._fetch_snapshots(requests, self.last_connection_id)

    def _rotation_due_at(self, conn: _Conn) -> int:
        due = conn.opened_at + round(self.settings.rotate_after_s * NS_PER_S)
        if self.settings.daily_rotation_utc is not None:
            due = min(due, next_daily_rotation(conn.opened_at, self.settings.daily_rotation_utc))
        return due

    async def _slot(self, slot: int, stop: asyncio.Event) -> None:
        backoff = Backoff(self.settings.backoff, random.Random(self.settings.seed + slot))
        current = await self._open(backoff, stop)
        while current is not None and not stop.is_set():
            assert current.task is not None
            wait_s = max(0.0, (self._rotation_due_at(current) - self.clock()) / NS_PER_S)
            in_band_resync = slot == 0 and not self.adapter.snapshot_requests()
            waiters: list[asyncio.Future[Any]] = [asyncio.ensure_future(current.task)]
            resync_wait = asyncio.ensure_future(self.resync_requested.wait())
            if in_band_resync:
                waiters.append(resync_wait)
            await asyncio.wait(waiters, timeout=wait_s, return_when=asyncio.FIRST_COMPLETED)
            resync_wait.cancel()
            if current.task.done():
                current = await self._open(backoff, stop)
                continue
            if in_band_resync and self.resync_requested.is_set():
                self.resync_requested.clear()
            current = await self._rotate(current, backoff, stop)

    async def _rotate(self, old: _Conn, backoff: Backoff, stop: asyncio.Event) -> _Conn | None:
        """Overlapping handover: the new connection must deliver data before the old closes."""
        new = await self._open(backoff, stop)
        if new is None:
            return old
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(new.first_data.wait(), self.settings.overlap_timeout_s)
        self.rotations += 1
        await self._close(old)
        return new

    async def _close(self, conn: _Conn) -> None:
        with contextlib.suppress(*TRANSPORT_ERRORS):
            await conn.ws.close()
        if conn.task is not None:
            conn.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await conn.task


class PollingVenueSession:
    """REST polling loop (MOEX ISS); a new connection id is used after every failure."""

    def __init__(
        self,
        adapter: VenueAdapter,
        pipeline: Pipeline,
        fetcher: HttpFetcher,
        settings: SessionSettings,
        clock: Clock,
        new_id: Callable[[], str],
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if adapter.settings.poll_interval_s is None:
            raise ValueError(f"{adapter.venue}: poll_interval_s is required for polling")
        self.adapter = adapter
        self.pipeline = pipeline
        self.fetcher = fetcher
        self.clock = clock
        self.new_id = new_id
        self.sleep = sleep
        self.interval = adapter.settings.poll_interval_s
        self.backoff = Backoff(settings.backoff, random.Random(settings.seed))
        self.rounds = 0
        self.failures = 0

    async def poll_once(self, connection_id: str) -> None:
        for req in self.adapter.poll_requests():
            body = await self.fetcher(req)
            self.pipeline.process(
                Frame(self.adapter.venue, self.clock(), connection_id, "rest", body, req)
            )
        self.rounds += 1

    async def run(self, stop: asyncio.Event) -> None:
        connection_id = self.new_id()
        while not stop.is_set():
            try:
                await self.poll_once(connection_id)
            except POLL_ERRORS as exc:
                self.failures += 1
                connection_id = self.new_id()
                delay = self.backoff.next_delay()
                log.warning(
                    "poll_failed",
                    venue=self.adapter.venue,
                    error=repr(exc),
                    retry_in_s=round(delay, 3),
                )
                await self.sleep(delay)
                continue
            self.backoff.reset()
            if not self.adapter.backlogged():
                await self.sleep(self.interval)
