"""``tape backfill``: historical candles (and Super Candles) into the same tape as live capture.

Live capture only starts accumulating from the day the recorder runs, while an RL agent needs
history from day one. Both sources serve history over REST:

* T-Bank ``GetCandles``: 1-minute candles, one request per instrument-day (the documented maximum
  period for minute candles);
* AlgoPack ``candles.json`` with ``from``/``till``, paged with ``start`` while a page comes back
  full; and the Super Candles ``tradestats``/``orderstats``/``obstats`` for whole days.

Responses go through the venue adapter and the normal pipeline, so backfilled rows are identical
in shape to live rows. ``ts_local`` of a backfilled row is the time it was fetched, which is the
honest knowledge time for the platform (I2 stamps it so), not the bar time.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import orjson
import structlog

from tape_recorder.adapters import VenueAdapter, build_adapter
from tape_recorder.adapters.moex import MoexIssAdapter
from tape_recorder.adapters.tbank import TBankAdapter
from tape_recorder.dedup import Deduplicator
from tape_recorder.frames import Frame, RestRequest
from tape_recorder.pipeline import Pipeline
from tape_recorder.schema import STATS_CHANNELS, Channel
from tape_recorder.settings import Settings
from tape_recorder.timeutil import Clock, datetime_to_ns
from tape_recorder.transport import TRANSPORT_ERRORS, HttpFetcher
from tape_recorder.writer import TapeWriter

log = structlog.get_logger(__name__)

BACKFILL_ERRORS: tuple[type[BaseException], ...] = (*TRANSPORT_ERRORS, ValueError)


@dataclass(slots=True)
class BackfillStats:
    requests: int = 0
    rows: int = 0
    failures: int = 0


def days(start: date, end: date) -> Iterator[date]:
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def _iss_page_rows(payload: str, table: str) -> int:
    doc = orjson.loads(payload)
    block = doc.get(table) or {}
    return len(block.get("data", []))


class Backfiller:
    """Fetches history for one venue through its adapter into a pipeline (see module doc)."""

    def __init__(
        self,
        settings: Settings,
        adapter: VenueAdapter,
        pipeline: Pipeline,
        fetcher: HttpFetcher,
        clock: Clock,
        sleep: Callable[[float], object] = asyncio.sleep,
    ) -> None:
        self.settings = settings
        self.venue = adapter.venue
        self.adapter = adapter
        self.pipeline = pipeline
        self.fetcher = fetcher
        self.clock = clock
        self.sleep = sleep
        self.stats = BackfillStats()

    async def _fetch(self, request: RestRequest) -> str | None:
        try:
            body = await self.fetcher(request)
        except BACKFILL_ERRORS as exc:  # one failed day must not stop the run
            self.stats.failures += 1
            log.warning("backfill_failed", venue=self.venue, tag=request.tag, error=repr(exc))
            return None
        self.stats.requests += 1
        events = self.pipeline.process(
            Frame(self.venue, self.clock(), f"backfill-{self.venue}", "rest", body, request)
        )
        self.stats.rows += sum(len(e.records) for e in events)
        await self._pace()
        return body

    async def _pace(self) -> None:
        interval = self.settings.backfill.min_request_interval_s
        if interval > 0:
            result = self.sleep(interval)
            if asyncio.iscoroutine(result):
                await result

    def _day_bounds(self, day: date) -> tuple[int, int]:
        tz = ZoneInfo(self.adapter.settings.timezone or "UTC")
        start = datetime.combine(day, time.min, tz)
        return datetime_to_ns(start), datetime_to_ns(start + timedelta(days=1)) - 1

    async def run(self, start: date, end: date) -> BackfillStats:
        adapter = self.adapter
        if isinstance(adapter, TBankAdapter):
            for request in adapter.resolve_requests():
                await self._fetch(request)
            for native in adapter.settings.symbols:
                for day in days(start, end):
                    lo, hi = self._day_bounds(day)
                    await self._fetch(adapter.candles_request(native, lo, hi))
        elif isinstance(adapter, MoexIssAdapter):
            await self._run_iss(adapter, start, end)
        return self.stats

    async def _run_iss(self, adapter: MoexIssAdapter, start: date, end: date) -> None:
        channels = adapter.settings.channels
        page = self.settings.backfill.iss_page_rows
        for native in adapter.settings.symbols:
            for day in days(start, end):
                if Channel.CANDLES in channels:
                    offset = 0
                    while True:
                        request = adapter.history_request("candles", native, day, start=str(offset))
                        body = await self._fetch(request)
                        n = 0 if body is None else _iss_page_rows(body, "candles")
                        if n < page:
                            break
                        offset += n
                for channel in STATS_CHANNELS:
                    if channel in channels:
                        await self._fetch(adapter.history_request(channel.value, native, day))


async def backfill(
    settings: Settings,
    venue: str,
    start: date,
    end: date,
    out_root: Path,
    fetcher: HttpFetcher,
    clock: Clock,
) -> BackfillStats:
    """Backfill ``venue`` for ``[start, end]`` (venue-local trade dates) into ``out_root``."""
    if venue not in settings.venues:
        raise ValueError(f"unknown venue {venue!r}")
    writer = TapeWriter(
        out_root, settings.data_path(settings.paths.staging), settings.writer, clock
    )
    adapter = build_adapter(venue, settings.venues[venue])
    # Dedup within the run: overlapping day ranges or repeated pages must not duplicate bars.
    window_s = 86_400.0 * ((end - start).days + 2)  # longer than the run's whole range
    dedup = Deduplicator(window_s=window_s, max_keys=settings.dedup.max_keys)
    pipeline = Pipeline({venue: adapter}, settings.region, writer, dedup)
    filler = Backfiller(settings, adapter, pipeline, fetcher, clock)
    try:
        return await filler.run(start, end)
    finally:
        writer.close()
