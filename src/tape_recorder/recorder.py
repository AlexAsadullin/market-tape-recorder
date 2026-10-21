"""``tape record``: live sessions for every enabled venue, the writer and the status service."""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path

import httpx
import structlog
import uvicorn

from tape_recorder.adapters import build_adapters
from tape_recorder.dedup import Deduplicator
from tape_recorder.monitor import StreamMonitor
from tape_recorder.pipeline import Pipeline
from tape_recorder.rawlog import RawFrameLog
from tape_recorder.service import create_app
from tape_recorder.session import PollingVenueSession, WsVenueSession, counter_ids
from tape_recorder.settings import Settings
from tape_recorder.timeutil import wall_clock_ns
from tape_recorder.transport import HttpxFetcher, WebsocketsConnector
from tape_recorder.writer import TapeWriter

log = structlog.get_logger(__name__)

# How often buffered rows are checked for time-based flush and rotation.
WRITER_TICK_S = 1.0


async def record(
    settings: Settings,
    out_root: Path,
    venues: list[str] | None,
    stop: asyncio.Event,
    raw_log: bool,
) -> None:
    selected = {
        name: cfg
        for name, cfg in settings.enabled_venues().items()
        if venues is None or name in venues
    }
    if not selected:
        raise ValueError("no enabled venues selected")
    adapters = build_adapters(selected)
    monitor = StreamMonitor(wall_clock_ns, settings.service.stale_after_s)
    writer = TapeWriter(
        out_root, settings.data_path(settings.paths.staging), settings.writer, wall_clock_ns
    )
    raw = RawFrameLog(settings.data_path(settings.paths.raw), settings.region) if raw_log else None
    dedup = (
        Deduplicator(settings.dedup.window_s, settings.dedup.max_keys)
        if settings.dedup.enabled
        else None
    )
    pipeline = Pipeline(adapters, settings.region, writer, dedup, monitor, raw)
    connector = WebsocketsConnector(
        settings.session.connect_timeout_s,
        settings.session.max_message_bytes,
        settings.http.user_agent,
    )
    async with httpx.AsyncClient(
        timeout=settings.http.timeout_s, headers={"User-Agent": settings.http.user_agent}
    ) as client:
        fetcher = HttpxFetcher(client)
        tasks: list[asyncio.Task[None]] = []
        for name, adapter in adapters.items():
            ids = counter_ids(f"{settings.region}-{name}-{wall_clock_ns()}")
            if adapter.transport == "ws":
                ws = WsVenueSession(
                    adapter,
                    pipeline,
                    connector,
                    fetcher,
                    settings.session,
                    wall_clock_ns,
                    monitor,
                    ids,
                )
                tasks.append(asyncio.create_task(ws.run(stop)))
            else:
                poll = PollingVenueSession(
                    adapter, pipeline, fetcher, settings.session, wall_clock_ns, ids
                )
                tasks.append(asyncio.create_task(poll.run(stop)))
        server: uvicorn.Server | None = None
        if settings.service.enabled:
            config = uvicorn.Config(
                create_app(monitor, settings.region),
                host=settings.service.host,
                port=settings.service.port,
                log_level="warning",
            )
            server = uvicorn.Server(config)
            tasks.append(asyncio.create_task(server.serve()))
        log.info("recording", venues=sorted(adapters), out=str(out_root), region=settings.region)
        try:
            while not stop.is_set():
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), WRITER_TICK_S)
                writer.tick()
        finally:
            if server is not None:
                server.should_exit = True
            await asyncio.gather(*tasks, return_exceptions=True)
            writer.close()
            if raw is not None:
                raw.close()
            log.info(
                "stopped",
                rows=writer.rows_written,
                parts=len(writer.finalised),
                duplicates=pipeline.stats.duplicates,
                parse_errors=pipeline.stats.parse_errors,
            )
