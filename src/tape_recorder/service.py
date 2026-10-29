"""FastAPI status endpoint: health, lag and last-seen per stream, and collector load."""

from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from tape_recorder import __version__
from tape_recorder.monitor import LoadMonitor, LoadStatus, StreamMonitor, StreamStatus, VenueStatus
from tape_recorder.schema import SCHEMA_VERSION


class Health(BaseModel):
    schema_version: str = SCHEMA_VERSION
    status: str
    region: str
    version: str
    streams: int
    stale_streams: int


def create_app(monitor: StreamMonitor, region: str, load: LoadMonitor | None = None) -> FastAPI:
    app = FastAPI(title="Market Tape Recorder", version=__version__)

    @app.get("/health")
    def health() -> Health:
        streams = monitor.streams()
        stale = sum(s.stale for s in streams)
        status = "starting" if not streams else ("degraded" if stale else "ok")
        return Health(
            status=status,
            region=region,
            version=__version__,
            streams=len(streams),
            stale_streams=stale,
        )

    @app.get("/v1/streams")
    def streams() -> list[StreamStatus]:
        return monitor.streams()

    @app.get("/v1/venues")
    def venues() -> list[VenueStatus]:
        return monitor.venues()

    if load is not None:
        probe = load

        @app.get("/v1/load")
        def load_status() -> LoadStatus:
            """Collector pressure; other jobs on the host poll this and pause while it is high."""
            return probe.status()

    return app
