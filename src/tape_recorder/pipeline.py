"""Frame -> adapter -> dedup -> sink: the path shared by live sessions and offline replay."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

import orjson
import structlog

from tape_recorder.adapters import ParsedEvent, RecordFactory, VenueAdapter
from tape_recorder.dedup import Deduplicator
from tape_recorder.frames import Frame
from tape_recorder.monitor import StreamMonitor
from tape_recorder.schema import Record

log = structlog.get_logger(__name__)


class RecordSink(Protocol):
    def write(self, records: Sequence[Record]) -> None: ...


class FrameSink(Protocol):
    def write_frame(self, frame: Frame) -> None: ...


@dataclass(slots=True)
class PipelineStats:
    frames: int = 0
    events: int = 0
    records: int = 0
    duplicates: int = 0
    parse_errors: int = 0


class Pipeline:
    def __init__(
        self,
        adapters: Mapping[str, VenueAdapter],
        capture_region: str,
        sink: RecordSink,
        dedup: Deduplicator | None,
        monitor: StreamMonitor | None = None,
        raw_log: FrameSink | None = None,
    ) -> None:
        self.adapters = adapters
        self.capture_region = capture_region
        self.sink = sink
        self.dedup = dedup
        self.monitor = monitor
        self.raw_log = raw_log
        self.stats = PipelineStats()
        # Events parsed from the last frame before dedup: a connection whose every event is a
        # duplicate is still delivering data (used for overlapping handovers).
        self.last_parsed = 0

    def _parse(self, frame: Frame) -> list[ParsedEvent]:
        adapter = self.adapters[frame.venue]
        factory = RecordFactory(
            frame.venue, self.capture_region, frame.connection_id, frame.ts_local
        )
        if frame.source == "rest":
            if frame.request is None:
                raise ValueError("REST frame without its request")
            return adapter.parse_rest(frame.request, frame.payload, factory)
        return adapter.parse_ws(frame.payload, factory)

    def process(self, frame: Frame) -> list[ParsedEvent]:
        """Normalise one frame; returns the events kept after dedup (already written)."""
        self.stats.frames += 1
        self.last_parsed = 0
        if self.raw_log is not None:
            self.raw_log.write_frame(frame)
        try:
            events = self._parse(frame)
        except (KeyError, ValueError, TypeError, IndexError, orjson.JSONDecodeError) as exc:
            self.stats.parse_errors += 1
            log.warning(
                "parse_error", venue=frame.venue, error=repr(exc), payload=frame.payload[:200]
            )
            return []
        self.last_parsed = len(events)
        kept: list[ParsedEvent] = []
        for event in events:
            if (
                self.dedup is not None
                and event.key is not None
                and not self.dedup.first_arrival((frame.venue, event.key), frame.ts_local)
            ):
                self.stats.duplicates += 1
                continue
            kept.append(event)
        records = [r for e in kept for r in e.records]
        if records:
            self.sink.write(records)
            if self.monitor is not None:
                self.monitor.observe(records)
        self.stats.events += len(kept)
        self.stats.records += len(records)
        return kept
