"""Offline replay of recorded frames (fixtures or raw logs) into a tape tree.

Replay goes through exactly the same adapters, dedup and writer as a live session. The writer
clock follows the frames' arrival times, so a replay is deterministic and its output is
byte-for-byte reproducible apart from Parquet metadata.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from tape_recorder.adapters import build_adapters
from tape_recorder.dedup import Deduplicator
from tape_recorder.frames import Frame, iter_frame_files, load_frames
from tape_recorder.pipeline import Pipeline, PipelineStats
from tape_recorder.settings import Settings
from tape_recorder.writer import TapeWriter


class FrameClock:
    """Writer clock driven by replayed arrival times."""

    def __init__(self) -> None:
        self.now = 0

    def __call__(self) -> int:
        return self.now


@dataclass(frozen=True, slots=True)
class ReplayResult:
    stats: PipelineStats
    files: list[Path]
    rows: int


def merge_streams(streams: Iterable[Sequence[Frame]]) -> list[Frame]:
    """Interleave per-venue streams by arrival time (stable for equal timestamps)."""
    return list(heapq.merge(*streams, key=lambda f: f.ts_local))


def replay_frames(
    settings: Settings, frames: Sequence[Frame], region: str, out_root: Path
) -> ReplayResult:
    venues = {f.venue for f in frames}
    unknown = venues - set(settings.venues)
    if unknown:
        raise ValueError(f"frames for unconfigured venues: {sorted(unknown)}")
    adapters = build_adapters({v: settings.venues[v] for v in sorted(venues)})
    clock = FrameClock()
    writer = TapeWriter(
        out_root, settings.data_path(settings.paths.staging), settings.writer, clock
    )
    dedup = (
        Deduplicator(settings.dedup.window_s, settings.dedup.max_keys)
        if settings.dedup.enabled
        else None
    )
    pipeline = Pipeline(adapters, region, writer, dedup)
    for frame in sorted(frames, key=lambda f: f.ts_local):
        clock.now = frame.ts_local
        pipeline.process(frame)
        writer.tick()
    files = writer.close()
    return ReplayResult(pipeline.stats, files, writer.rows_written)


def replay_files(
    settings: Settings, paths: Iterable[Path], region: str, out_root: Path
) -> ReplayResult:
    streams = [load_frames(p) for p in iter_frame_files(paths)]
    return replay_frames(settings, merge_streams(streams), region, out_root)
