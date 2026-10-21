"""Optional raw frame log: one JSON line per received frame, replayable with ``tape replay``."""

from __future__ import annotations

from pathlib import Path
from typing import BinaryIO

from tape_recorder.frames import Frame, frame_to_log_line
from tape_recorder.timeutil import ns_to_date


class RawFrameLog:
    """Appends frames to ``<root>/venue=<v>/date=<d>/<region>.jsonl`` (line-buffered)."""

    def __init__(self, root: Path, region: str) -> None:
        self.root = root
        self.region = region
        self._files: dict[tuple[str, str], BinaryIO] = {}

    def write_frame(self, frame: Frame) -> None:
        key = (frame.venue, ns_to_date(frame.ts_local).isoformat())
        fh = self._files.get(key)
        if fh is None:
            path = self.root / f"venue={key[0]}" / f"date={key[1]}" / f"{self.region}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            fh = self._files[key] = path.open("ab")
        fh.write(frame_to_log_line(frame))
        fh.flush()

    def close(self) -> None:
        for fh in self._files.values():
            fh.close()
        self._files.clear()
