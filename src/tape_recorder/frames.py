"""Raw frames: what a transport delivered, before normalisation.

A frame is the unit of capture, replay and fixtures. Fixture files and the optional raw log share
one JSON shape, so anything recorded can be replayed through the same adapters offline::

    {"venue": "okx", "frames": [{"ts_local": 1761000000000000000, "connection": "c1",
                                 "source": "ws", "payload": {...} | "text"}, ...]}

``recv_ms`` may be used instead of ``ts_local`` in hand-made fixtures.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

import orjson

from tape_recorder.timeutil import NS_PER_MS

FrameSource = Literal["ws", "rest"]


@dataclass(frozen=True, slots=True)
class RestRequest:
    """A REST call an adapter wants made; ``tag`` tells the adapter how to parse the reply."""

    url: str
    tag: str
    native_symbol: str
    params: tuple[tuple[str, str], ...] = ()
    headers: tuple[tuple[str, str], ...] = field(default=(), repr=False)

    def to_json(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "tag": self.tag,
            "native_symbol": self.native_symbol,
            "params": [list(p) for p in self.params],
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> RestRequest:
        return cls(
            url=str(data["url"]),
            tag=str(data["tag"]),
            native_symbol=str(data["native_symbol"]),
            params=tuple((str(k), str(v)) for k, v in data.get("params", [])),
        )


@dataclass(frozen=True, slots=True)
class Frame:
    venue: str
    ts_local: int
    connection_id: str
    source: FrameSource
    payload: str
    request: RestRequest | None = None

    def with_(self, **changes: Any) -> Frame:
        return replace(self, **changes)

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "ts_local": self.ts_local,
            "connection": self.connection_id,
            "source": self.source,
            "payload": self.payload,
        }
        if self.request is not None:
            out["request"] = self.request.to_json()
        return out


def _payload_text(value: Any) -> str:
    return value if isinstance(value, str) else orjson.dumps(value).decode()


def frame_from_json(venue: str, data: dict[str, Any]) -> Frame:
    if "ts_local" in data:
        ts_local = int(data["ts_local"])
    else:
        recv = data["recv_ms"]
        ts_local = recv * NS_PER_MS if isinstance(recv, int) else round(float(recv) * NS_PER_MS)
    request = RestRequest.from_json(data["request"]) if data.get("request") else None
    source: FrameSource = "rest" if data.get("source") == "rest" else "ws"
    return Frame(
        venue=venue,
        ts_local=ts_local,
        connection_id=str(data.get("connection", "c1")),
        source=source,
        payload=_payload_text(data["payload"]),
        request=request,
    )


def load_frames(path: Path) -> list[Frame]:
    """Load a fixture (``.json``) or a raw log (``.jsonl``, one frame object per line)."""
    if path.suffix == ".jsonl":
        frames = []
        for line in path.read_bytes().splitlines():
            if line.strip():
                data = orjson.loads(line)
                frames.append(frame_from_json(str(data["venue"]), data))
        return frames
    doc = orjson.loads(path.read_bytes())
    venue = str(doc["venue"])
    return [frame_from_json(venue, f) for f in doc["frames"]]


def iter_frame_files(paths: Iterable[Path]) -> Iterator[Path]:
    for path in paths:
        if path.is_dir():
            yield from sorted(p for p in path.rglob("*") if p.suffix in {".json", ".jsonl"})
        else:
            yield path


def frame_to_log_line(frame: Frame) -> bytes:
    return orjson.dumps({"venue": frame.venue, **frame.to_json()}) + b"\n"
