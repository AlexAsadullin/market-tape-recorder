"""Deterministic fault injection on frame streams, for tests and the reproduce demo.

Each fault targets the ``index``-th frame of one venue's stream:

- ``drop``: the frame is lost (a sequence gap for book deltas).
- ``duplicate``: a redundant connection delivers a copy ``delay_ms`` later.
- ``reorder``: the frame is delayed past the next one, so it arrives out of order.
- ``cross``: the first bid of the frame is moved above the best ask (a corrupted, crossed book).
- ``resubscribe``: the connection is replaced; frames in the next ``gap_ms`` are never received.
"""

from __future__ import annotations

import random
from typing import Any

import orjson

from tape_recorder.frames import Frame
from tape_recorder.settings import FaultSpec
from tape_recorder.timeutil import NS_PER_MS

# How far the corrupted bid is pushed above the best ask (relative); any positive value crosses.
CROSS_MARKUP = 1.05


def _first_level_list(obj: Any, keys: tuple[str, ...]) -> list[Any] | None:
    if isinstance(obj, dict):
        for k in keys:
            if isinstance(obj.get(k), list):
                found: list[Any] = obj[k]
                return found
        children: list[Any] = list(obj.values())
    elif isinstance(obj, list):
        children = obj
    else:
        return None
    for v in children:
        sub = _first_level_list(v, keys)
        if sub is not None:
            return sub
    return None


def _cross_payload(payload: str) -> str:
    doc = orjson.loads(payload)
    data = doc.get("data", doc) if isinstance(doc, dict) else doc
    levels = data.get("levels") if isinstance(data, dict) else None
    if isinstance(levels, list) and len(levels) == 2:  # Hyperliquid l2Book
        bids, asks = levels
        ask = min(float(a["px"]) for a in asks)
        bids.insert(0, {"px": str(ask * CROSS_MARKUP), "sz": "1", "n": 1})
        return orjson.dumps(doc).decode()
    bids = _first_level_list(doc, ("b", "bids"))
    asks = _first_level_list(doc, ("a", "asks"))
    if bids is None:
        raise ValueError("frame has no bid levels to corrupt")
    ref = [float(a[0]) for a in asks or []] or [float(b[0]) for b in bids]
    if not ref:
        raise ValueError("frame has no levels to corrupt")
    bids.insert(0, [str(max(ref) * CROSS_MARKUP), "1"])
    return orjson.dumps(doc).decode()


def apply_fault(frames: list[Frame], fault: FaultSpec) -> list[Frame]:
    """Return a new frame list with ``fault`` applied (frames of one venue, arrival order)."""
    out = list(frames)
    i = fault.index
    if i >= len(out):
        raise IndexError(f"fault index {i} beyond {len(out)} frames")
    target = out[i]
    match fault.kind:
        case "drop":
            del out[i]
        case "duplicate":
            copy = target.with_(
                connection_id=f"{target.connection_id}-dup",
                ts_local=target.ts_local + round(fault.delay_ms * NS_PER_MS),
            )
            out.append(copy)
            out.sort(key=lambda f: f.ts_local)
        case "reorder":
            if i + 1 >= len(out):
                raise IndexError("reorder needs a following frame")
            nxt = out[i + 1]
            # The frame is delayed past its successor: it arrives second, with the later time.
            out[i], out[i + 1] = nxt, target.with_(ts_local=nxt.ts_local + 1)
        case "cross":
            out[i] = target.with_(payload=_cross_payload(target.payload))
        case "resubscribe":
            blind_end = target.ts_local + round(fault.gap_ms * NS_PER_MS)
            moved: list[Frame] = out[:i]
            for f in out[i:]:
                if f.connection_id != target.connection_id:
                    moved.append(f)
                elif f.ts_local >= blind_end:
                    moved.append(f.with_(connection_id=f"{f.connection_id}-r1"))
            out = moved
    return out


def as_region(
    frames: list[Frame], region: str, latency_ms: float, jitter_ms: float, rng: random.Random
) -> list[Frame]:
    """Re-stamp a stream as seen from another region: added latency, jitter, region-unique ids.

    Arrival times stay strictly increasing per connection, so jitter never reorders a stream.
    """
    last: dict[str, int] = {}
    out: list[Frame] = []
    for f in frames:
        delay = latency_ms + rng.uniform(0.0, jitter_ms)
        ts = f.ts_local + round(delay * NS_PER_MS)
        conn = f"{region}-{f.connection_id}"
        ts = max(ts, last.get(conn, ts - 1) + 1)
        last[conn] = ts
        out.append(f.with_(ts_local=ts, connection_id=conn))
    out.sort(key=lambda f: f.ts_local)
    return out
