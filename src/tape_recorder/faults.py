"""Deterministic fault injection on frame streams, for tests and the reproduce demo.

Each fault targets the ``index``-th frame of one venue's stream:

- ``drop``: the frame is lost (a missed poll or a lost stream message).
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


def _quotation(price: float) -> dict[str, Any]:
    units = int(price)
    return {"units": str(units), "nano": round((price - units) * 1e9)}


def _cross_payload(payload: str) -> str:
    """Insert a bid above the best ask into a T-Bank or ISS order-book payload."""
    doc = orjson.loads(payload)
    book = doc.get("orderbook") if isinstance(doc, dict) else None
    if isinstance(book, dict) and isinstance(book.get("bids"), list):  # T-Bank stream message
        asks = book.get("asks") or book["bids"]
        prices = [int(lv["price"]["units"]) + int(lv["price"]["nano"]) / 1e9 for lv in asks]
        if not prices:
            raise ValueError("frame has no levels to corrupt")
        book["bids"].insert(0, {"price": _quotation(max(prices) * CROSS_MARKUP), "quantity": "1"})
        return orjson.dumps(doc).decode()
    if isinstance(book, dict) and isinstance(book.get("data"), list):  # ISS orderbook table
        cols: list[str] = book["columns"]
        side, price = cols.index("BUYSELL"), cols.index("PRICE")
        rows: list[list[Any]] = book["data"]
        if not rows:
            raise ValueError("frame has no levels to corrupt")
        top = max(float(r[price]) for r in rows)
        corrupt = list(rows[0])
        corrupt[side], corrupt[price] = "B", round(top * CROSS_MARKUP, 2)
        rows.insert(0, corrupt)
        return orjson.dumps(doc).decode()
    raise ValueError("frame has no order book to corrupt")


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
