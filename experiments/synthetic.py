"""Synthetic L2 streams in real venue wire formats, for the report experiments.

The generating process: a book with ``levels`` price levels per side around a mid price; every
message changes one to three levels (a size update, a new level next to the touch, or a deletion),
and the book is kept uncrossed. Update ids follow each venue's documented rule (Binance spot
``U``/``u``, Bybit consecutive ``u``, OKX ``seqId``/``prevSeqId``). A full snapshot is inserted
every ``snapshot_every`` messages (a REST snapshot for Binance, an in-band snapshot for Bybit and
OKX), which stands in for the resynchronisation that a live session performs after a gap.
Messages are spaced by a fixed interval or by exponential inter-arrival times, and every frame
arrives ``latency_ms`` plus a uniform jitter after its exchange timestamp.

The data are not market data and serve only to measure software behaviour.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

import orjson
from pydantic import BaseModel, ConfigDict, Field

from tape_recorder.frames import Frame, RestRequest
from tape_recorder.timeutil import NS_PER_MS

Level = tuple[float, float]

T0_MS = 1_792_497_600_000  # 2026-10-20T12:00:00Z, the fixture day
MID = 67000.0
TICK = 0.1
LEVELS = 20
BINANCE_SNAPSHOT_URL = "https://api.binance.com/api/v3/depth"


class StreamSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    messages: int = Field(gt=0)
    interval_ms: float = Field(gt=0)
    interval_kind: Literal["fixed", "exponential"]
    snapshot_every: int = Field(gt=0)
    latency_ms: float = Field(ge=0)
    jitter_ms: float = Field(ge=0)


@dataclass
class SyntheticBook:
    rng: random.Random
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for i in range(LEVELS):
            self.bids[round(MID - TICK * (i + 1), 1)] = self._size()
            self.asks[round(MID + TICK * (i + 1), 1)] = self._size()

    def _size(self) -> float:
        return round(self.rng.uniform(0.01, 5.0), 3)

    def step(self) -> tuple[list[Level], list[Level]]:
        bids: list[Level] = []
        asks: list[Level] = []
        for _ in range(self.rng.randint(1, 3)):
            is_bid = self.rng.random() < 0.5
            book, out = (self.bids, bids) if is_bid else (self.asks, asks)
            r = self.rng.random()
            if r < 0.25 and len(book) > LEVELS // 2:
                price = self.rng.choice(list(book))
                del book[price]
                out.append((price, 0.0))
                continue
            if r < 0.45:
                best_bid, best_ask = max(self.bids), min(self.asks)
                if best_ask - best_bid > 2 * TICK:
                    price = round(best_bid + TICK if is_bid else best_ask - TICK, 1)
                else:
                    price = round(best_bid - TICK * 3 if is_bid else best_ask + TICK * 3, 1)
            else:
                price = self.rng.choice(list(book))
            size = self._size()
            book[price] = size
            out.append((price, size))
        return bids, asks

    def snapshot(self) -> tuple[list[Level], list[Level]]:
        return sorted(self.bids.items(), reverse=True), sorted(self.asks.items())


def _fmt(levels: list[Level]) -> list[list[str]]:
    return [[f"{p:.1f}", f"{s:.3f}"] for p, s in levels]


def _fmt_okx(levels: list[Level]) -> list[list[str]]:
    return [[f"{p:.1f}", f"{s:.3f}", "0", "1"] for p, s in levels]


@dataclass(frozen=True, slots=True)
class Encoded:
    payload: str
    request: RestRequest | None = None


class _Binance:
    """Spot ``depthUpdate`` with ``U = previous u + 1``; snapshots come from REST ``/depth``."""

    def __init__(self, rng: random.Random) -> None:
        self.rng, self.last = rng, 1_000_000

    def delta(self, t_ms: int, b: list[Level], a: list[Level]) -> Encoded:
        first = self.last + 1
        self.last = first + self.rng.randint(0, 3)
        data = {"e": "depthUpdate", "E": t_ms, "s": "BTCUSDT", "U": first, "u": self.last}
        data |= {"b": _fmt(b), "a": _fmt(a)}
        return Encoded(orjson.dumps({"stream": "btcusdt@depth@100ms", "data": data}).decode())

    def snapshot(self, t_ms: int, b: list[Level], a: list[Level]) -> Encoded:
        body = {"lastUpdateId": self.last, "bids": _fmt(b), "asks": _fmt(a)}
        req = RestRequest(
            BINANCE_SNAPSHOT_URL, "depth", "BTCUSDT", (("symbol", "BTCUSDT"), ("limit", "1000"))
        )
        return Encoded(orjson.dumps(body).decode(), req)


class _Bybit:
    """``orderbook.50`` with ``u`` increasing by one per message; snapshots are in-band."""

    TOPIC = "orderbook.50.BTCUSDT"

    def __init__(self, rng: random.Random) -> None:
        self.u = 5_000_000

    def _msg(self, kind: str, t_ms: int, b: list[Level], a: list[Level]) -> Encoded:
        data = {"s": "BTCUSDT", "b": _fmt(b), "a": _fmt(a), "u": self.u, "seq": self.u}
        doc = {"topic": self.TOPIC, "type": kind, "ts": t_ms, "data": data, "cts": t_ms}
        return Encoded(orjson.dumps(doc).decode())

    def delta(self, t_ms: int, b: list[Level], a: list[Level]) -> Encoded:
        self.u += 1
        return self._msg("delta", t_ms, b, a)

    def snapshot(self, t_ms: int, b: list[Level], a: list[Level]) -> Encoded:
        return self._msg("snapshot", t_ms, b, a)


class _Okx:
    """``books`` with ``prevSeqId == previous seqId``; snapshots carry ``prevSeqId = -1``."""

    ARG: ClassVar[dict[str, str]] = {"channel": "books", "instId": "BTC-USDT-SWAP"}

    def __init__(self, rng: random.Random) -> None:
        self.rng, self.seq = rng, 9_000_000

    def _msg(self, action: str, t_ms: int, prev: int, b: list[Level], a: list[Level]) -> Encoded:
        data = {"asks": _fmt_okx(a), "bids": _fmt_okx(b), "ts": str(t_ms), "checksum": 0}
        data |= {"prevSeqId": prev, "seqId": self.seq}
        return Encoded(orjson.dumps({"arg": self.ARG, "action": action, "data": [data]}).decode())

    def delta(self, t_ms: int, b: list[Level], a: list[Level]) -> Encoded:
        prev = self.seq
        self.seq += self.rng.randint(1, 3)
        return self._msg("update", t_ms, prev, b, a)

    def snapshot(self, t_ms: int, b: list[Level], a: list[Level]) -> Encoded:
        return self._msg("snapshot", t_ms, -1, b, a)


ENCODERS: dict[str, Callable[[random.Random], Any]] = {
    "binance": _Binance,
    "bybit": _Bybit,
    "okx": _Okx,
}


def venue_frames(venue: str, spec: StreamSpec, seed: int, connection: str = "c1") -> list[Frame]:
    """One connection's frames for ``venue``: an opening snapshot, deltas, periodic snapshots."""
    rng = random.Random(f"{venue}-{seed}")
    book = SyntheticBook(rng)
    enc = ENCODERS[venue](rng)
    frames: list[Frame] = []
    t_ms = float(T0_MS)

    def emit(e: Encoded) -> None:
        arrival_ms = t_ms + spec.latency_ms + rng.uniform(0.0, spec.jitter_ms)
        ts = round(arrival_ms * NS_PER_MS)
        if frames:
            ts = max(ts, frames[-1].ts_local + 1)
        source: Literal["ws", "rest"] = "rest" if e.request is not None else "ws"
        frames.append(Frame(venue, ts, connection, source, e.payload, e.request))

    emit(enc.snapshot(round(t_ms), *book.snapshot()))
    for i in range(1, spec.messages + 1):
        gap = spec.interval_ms
        if spec.interval_kind == "exponential":
            gap = rng.expovariate(1.0 / spec.interval_ms)
        t_ms += gap
        emit(enc.delta(round(t_ms), *book.step()))
        if i % spec.snapshot_every == 0:
            emit(enc.snapshot(round(t_ms), *book.snapshot()))
    return frames


def is_delta(frame: Frame) -> bool:
    """True for book-delta frames (the frames that faults may target)."""
    if frame.source != "ws":
        return False
    p = frame.payload
    return '"depthUpdate"' in p or '"type":"delta"' in p or '"action":"update"' in p
