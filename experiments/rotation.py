"""Experiment 2: the blind interval of a planned reconnect, measured on a live session.

A fake venue broadcasts Binance spot ``trade`` messages with consecutive trade ids every
``message_interval_ms`` of wall-clock time. A connection receives only the messages that the venue
emits after the connection finished its connect-and-subscribe step, which takes a random
``connect_ms`` per connection. The real :class:`tape_recorder.session.WsVenueSession` records
into a real :class:`tape_recorder.writer.TapeWriter` and rotates every ``rotate_after_s``; the
certifier then measures the blind time (holes in the union of connection spans) and the missing
trade ids. Each mode in ``modes`` is one rotation policy.
"""

from __future__ import annotations

import asyncio
import math
import random
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import orjson

from experiments.config import RotationConfig, RotationMode
from tape_recorder.adapters import build_adapter
from tape_recorder.dedup import Deduplicator
from tape_recorder.frames import RestRequest
from tape_recorder.pipeline import Pipeline
from tape_recorder.quality.certify import certify_tree
from tape_recorder.schema import Channel
from tape_recorder.session import WsVenueSession, counter_ids
from tape_recorder.settings import Settings, SymbolSettings
from tape_recorder.timeutil import NS_PER_MS, NS_PER_S, wall_clock_ns
from tape_recorder.writer import TapeWriter

VENUE = "binance"
NATIVE = "BTCUSDT"
SYMBOL = "BTC-USDT-SPOT"
FIRST_TRADE_ID = 900_000
MISSING_IDS = re.compile(r"missing ids (\d+)\.\.(\d+)")


class BroadcastVenue:
    """Emits trade ``k`` at ``start + k * interval`` regardless of who listens."""

    def __init__(self, start_ns: int, interval_ns: int) -> None:
        self.start_ns, self.interval_ns = start_ns, interval_ns

    def emit_ns(self, k: int) -> int:
        return self.start_ns + k * self.interval_ns

    def first_index_after(self, t_ns: int) -> int:
        return max(0, math.ceil((t_ns - self.start_ns) / self.interval_ns))

    def message(self, k: int) -> str:
        t_ms = self.emit_ns(k) // NS_PER_MS
        data = {"e": "trade", "E": t_ms, "s": NATIVE, "t": FIRST_TRADE_ID + k, "p": "67000.0"}
        data |= {"q": "0.01", "T": t_ms, "m": k % 2 == 0, "M": True}
        return orjson.dumps({"stream": "btcusdt@trade", "data": data}).decode()


class BroadcastConnection:
    def __init__(self, venue: BroadcastVenue) -> None:
        self.venue = venue
        self.next_k = venue.first_index_after(wall_clock_ns())
        self.closed = asyncio.Event()

    async def send(self, message: str) -> None:
        return None

    async def recv(self) -> str:
        if self.closed.is_set():
            raise ConnectionError("closed")
        delay = (self.venue.emit_ns(self.next_k) - wall_clock_ns()) / NS_PER_S
        if delay > 0:
            try:
                await asyncio.wait_for(self.closed.wait(), delay)
            except TimeoutError:
                pass
            else:
                raise ConnectionError("closed")
        k = self.next_k
        self.next_k += 1
        return self.venue.message(k)

    async def close(self) -> None:
        self.closed.set()


class BroadcastConnector:
    """Opens a connection after a random connect-and-subscribe latency."""

    def __init__(self, venue: BroadcastVenue, connect_ms: tuple[float, float], seed: str) -> None:
        self.venue, self.connect_ms, self.rng = venue, connect_ms, random.Random(seed)

    async def __call__(self, url: str) -> BroadcastConnection:
        await asyncio.sleep(self.rng.uniform(*self.connect_ms) / 1000)
        return BroadcastConnection(self.venue)


async def _no_rest(request: RestRequest) -> str:
    raise OSError("the broadcast venue has no REST endpoint")


@dataclass(frozen=True, slots=True)
class RotationRun:
    mode: str
    run: int
    rotations: int
    blind_ms_total: float
    blind_ms_max: float
    missing_trades: int
    trades_stored: int

    @property
    def blind_ms_per_rotation(self) -> float:
        return self.blind_ms_total / self.rotations if self.rotations else 0.0

    @property
    def missing_per_rotation(self) -> float:
        return self.missing_trades / self.rotations if self.rotations else 0.0


async def _record(
    settings: Settings, cfg: RotationConfig, mode: RotationMode, seed: str, root: Path
) -> None:
    venue_cfg = settings.venues[VENUE].model_copy(
        update={"channels": [Channel.TRADES], "symbols": {NATIVE: SymbolSettings(symbol=SYMBOL)}}
    )
    adapter = build_adapter(VENUE, venue_cfg)
    writer = TapeWriter(root, root / "staging", settings.writer, wall_clock_ns)
    dedup = Deduplicator(settings.dedup.window_s, settings.dedup.max_keys)
    pipeline = Pipeline({VENUE: adapter}, settings.region, writer, dedup)
    session_cfg = settings.session.model_copy(
        update={
            "redundant_connections": mode.redundant_connections,
            "overlap_rotation": mode.overlap_rotation,
            "rotate_after_s": cfg.rotate_after_s,
            "daily_rotation_utc": None,
        }
    )
    venue = BroadcastVenue(wall_clock_ns(), round(cfg.message_interval_ms * NS_PER_MS))
    session = WsVenueSession(
        adapter,
        pipeline,
        BroadcastConnector(venue, cfg.connect_ms, seed),
        _no_rest,
        session_cfg,
        wall_clock_ns,
        None,
        counter_ids(f"conn-{seed}"),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(session.run(stop))
    await asyncio.sleep(cfg.duration_s)
    stop.set()
    await task
    writer.close()


def run_rotation(settings: Settings, cfg: RotationConfig, seed: int, work: Path) -> list[RotationRun]:
    out: list[RotationRun] = []
    for name, mode in cfg.modes.items():
        for run in range(cfg.runs):
            root = work / f"rotation-{name}-{run}"
            asyncio.run(_record(settings, cfg, mode, f"{seed}-{name}-{run}", root))
            (cert,) = [c for c, _ in certify_tree(settings, root, root / "certs", venue=VENUE)]
            shutil.rmtree(root)
            missing = 0
            for gap in cert.gaps:
                m = MISSING_IDS.fullmatch(gap.detail or "") if gap.kind == "trade_id" else None
                if m is not None:
                    missing += int(m.group(2)) - int(m.group(1)) + 1
            out.append(
                RotationRun(
                    mode=name,
                    run=run,
                    rotations=cert.resubscription.connections - mode.redundant_connections,
                    blind_ms_total=cert.resubscription.gap_ms_total,
                    blind_ms_max=cert.resubscription.gap_ms_max,
                    missing_trades=missing,
                    trades_stored=cert.rows.get(str(Channel.TRADES), 0),
                )
            )
    return out
