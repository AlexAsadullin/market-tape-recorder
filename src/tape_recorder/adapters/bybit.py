"""Bybit v5 public linear streams (direct ``websockets`` client).

Sequence ids kept for gap detection: ``orderbook.<depth>`` messages carry ``u``, the update id,
which increases by one per message on a topic. A ``snapshot`` resets the book (also sent with
``u == 1`` after a service restart); a ``delta`` continues it, so ``u`` is stored as both ``seq``
and ``first_seq`` and continuity requires ``u == previous u + 1``. Public trade ids are UUIDs, so
trades carry no sequence.
"""

from __future__ import annotations

from typing import Any

import orjson

from tape_recorder.adapters.base import (
    ParsedEvent,
    RecordFactory,
    SymbolMap,
    TransportKind,
    levels,
)
from tape_recorder.frames import RestRequest
from tape_recorder.schema import Channel, Side
from tape_recorder.settings import VenueSettings
from tape_recorder.timeutil import ms_to_ns

# Bybit accepts at most this many topics per subscribe request (protocol limit).
MAX_ARGS_PER_SUBSCRIBE = 10


class BybitAdapter:
    transport: TransportKind = "ws"

    def __init__(self, venue: str, settings: VenueSettings) -> None:
        if settings.ws_url is None or settings.book_depth is None:
            raise ValueError(f"{venue}: ws_url and book_depth are required")
        self.venue = venue
        self.settings = settings
        self.symbols = SymbolMap(settings.symbols)
        self._tickers: dict[str, dict[str, Any]] = {}

    def ws_url(self) -> str:
        assert self.settings.ws_url is not None
        return self.settings.ws_url

    def _topics(self) -> list[str]:
        ch = self.settings.channels
        out: list[str] = []
        for native in self.symbols.natives():
            if Channel.BOOK_DELTA in ch or Channel.BOOK_SNAPSHOT in ch:
                out.append(f"orderbook.{self.settings.book_depth}.{native}")
            if Channel.TRADES in ch:
                out.append(f"publicTrade.{native}")
            if Channel.FUNDING in ch or Channel.OPEN_INTEREST in ch:
                out.append(f"tickers.{native}")
            if Channel.LIQUIDATIONS in ch:
                out.append(f"allLiquidation.{native}")
        return out

    def subscribe_messages(self) -> list[str]:
        topics = self._topics()
        return [
            orjson.dumps(
                {"op": "subscribe", "args": topics[i : i + MAX_ARGS_PER_SUBSCRIBE]}
            ).decode()
            for i in range(0, len(topics), MAX_ARGS_PER_SUBSCRIBE)
        ]

    def snapshot_requests(self) -> list[RestRequest]:
        return []

    def poll_requests(self) -> list[RestRequest]:
        return []

    def backlogged(self) -> bool:
        return False

    def parse_rest(
        self, request: RestRequest, payload: str, factory: RecordFactory
    ) -> list[ParsedEvent]:
        return []

    def parse_ws(self, payload: str, factory: RecordFactory) -> list[ParsedEvent]:
        msg: Any = orjson.loads(payload)
        if not isinstance(msg, dict) or "topic" not in msg:
            return []
        kind, _, native = str(msg["topic"]).rpartition(".")
        cfg = self.symbols.resolve(native)
        if cfg is None:
            return []
        symbol, mult = cfg.symbol, cfg.contract_size
        if kind.startswith("orderbook"):
            return self._book(msg, symbol, mult, factory)
        if kind == "publicTrade":
            return [
                ParsedEvent(
                    ("trade", symbol, t["i"]),
                    (
                        factory.trade(
                            symbol,
                            ms_to_ns(t["T"]),
                            None,
                            str(t["i"]),
                            float(t["p"]),
                            float(t["v"]) * mult,
                            Side.BUY if t["S"] == "Buy" else Side.SELL,
                        ),
                    ),
                )
                for t in msg["data"]
            ]
        if kind == "tickers":
            return self._ticker(msg, symbol, mult, factory)
        if kind == "allLiquidation":
            # ``S`` is the liquidated position's side; we store the forced order's side.
            return [
                ParsedEvent(
                    ("liq", symbol, int(x["T"]), x["S"], x["v"], x["p"]),
                    (
                        factory.liquidation(
                            symbol,
                            ms_to_ns(x["T"]),
                            Side.SELL if x["S"] == "Buy" else Side.BUY,
                            float(x["p"]),
                            float(x["v"]) * mult,
                        ),
                    ),
                )
                for x in msg["data"]
            ]
        return []

    def _book(
        self, msg: dict[str, Any], symbol: str, mult: float, f: RecordFactory
    ) -> list[ParsedEvent]:
        data = msg["data"]
        update_id = int(data["u"])
        ts = ms_to_ns(msg.get("cts") or msg["ts"])
        bids, asks = levels(data.get("b", []), mult), levels(data.get("a", []), mult)
        if msg.get("type") == "snapshot":
            snap = f.snapshot(symbol, ts, update_id, f"{update_id}-{msg['ts']}", bids, asks)
            return [ParsedEvent(("snap", symbol, update_id, msg["ts"]), snap)]
        deltas = f.deltas(symbol, ts, update_id, None, update_id, bids, asks)
        return [ParsedEvent(("delta", symbol, update_id), deltas)] if deltas else []

    def _ticker(
        self, msg: dict[str, Any], symbol: str, mult: float, f: RecordFactory
    ) -> list[ParsedEvent]:
        data: dict[str, Any] = msg["data"]
        state = self._tickers.setdefault(symbol, {})
        if msg.get("type") == "snapshot":
            state.clear()
        state.update({k: v for k, v in data.items() if v not in (None, "")})
        ts_ms = int(msg["ts"])
        ts = ms_to_ns(ts_ms)
        events: list[ParsedEvent] = []
        funding_changed = "fundingRate" in data or "markPrice" in data
        funding_known = "fundingRate" in state and "markPrice" in state
        if Channel.FUNDING in self.settings.channels and funding_changed and funding_known:
            index = float(state["indexPrice"]) if "indexPrice" in state else None
            nxt = ms_to_ns(state["nextFundingTime"]) if "nextFundingTime" in state else None
            rate, mark = float(state["fundingRate"]), float(state["markPrice"])
            rec = f.funding(symbol, ts, rate, mark, index, nxt)
            events.append(ParsedEvent(("funding", symbol, ts_ms, rate, mark), (rec,)))
        if Channel.OPEN_INTEREST in self.settings.channels and "openInterest" in data:
            oi = float(state["openInterest"]) * mult
            value = float(state["openInterestValue"]) if "openInterestValue" in state else None
            rec_oi = f.open_interest(symbol, ts, oi, value)
            events.append(ParsedEvent(("oi", symbol, ts_ms, oi), (rec_oi,)))
        return events
