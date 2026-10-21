"""Hyperliquid (on-chain perp DEX) public WebSocket (direct ``websockets`` client).

``l2Book`` pushes full top-of-book snapshots (no deltas, no sequence ids), so books are recorded as
``book_snapshot`` and audited for crossing and silence rather than sequence continuity.
``trades`` carry the two wallet addresses (``users = [buyer, seller]``), stored in the optional
``buyer``/``seller`` columns: wallet-level fills. ``activeAssetCtx`` gives funding, mark/oracle
price and open interest without a timestamp, so ``ts_exchange`` is null for those rows.
Liquidations are not published on a public channel and are therefore not recorded.
"""

from __future__ import annotations

from typing import Any

import orjson

from tape_recorder.adapters.base import (
    ParsedEvent,
    RecordFactory,
    SymbolMap,
    TransportKind,
)
from tape_recorder.frames import RestRequest
from tape_recorder.schema import Channel, Side
from tape_recorder.settings import VenueSettings
from tape_recorder.timeutil import ms_to_ns


class HyperliquidAdapter:
    transport: TransportKind = "ws"

    def __init__(self, venue: str, settings: VenueSettings) -> None:
        if settings.ws_url is None:
            raise ValueError(f"{venue}: ws_url is required")
        self.venue = venue
        self.settings = settings
        self.symbols = SymbolMap(settings.symbols)

    def ws_url(self) -> str:
        assert self.settings.ws_url is not None
        return self.settings.ws_url

    def subscribe_messages(self) -> list[str]:
        ch = self.settings.channels
        subs: list[dict[str, str]] = []
        for coin in self.symbols.natives():
            if Channel.BOOK_SNAPSHOT in ch:
                subs.append({"type": "l2Book", "coin": coin})
            if Channel.TRADES in ch:
                subs.append({"type": "trades", "coin": coin})
            if Channel.FUNDING in ch or Channel.OPEN_INTEREST in ch:
                subs.append({"type": "activeAssetCtx", "coin": coin})
        return [orjson.dumps({"method": "subscribe", "subscription": s}).decode() for s in subs]

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
        if not isinstance(msg, dict):
            return []
        channel = msg.get("channel")
        data: Any = msg.get("data")
        if channel == "l2Book":
            return self._book(data, factory)
        if channel == "trades":
            return self._trades(data, factory)
        if channel == "activeAssetCtx":
            return self._ctx(data, factory)
        return []

    def _book(self, d: dict[str, Any], f: RecordFactory) -> list[ParsedEvent]:
        cfg = self.symbols.resolve(str(d["coin"]))
        if cfg is None:
            return []
        bids_raw, asks_raw = d["levels"]
        mult = cfg.contract_size
        bids = [(float(x["px"]), float(x["sz"]) * mult) for x in bids_raw]
        asks = [(float(x["px"]), float(x["sz"]) * mult) for x in asks_raw]
        t = int(d["time"])
        snap = f.snapshot(cfg.symbol, ms_to_ns(t), None, f"{d['coin']}-{t}", bids, asks)
        return [ParsedEvent(("l2", cfg.symbol, t), snap)]

    def _trades(self, data: list[dict[str, Any]], f: RecordFactory) -> list[ParsedEvent]:
        out: list[ParsedEvent] = []
        for t in data:
            cfg = self.symbols.resolve(str(t["coin"]))
            if cfg is None:
                continue
            users = t.get("users") or [None, None]
            rec = f.trade(
                cfg.symbol,
                ms_to_ns(t["time"]),
                None,
                str(t["tid"]),
                float(t["px"]),
                float(t["sz"]) * cfg.contract_size,
                Side.BUY if t["side"] == "B" else Side.SELL,
                buyer=users[0],
                seller=users[1],
            )
            out.append(ParsedEvent(("trade", cfg.symbol, t["tid"]), (rec,)))
        return out

    def _ctx(self, d: dict[str, Any], f: RecordFactory) -> list[ParsedEvent]:
        cfg = self.symbols.resolve(str(d["coin"]))
        if cfg is None:
            return []
        ctx = d["ctx"]
        mark = float(ctx["markPx"])
        out: list[ParsedEvent] = []
        if Channel.FUNDING in self.settings.channels:
            oracle = float(ctx["oraclePx"]) if ctx.get("oraclePx") else None
            rate = float(ctx["funding"])
            rec = f.funding(cfg.symbol, None, rate, mark, oracle, None)
            out.append(ParsedEvent(("ctx-f", cfg.symbol, ctx["funding"], ctx["markPx"]), (rec,)))
        if Channel.OPEN_INTEREST in self.settings.channels:
            oi = float(ctx["openInterest"]) * cfg.contract_size
            rec_oi = f.open_interest(cfg.symbol, None, oi, oi * mark)
            out.append(ParsedEvent(("ctx-oi", cfg.symbol, ctx["openInterest"]), (rec_oi,)))
        return out
