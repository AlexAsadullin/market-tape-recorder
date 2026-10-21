"""OKX v5 public channels (direct ``websockets`` client).

Sequence ids kept for gap detection: ``books`` messages carry ``seqId`` and ``prevSeqId``; a
snapshot has ``prevSeqId == -1`` and continuity requires ``prevSeqId == previous seqId`` (an
unchanged book repeats the same id). Trade ids are increasing but not consecutive (the channel
aggregates fills), so trades carry no sequence.

``funding-rate`` has no mark price, so the latest ``mark-price`` value is cached and joined.
Sizes are converted from contracts to base units with ``contract_size`` (OKX ``ctVal``).
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


class OkxAdapter:
    transport: TransportKind = "ws"

    def __init__(self, venue: str, settings: VenueSettings) -> None:
        if settings.ws_url is None:
            raise ValueError(f"{venue}: ws_url is required")
        self.venue = venue
        self.settings = settings
        self.symbols = SymbolMap(settings.symbols)
        self._mark: dict[str, float] = {}

    def ws_url(self) -> str:
        assert self.settings.ws_url is not None
        return self.settings.ws_url

    def subscribe_messages(self) -> list[str]:
        ch = self.settings.channels
        args: list[dict[str, str]] = []
        for native in self.symbols.natives():
            if Channel.BOOK_DELTA in ch or Channel.BOOK_SNAPSHOT in ch:
                args.append({"channel": "books", "instId": native})
            if Channel.TRADES in ch:
                args.append({"channel": "trades", "instId": native})
            if Channel.FUNDING in ch:
                args.append({"channel": "mark-price", "instId": native})
                args.append({"channel": "funding-rate", "instId": native})
            if Channel.OPEN_INTEREST in ch:
                args.append({"channel": "open-interest", "instId": native})
        if Channel.LIQUIDATIONS in ch:
            args.append({"channel": "liquidation-orders", "instType": "SWAP"})
        return [orjson.dumps({"op": "subscribe", "args": args}).decode()]

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
        if payload == "pong":
            return []
        msg: Any = orjson.loads(payload)
        if not isinstance(msg, dict) or "data" not in msg or "arg" not in msg:
            return []
        channel = msg["arg"].get("channel")
        if channel == "liquidation-orders":
            return self._liquidations(msg["data"], factory)
        cfg = self.symbols.resolve(str(msg["arg"].get("instId", "")))
        if cfg is None:
            return []
        symbol, mult = cfg.symbol, cfg.contract_size
        out: list[ParsedEvent] = []
        for d in msg["data"]:
            out.extend(self._one(channel, msg.get("action"), d, symbol, mult, factory))
        return out

    def _one(
        self,
        channel: str,
        action: str | None,
        d: dict[str, Any],
        symbol: str,
        mult: float,
        f: RecordFactory,
    ) -> list[ParsedEvent]:
        if channel == "books":
            seq, prev = int(d["seqId"]), int(d["prevSeqId"])
            ts = ms_to_ns(d["ts"])
            bids, asks = levels(d.get("bids", []), mult), levels(d.get("asks", []), mult)
            if action == "snapshot" or prev == -1:
                snap = f.snapshot(symbol, ts, seq, f"{seq}-{d['ts']}", bids, asks)
                return [ParsedEvent(("snap", symbol, seq, d["ts"]), snap)]
            deltas = f.deltas(symbol, ts, seq, prev, None, bids, asks)
            return [ParsedEvent(("delta", symbol, seq), deltas)] if deltas else []
        if channel == "trades":
            rec = f.trade(
                symbol,
                ms_to_ns(d["ts"]),
                None,
                str(d["tradeId"]),
                float(d["px"]),
                float(d["sz"]) * mult,
                Side.BUY if d["side"] == "buy" else Side.SELL,
            )
            return [ParsedEvent(("trade", symbol, d["tradeId"]), (rec,))]
        if channel == "mark-price":
            self._mark[symbol] = float(d["markPx"])
            return []
        if channel == "funding-rate":
            mark = self._mark.get(symbol)
            if mark is None:
                return []
            ts_ms = int(d.get("ts") or d["fundingTime"])
            nxt = ms_to_ns(d["fundingTime"]) if d.get("fundingTime") else None
            rate = float(d["fundingRate"])
            rec_f = f.funding(symbol, ms_to_ns(ts_ms), rate, mark, None, nxt)
            return [ParsedEvent(("funding", symbol, ts_ms, rate), (rec_f,))]
        if channel == "open-interest":
            # ``oiCcy`` is open interest in base currency, matching other venues' units.
            value = float(d["oiUsd"]) if d.get("oiUsd") else None
            rec_oi = f.open_interest(symbol, ms_to_ns(d["ts"]), float(d["oiCcy"]), value)
            return [ParsedEvent(("oi", symbol, int(d["ts"]), d["oiCcy"]), (rec_oi,))]
        return []

    def _liquidations(self, data: list[dict[str, Any]], f: RecordFactory) -> list[ParsedEvent]:
        out: list[ParsedEvent] = []
        for item in data:
            cfg = self.symbols.resolve(str(item.get("instId", "")))
            if cfg is None:
                continue
            for x in item.get("details", []):
                rec = f.liquidation(
                    cfg.symbol,
                    ms_to_ns(x["ts"]),
                    Side.BUY if x["side"] == "buy" else Side.SELL,
                    float(x["bkPx"]),
                    float(x["sz"]) * cfg.contract_size,
                )
                key = ("liq", cfg.symbol, int(x["ts"]), x["side"], x["sz"], x["bkPx"])
                out.append(ParsedEvent(key, (rec,)))
        return out
