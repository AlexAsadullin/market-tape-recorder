"""Binance spot and USD-M perpetuals (direct ``websockets`` client, combined streams).

Sequence ids kept for gap detection:
- spot ``depthUpdate``: ``U`` (first update id) -> ``first_seq``, ``u`` (final) -> ``seq``;
  continuity requires ``U == previous u + 1``.
- USD-M ``depthUpdate``: additionally ``pu`` (previous final id) -> ``prev_seq``; continuity
  requires ``pu == previous u`` (update ids are not consecutive per symbol on futures).
- REST ``/depth`` snapshot: ``lastUpdateId`` -> ``seq``; the book is aligned by the first delta
  with ``U <= lastUpdateId + 1 <= u``.
- spot ``trade`` ids and USD-M ``aggTrade`` ids are consecutive per symbol.
"""

from __future__ import annotations

from typing import Any, Literal

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

BinanceMarket = Literal["spot", "usdm"]


class BinanceAdapter:
    transport: TransportKind = "ws"

    def __init__(self, venue: str, settings: VenueSettings, market: BinanceMarket) -> None:
        if settings.ws_url is None or settings.rest_url is None or settings.snapshot_path is None:
            raise ValueError(f"{venue}: ws_url, rest_url and snapshot_path are required")
        self.venue = venue
        self.settings = settings
        self.market = market
        self.symbols = SymbolMap(settings.symbols)

    def ws_url(self) -> str:
        assert self.settings.ws_url is not None
        return self.settings.ws_url

    def _streams(self) -> list[str]:
        ch = self.settings.channels
        out: list[str] = []
        for native in self.symbols.natives():
            n = native.lower()
            if Channel.TRADES in ch:
                out.append(f"{n}@trade" if self.market == "spot" else f"{n}@aggTrade")
            if Channel.BOOK_DELTA in ch:
                suffix = f"@{self.settings.depth_interval}" if self.settings.depth_interval else ""
                out.append(f"{n}@depth{suffix}")
            if self.market == "usdm" and Channel.FUNDING in ch:
                suffix = (
                    f"@{self.settings.mark_price_interval}"
                    if self.settings.mark_price_interval
                    else ""
                )
                out.append(f"{n}@markPrice{suffix}")
            if self.market == "usdm" and Channel.LIQUIDATIONS in ch:
                out.append(f"{n}@forceOrder")
        return out

    def subscribe_messages(self) -> list[str]:
        msg = {"method": "SUBSCRIBE", "params": self._streams(), "id": 1}
        return [orjson.dumps(msg).decode()]

    def snapshot_requests(self) -> list[RestRequest]:
        if Channel.BOOK_DELTA not in self.settings.channels:
            return []
        assert self.settings.rest_url is not None and self.settings.snapshot_path is not None
        url = self.settings.rest_url + self.settings.snapshot_path
        params: list[tuple[str, str]] = []
        if self.settings.snapshot_limit is not None:
            params.append(("limit", str(self.settings.snapshot_limit)))
        return [
            RestRequest(url, "depth", native, (("symbol", native.upper()), *params))
            for native in self.symbols.natives()
        ]

    def poll_requests(self) -> list[RestRequest]:
        return []

    def backlogged(self) -> bool:
        return False

    def parse_ws(self, payload: str, factory: RecordFactory) -> list[ParsedEvent]:
        msg: Any = orjson.loads(payload)
        if isinstance(msg, dict) and "stream" in msg and "data" in msg:
            msg = msg["data"]
        if not isinstance(msg, dict):
            return []
        handler = {
            "trade": self._trade,
            "aggTrade": self._agg_trade,
            "depthUpdate": self._depth,
            "markPriceUpdate": self._mark_price,
            "forceOrder": self._force_order,
        }.get(str(msg.get("e")))
        return handler(msg, factory) if handler is not None else []

    def _symbol(self, native: str) -> tuple[str, float] | None:
        cfg = self.symbols.resolve(native)
        return (cfg.symbol, cfg.contract_size) if cfg is not None else None

    def _trade(self, m: dict[str, Any], f: RecordFactory) -> list[ParsedEvent]:
        sym = self._symbol(m["s"])
        if sym is None:
            return []
        trade_id = int(m["t"])
        rec = f.trade(
            sym[0],
            ms_to_ns(m["T"]),
            trade_id,
            str(trade_id),
            float(m["p"]),
            float(m["q"]) * sym[1],
            Side.SELL if m["m"] else Side.BUY,
        )
        return [ParsedEvent(("trade", sym[0], trade_id), (rec,))]

    def _agg_trade(self, m: dict[str, Any], f: RecordFactory) -> list[ParsedEvent]:
        sym = self._symbol(m["s"])
        if sym is None:
            return []
        agg_id = int(m["a"])
        rec = f.trade(
            sym[0],
            ms_to_ns(m["T"]),
            agg_id,
            str(agg_id),
            float(m["p"]),
            float(m["q"]) * sym[1],
            Side.SELL if m["m"] else Side.BUY,
        )
        return [ParsedEvent(("agg", sym[0], agg_id), (rec,))]

    def _depth(self, m: dict[str, Any], f: RecordFactory) -> list[ParsedEvent]:
        sym = self._symbol(m["s"])
        if sym is None:
            return []
        final_id = int(m["u"])
        prev = int(m["pu"]) if "pu" in m else None
        ts = ms_to_ns(m["T"] if "T" in m else m["E"])
        recs = f.deltas(
            sym[0],
            ts,
            final_id,
            prev,
            int(m["U"]),
            levels(m.get("b", []), sym[1]),
            levels(m.get("a", []), sym[1]),
        )
        return [ParsedEvent(("depth", sym[0], final_id), recs)] if recs else []

    def _mark_price(self, m: dict[str, Any], f: RecordFactory) -> list[ParsedEvent]:
        sym = self._symbol(m["s"])
        if sym is None or m.get("r") in (None, ""):
            return []
        index = float(m["i"]) if m.get("i") not in (None, "") else None
        next_ts = ms_to_ns(m["T"]) if m.get("T") else None
        rec = f.funding(sym[0], ms_to_ns(m["E"]), float(m["r"]), float(m["p"]), index, next_ts)
        return [ParsedEvent(("mark", sym[0], int(m["E"])), (rec,))]

    def _force_order(self, m: dict[str, Any], f: RecordFactory) -> list[ParsedEvent]:
        o = m["o"]
        sym = self._symbol(o["s"])
        if sym is None:
            return []
        avg = float(o.get("ap") or 0.0)
        price = avg if avg > 0 else float(o["p"])
        size = float(o.get("z") or o["q"]) * sym[1]
        # Side of the forced (liquidation) order: SELL closes a long.
        rec = f.liquidation(sym[0], ms_to_ns(o["T"]), str(o["S"]).lower(), price, size)
        key = ("liq", sym[0], int(o["T"]), o["S"], o["q"], o["p"])
        return [ParsedEvent(key, (rec,))]

    def parse_rest(
        self, request: RestRequest, payload: str, factory: RecordFactory
    ) -> list[ParsedEvent]:
        if request.tag != "depth":
            return []
        sym = self._symbol(request.native_symbol)
        if sym is None:
            return []
        doc: Any = orjson.loads(payload)
        last_id = int(doc["lastUpdateId"])
        ts_ms = doc.get("T") or doc.get("E")
        recs = factory.snapshot(
            sym[0],
            ms_to_ns(ts_ms) if ts_ms else None,
            last_id,
            f"rest-{last_id}",
            levels(doc["bids"], sym[1]),
            levels(doc["asks"], sym[1]),
        )
        return [ParsedEvent(("rest-depth", sym[0], last_id), recs)]
