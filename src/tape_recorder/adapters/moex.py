"""MOEX ISS REST polling (``httpx``): trades and order book snapshots.

ISS is request/response only, so this adapter is a ``rest_poll`` transport. Trades are paged with
a ``tradeno`` cursor (``next_trade=1`` returns trades after it); when a page comes back full the
session polls again immediately. Order books are snapshots with ``SEQNUM``. ISS wall-clock times
are Moscow time and are converted to UTC ns with the configured time zone. Quantities are lots and
are converted to shares with ``contract_size`` (the lot size).

AlgoPack: when an API key is configured (``TAPE_VENUES__MOEX__API_KEY``), requests go to the
``algopack_url`` gateway with a bearer token, which serves the same ISS resources in real time.
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
from tape_recorder.settings import SymbolSettings, VenueSettings
from tape_recorder.timeutil import local_date, local_to_ns

ISS_DATETIME = "%Y-%m-%d %H:%M:%S"


def _table(doc: dict[str, Any], name: str) -> list[dict[str, Any]]:
    block = doc.get(name) or {}
    columns: list[str] = block.get("columns", [])
    return [dict(zip(columns, row, strict=True)) for row in block.get("data", [])]


class MoexIssAdapter:
    transport: TransportKind = "rest_poll"

    def __init__(self, venue: str, settings: VenueSettings) -> None:
        if settings.rest_url is None or settings.timezone is None:
            raise ValueError(f"{venue}: rest_url and timezone are required")
        for native, sym in settings.symbols.items():
            if not (sym.engine and sym.market and sym.board):
                raise ValueError(f"{venue}/{native}: engine, market and board are required")
        self.venue = venue
        self.settings = settings
        self.symbols = SymbolMap(settings.symbols)
        self.tz = settings.timezone
        self._cursor: dict[str, int] = {}
        self._backlog: set[str] = set()

    def ws_url(self) -> str:
        raise NotImplementedError("MOEX ISS is polled over REST")

    def subscribe_messages(self) -> list[str]:
        return []

    def snapshot_requests(self) -> list[RestRequest]:
        return []

    def _base_and_headers(self) -> tuple[str, tuple[tuple[str, str], ...]]:
        key = self.settings.api_key
        if key is not None and self.settings.algopack_url:
            return self.settings.algopack_url, (
                ("Authorization", f"Bearer {key.get_secret_value()}"),
            )
        assert self.settings.rest_url is not None
        return self.settings.rest_url, ()

    def _url(self, kind: str, native: str, sym: SymbolSettings) -> str:
        base, _ = self._base_and_headers()
        path = self.settings.rest_paths[kind].format(
            engine=sym.engine, market=sym.market, board=sym.board, secid=native
        )
        return base + path

    def poll_requests(self) -> list[RestRequest]:
        _, headers = self._base_and_headers()
        out: list[RestRequest] = []
        for native, sym in self.settings.symbols.items():
            if Channel.TRADES in self.settings.channels:
                params = dict(self.settings.rest_params.get("trades", {}))
                if native in self._cursor:
                    params["tradeno"] = str(self._cursor[native])
                out.append(
                    RestRequest(
                        self._url("trades", native, sym),
                        "trades",
                        native,
                        tuple(params.items()),
                        headers,
                    )
                )
            if Channel.BOOK_SNAPSHOT in self.settings.channels:
                params = dict(self.settings.rest_params.get("orderbook", {}))
                out.append(
                    RestRequest(
                        self._url("orderbook", native, sym),
                        "orderbook",
                        native,
                        tuple(params.items()),
                        headers,
                    )
                )
        return out

    def backlogged(self) -> bool:
        return bool(self._backlog)

    def parse_ws(self, payload: str, factory: RecordFactory) -> list[ParsedEvent]:
        return []

    def parse_rest(
        self, request: RestRequest, payload: str, factory: RecordFactory
    ) -> list[ParsedEvent]:
        cfg = self.symbols.resolve(request.native_symbol)
        if cfg is None:
            return []
        doc: Any = orjson.loads(payload)
        if request.tag == "trades":
            return self._trades(request, _table(doc, "trades"), cfg, factory)
        if request.tag == "orderbook":
            return self._orderbook(_table(doc, "orderbook"), cfg, factory)
        return []

    def _trades(
        self,
        request: RestRequest,
        rows: list[dict[str, Any]],
        cfg: SymbolSettings,
        f: RecordFactory,
    ) -> list[ParsedEvent]:
        native = request.native_symbol
        limit = dict(request.params).get("limit")
        if limit is not None and len(rows) >= int(limit):
            self._backlog.add(native)
        else:
            self._backlog.discard(native)
        out: list[ParsedEvent] = []
        for r in rows:
            trade_no = int(r["TRADENO"])
            stamp = r.get("SYSTIME") or f"{r['TRADEDATE']} {r['TRADETIME']}"
            rec = f.trade(
                cfg.symbol,
                local_to_ns(str(stamp), ISS_DATETIME, self.tz),
                trade_no,
                str(trade_no),
                float(r["PRICE"]),
                float(r["QUANTITY"]) * cfg.contract_size,
                Side.BUY if r.get("BUYSELL") == "B" else Side.SELL,
            )
            out.append(ParsedEvent(("trade", cfg.symbol, trade_no), (rec,)))
            self._cursor[native] = max(self._cursor.get(native, 0), trade_no)
        return out

    def _orderbook(
        self, rows: list[dict[str, Any]], cfg: SymbolSettings, f: RecordFactory
    ) -> list[ParsedEvent]:
        if not rows:
            return []
        seqnum = max(int(r["SEQNUM"]) for r in rows)
        update = max(str(r["UPDATETIME"]) for r in rows)
        day = local_date(f.ts_local, self.tz).isoformat()
        ts = local_to_ns(f"{day} {update}", ISS_DATETIME, self.tz)
        bids = [
            (float(r["PRICE"]), float(r["QUANTITY"]) * cfg.contract_size)
            for r in rows
            if r["BUYSELL"] == "B"
        ]
        asks = [
            (float(r["PRICE"]), float(r["QUANTITY"]) * cfg.contract_size)
            for r in rows
            if r["BUYSELL"] == "S"
        ]
        snap = f.snapshot(cfg.symbol, ts, seqnum, f"{cfg.board}-{seqnum}", bids, asks)
        return [ParsedEvent(("ob", cfg.symbol, seqnum), snap)]
