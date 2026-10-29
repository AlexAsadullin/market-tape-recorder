"""MOEX ISS and AlgoPack REST polling (``httpx``).

One adapter serves two venues that differ only in settings:

* ``moex-algopack``: ``https://apim.moex.com`` with ``Authorization: Bearer <token>``, real time;
* ``moex-iss``: the public ``https://iss.moex.com`` without a token, delayed by 15 minutes and
  without order books or Super Candles.

ISS is request/response only, so this is a ``rest_poll`` transport. Every response is ISS JSON,
where each table is ``{metadata, columns, data}``. Per polling round the adapter asks for:

* ``trades``: all trades after the last ``TRADENO`` seen (a full page is polled again at once);
* ``orderbook``: a 20-level snapshot stamped with ``SEQNUM``;
* ``candles``: candles from the begin time of the newest candle seen, so the open candle is
  re-read until it closes (each revision is stored; I2 keeps the latest per key);
* ``marketdata``: the security's ``TRADINGSTATUS``;
* ``tradestats``, ``orderstats``, ``obstats``: the latest AlgoPack Super Candle (``latest=1``).

``poll_every`` in settings runs slow resources only every N rounds. All MOEX wall-clock times are
Moscow time and are converted to UTC ns with the configured time zone. Quantities are lots and are
converted to shares with ``contract_size`` (the lot size). Field names follow
moexalgo.github.io/docs/api (read 2026-10-29); see ``docs/moex-sources.md`` in the umbrella repo.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import orjson
import structlog

from tape_recorder.adapters.base import (
    ParsedEvent,
    RecordFactory,
    SymbolMap,
    TransportKind,
)
from tape_recorder.frames import RestRequest
from tape_recorder.schema import STATS_CHANNELS, STATS_COLUMNS, Channel, Side
from tape_recorder.settings import SymbolSettings, VenueSettings
from tape_recorder.timeutil import NS_PER_S, local_date, local_to_ns

log = structlog.get_logger(__name__)

ISS_DATETIME = "%Y-%m-%d %H:%M:%S"

# ISS candle interval codes -> tape interval labels (contract 6.2 revision 2).
CANDLE_INTERVALS: dict[str, str] = {"1": "1m", "10": "10m", "60": "1h", "24": "1d"}


def _table(doc: dict[str, Any], name: str) -> list[dict[str, Any]]:
    block = doc.get(name) or {}
    columns: list[str] = block.get("columns", [])
    return [dict(zip(columns, row, strict=True)) for row in block.get("data", [])]


def _float(value: Any) -> float | None:
    return None if value is None else float(value)


class MoexIssAdapter:
    transport: TransportKind = "rest_poll"

    def __init__(self, venue: str, settings: VenueSettings) -> None:
        if settings.rest_url is None or settings.timezone is None:
            raise ValueError(f"{venue}: rest_url and timezone are required")
        for native, sym in settings.symbols.items():
            if not (sym.engine and sym.market and sym.board):
                raise ValueError(f"{venue}/{native}: engine, market and board are required")
        if (
            Channel.CANDLES in settings.channels
            and settings.candle_interval not in CANDLE_INTERVALS
        ):
            raise ValueError(f"{venue}: candle_interval must be one of {sorted(CANDLE_INTERVALS)}")
        self.venue = venue
        self.settings = settings
        self.symbols = SymbolMap(settings.symbols)
        self.tz = settings.timezone
        self._trade_cursor: dict[str, int] = {}
        self._candle_cursor: dict[str, str] = {}
        self._backlog: set[str] = set()
        self._round = 0

    def ws_url(self) -> str:
        raise NotImplementedError("MOEX ISS is polled over REST")

    def ws_headers(self) -> tuple[tuple[str, str], ...]:
        return ()

    def ws_subprotocols(self) -> tuple[str, ...]:
        return ()

    def subscribe_messages(self) -> list[str]:
        return []

    def snapshot_requests(self) -> list[RestRequest]:
        return []

    def _headers(self) -> tuple[tuple[str, str], ...]:
        token = self.settings.token()
        if token is None:
            return ()
        return (("Authorization", f"Bearer {token.get_secret_value()}"),)

    def _request(self, tag: str, native: str, sym: SymbolSettings, **extra: str) -> RestRequest:
        assert self.settings.rest_url is not None
        path = self.settings.rest_paths[tag].format(
            engine=sym.engine, market=sym.market, board=sym.board, secid=native
        )
        params = dict(self.settings.rest_params.get(tag, {}))
        params.update(extra)
        return RestRequest(
            self.settings.rest_url + path, tag, native, tuple(params.items()), self._headers()
        )

    def history_request(self, tag: str, native: str, day: date, **extra: str) -> RestRequest:
        """A whole trade date of ``candles`` or a Super Candles channel (``tape backfill``)."""
        sym = self.settings.symbols[native]
        if tag == "candles":
            assert self.settings.candle_interval is not None
            params = {
                "interval": self.settings.candle_interval,
                "from": f"{day.isoformat()} 00:00:00",
                "till": f"{day.isoformat()} 23:59:59",
            }
        else:
            params = {"from": day.isoformat(), "till": day.isoformat(), "latest": "0"}
        params.update(extra)
        return self._request(tag, native, sym, **params)

    def _due(self, tag: str) -> bool:
        every = self.settings.poll_every.get(tag, 1)
        return self._round % every == 0

    def poll_requests(self) -> list[RestRequest]:
        channels = self.settings.channels
        out: list[RestRequest] = []
        for native, sym in self.settings.symbols.items():
            if Channel.TRADES in channels and self._due("trades"):
                cursor = self._trade_cursor.get(native)
                extra = {"tradeno": str(cursor)} if cursor is not None else {}
                out.append(self._request("trades", native, sym, **extra))
            if Channel.BOOK_SNAPSHOT in channels and self._due("orderbook"):
                out.append(self._request("orderbook", native, sym))
            if Channel.CANDLES in channels and self._due("candles"):
                assert self.settings.candle_interval is not None
                extra = {"interval": self.settings.candle_interval}
                if native in self._candle_cursor:
                    extra["from"] = self._candle_cursor[native]
                out.append(self._request("candles", native, sym, **extra))
            if Channel.TRADING_STATUS in channels and self._due("marketdata"):
                out.append(self._request("marketdata", native, sym))
            for channel in STATS_CHANNELS:
                if channel in channels and self._due(channel.value):
                    out.append(self._request(channel.value, native, sym))
        self._round += 1
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
        match request.tag:
            case "trades":
                return self._trades(request, _table(doc, "trades"), cfg, factory)
            case "orderbook":
                return self._orderbook(_table(doc, "orderbook"), cfg, factory)
            case "candles":
                return self._candles(request, _table(doc, "candles"), cfg, factory)
            case "marketdata":
                self._check_lot(_table(doc, "securities"), cfg)
                return self._status(_table(doc, "marketdata"), cfg, factory)
            case tag if tag in {c.value for c in STATS_CHANNELS}:
                return self._stats(Channel(tag), _table(doc, "data"), cfg, factory)
        return []

    def _ns(self, text: str) -> int:
        return local_to_ns(text, ISS_DATETIME, self.tz)

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
            day = r.get("TRADEDATE") or local_date(f.ts_local, self.tz).isoformat()
            session = r.get("TRADINGSESSION")
            rec = f.trade(
                cfg.symbol,
                self._ns(f"{day} {r['TRADETIME']}"),
                trade_no,
                str(trade_no),
                float(r["PRICE"]),
                float(r["QUANTITY"]) * cfg.contract_size,
                Side.BUY if r.get("BUYSELL") == "B" else Side.SELL,
                None if session is None else str(session),
            )
            out.append(ParsedEvent(("trade", cfg.symbol, trade_no), (rec,)))
            self._trade_cursor[native] = max(self._trade_cursor.get(native, 0), trade_no)
        return out

    def _orderbook(
        self, rows: list[dict[str, Any]], cfg: SymbolSettings, f: RecordFactory
    ) -> list[ParsedEvent]:
        if not rows:
            return []
        seqnum = max(int(r["SEQNUM"]) for r in rows)
        update = max(str(r["UPDATETIME"]) for r in rows)
        day = local_date(f.ts_local, self.tz).isoformat()
        ts = self._ns(f"{day} {update}")
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

    def _candles(
        self,
        request: RestRequest,
        rows: list[dict[str, Any]],
        cfg: SymbolSettings,
        f: RecordFactory,
    ) -> list[ParsedEvent]:
        interval = CANDLE_INTERVALS[dict(request.params)["interval"]]
        out: list[ParsedEvent] = []
        for r in rows:
            # ISS candle volume is in shares already (value / price = volume in the docs example).
            ts_open = self._ns(str(r["begin"]))
            # ISS "end" is the last whole second of the bar; the tape stores its last nanosecond.
            ts_close = self._ns(str(r["end"])) + NS_PER_S - 1
            close, volume = float(r["close"]), float(r["volume"])
            rec = f.candle(
                cfg.symbol,
                interval,
                ts_open,
                ts_close,
                (float(r["open"]), float(r["high"]), float(r["low"]), close),
                volume,
                _float(r.get("value")),
            )
            # The open candle changes between polls: every revision is a distinct event.
            out.append(
                ParsedEvent(("candle", cfg.symbol, interval, ts_open, close, volume), (rec,))
            )
            self._candle_cursor[request.native_symbol] = max(
                self._candle_cursor.get(request.native_symbol, ""), str(r["begin"])
            )
        return out

    def _check_lot(self, rows: list[dict[str, Any]], cfg: SymbolSettings) -> None:
        """Warn when the exchange's lot size differs from config, since sizes would be wrong."""
        for r in rows:
            lot = r.get("LOTSIZE")
            if (
                r.get("BOARDID") == cfg.board
                and lot is not None
                and float(lot) != cfg.contract_size
            ):
                log.warning(
                    "lot_size_mismatch",
                    venue=self.venue,
                    symbol=cfg.symbol,
                    configured=cfg.contract_size,
                    exchange=float(lot),
                )

    def _status(
        self, rows: list[dict[str, Any]], cfg: SymbolSettings, f: RecordFactory
    ) -> list[ParsedEvent]:
        out: list[ParsedEvent] = []
        for r in rows:
            if r.get("BOARDID") not in (None, cfg.board):
                continue
            status = r.get("TRADINGSTATUS")
            if status is None:
                continue
            time_text = r.get("UPDATETIME") or r.get("TIME")
            ts = None
            if time_text:
                day = local_date(f.ts_local, self.tz).isoformat()
                ts = self._ns(f"{day} {time_text}")
            rec = f.trading_status(cfg.symbol, ts, str(status))
            out.append(ParsedEvent(("status", cfg.symbol, str(status), ts), (rec,)))
        return out

    def _stats(
        self,
        channel: Channel,
        rows: list[dict[str, Any]],
        cfg: SymbolSettings,
        f: RecordFactory,
    ) -> list[ParsedEvent]:
        names = STATS_COLUMNS[channel]
        out: list[ParsedEvent] = []
        for r in rows:
            ts = self._ns(f"{r['tradedate']} {r['tradetime']}")
            published = r.get("SYSTIME")
            rec = f.stats_bar(
                channel,
                cfg.symbol,
                ts,
                None if published is None else self._ns(str(published)),
                {name: _float(r.get(name)) for name in names},
            )
            out.append(ParsedEvent((channel.value, cfg.symbol, ts), (rec,)))
        return out
