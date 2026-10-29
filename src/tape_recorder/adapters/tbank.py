"""T-Bank Invest API: the market-data WebSocket stream, plus REST for resolution and history.

Stream: ``MarketDataStreamService/MarketDataStream`` over the JSON WebSocket proxy, with the
bearer token in the handshake and the ``json`` (camelCase) or ``json-proto`` subprotocol. One
subscribe message per data type is sent after connect: order books (``depth`` levels, every
message is a full snapshot, not a delta), anonymous trades, candles and trading status.

The stream messages carry ``figi`` and ``instrumentUid`` but not always the ticker, so each
configured symbol is resolved once through ``InstrumentsService/ShareBy`` (``resolve_requests``
and ``apply_resolution``); the same answer reports the lot size and price tick, and a mismatch
with config is logged, because sizes in the tape would otherwise be wrong.

Quantities are lots and are converted to shares with the lot size; prices are ``Quotation``
objects (``units + nano / 1e9``). T-Bank trades have no trade id or sequence number, so the dedup
key of a trade is its full content (time to the microsecond, price, quantity and direction), and
the certificate checks staleness and crossed books instead of sequence gaps.

REST history (``GetCandles``, ``GetLastTrades``, ``GetOrderBook``) is a ``POST`` with a JSON body
to ``{rest_url}{rest_service_prefix}.<Service>/<Method>``; the responses are parsed by
``parse_rest`` with the same record builders as the stream. Message shapes follow the protobuf
contracts in ``docs/tinvest-contracts`` of the umbrella repo (read 2026-10-29).
"""

from __future__ import annotations

import re
from typing import Any

import orjson
import structlog

from tape_recorder.adapters.base import Level, ParsedEvent, RecordFactory, SymbolMap, TransportKind
from tape_recorder.frames import RestRequest
from tape_recorder.schema import Channel, Side
from tape_recorder.settings import SymbolSettings, VenueSettings
from tape_recorder.timeutil import NS_PER_S, ns_to_iso, rfc3339_to_ns

log = structlog.get_logger(__name__)

STREAM_INTERVALS: dict[str, tuple[str, int]] = {
    "SUBSCRIPTION_INTERVAL_ONE_MINUTE": ("1m", 60),
    "SUBSCRIPTION_INTERVAL_FIVE_MINUTES": ("5m", 300),
    "SUBSCRIPTION_INTERVAL_TEN_MINUTES": ("10m", 600),
    "SUBSCRIPTION_INTERVAL_10_MIN": ("10m", 600),
    "SUBSCRIPTION_INTERVAL_ONE_HOUR": ("1h", 3600),
    "SUBSCRIPTION_INTERVAL_ONE_DAY": ("1d", 86_400),
}
HISTORY_INTERVALS: dict[str, tuple[str, int]] = {
    "CANDLE_INTERVAL_1_MIN": ("1m", 60),
    "CANDLE_INTERVAL_5_MIN": ("5m", 300),
    "CANDLE_INTERVAL_10_MIN": ("10m", 600),
    "CANDLE_INTERVAL_HOUR": ("1h", 3600),
    "CANDLE_INTERVAL_DAY": ("1d", 86_400),
}
SUBSCRIBE = "SUBSCRIPTION_ACTION_SUBSCRIBE"
SUCCESS = "SUBSCRIPTION_STATUS_SUCCESS"

_CAMEL = re.compile(r"(?<!^)(?=[A-Z])")


def snake_keys(value: Any) -> Any:
    """Convert camelCase keys to snake_case recursively, so ``json`` and ``json-proto`` agree."""
    if isinstance(value, dict):
        return {_CAMEL.sub("_", k).lower(): snake_keys(v) for k, v in value.items()}
    if isinstance(value, list):
        return [snake_keys(v) for v in value]
    return value


def quotation(q: dict[str, Any] | None) -> float | None:
    """``Quotation``/``MoneyValue`` to float; both parts carry the sign."""
    if not q:
        return None
    return int(q.get("units", 0) or 0) + int(q.get("nano", 0) or 0) / 1e9


class TBankAdapter:
    transport: TransportKind = "ws"

    def __init__(self, venue: str, settings: VenueSettings) -> None:
        if settings.ws_url is None or settings.rest_url is None:
            raise ValueError(f"{venue}: ws_url and rest_url are required")
        if settings.rest_service_prefix is None:
            raise ValueError(f"{venue}: rest_service_prefix is required")
        for native, sym in settings.symbols.items():
            if not sym.board:
                raise ValueError(f"{venue}/{native}: board (the T-Bank class code) is required")
        self.venue = venue
        self.settings = settings
        self.symbols = SymbolMap(settings.symbols)
        self._by_key: dict[str, SymbolSettings] = {}
        self._lot: dict[str, float] = {}
        for native, sym in settings.symbols.items():
            self._lot[sym.symbol] = sym.contract_size
            self._by_key[f"{native}_{sym.board}".upper()] = sym
            if sym.instrument_uid:
                self._by_key[sym.instrument_uid] = sym

    # -- connection -----------------------------------------------------------------------------

    def ws_url(self) -> str:
        assert self.settings.ws_url is not None
        return self.settings.ws_url

    def _auth(self) -> tuple[tuple[str, str], ...]:
        token = self.settings.token()
        if token is None:
            return ()
        return (("Authorization", f"Bearer {token.get_secret_value()}"),)

    def ws_headers(self) -> tuple[tuple[str, str], ...]:
        return self._auth()

    def ws_subprotocols(self) -> tuple[str, ...]:
        return (self.settings.ws_subprotocol,) if self.settings.ws_subprotocol else ()

    def _instrument_id(self, native: str, sym: SymbolSettings) -> str:
        return sym.instrument_id or f"{native}_{sym.board}"

    def subscribe_messages(self) -> list[str]:
        channels = self.settings.channels
        ids = [self._instrument_id(n, s) for n, s in self.settings.symbols.items()]
        messages: list[dict[str, Any]] = []
        if Channel.BOOK_SNAPSHOT in channels:
            depth = self.settings.book_depth or 20
            messages.append(
                {
                    "subscribeOrderBookRequest": {
                        "subscriptionAction": SUBSCRIBE,
                        "instruments": [{"instrumentId": i, "depth": depth} for i in ids],
                    }
                }
            )
        if Channel.TRADES in channels:
            messages.append(
                {
                    "subscribeTradesRequest": {
                        "subscriptionAction": SUBSCRIBE,
                        "instruments": [{"instrumentId": i} for i in ids],
                    }
                }
            )
        if Channel.CANDLES in channels:
            interval = self.settings.candle_interval or "SUBSCRIPTION_INTERVAL_ONE_MINUTE"
            messages.append(
                {
                    "subscribeCandlesRequest": {
                        "subscriptionAction": SUBSCRIBE,
                        "instruments": [{"instrumentId": i, "interval": interval} for i in ids],
                        "waitingClose": False,
                    }
                }
            )
        if Channel.TRADING_STATUS in channels:
            messages.append(
                {
                    "subscribeInfoRequest": {
                        "subscriptionAction": SUBSCRIBE,
                        "instruments": [{"instrumentId": i} for i in ids],
                    }
                }
            )
        return [orjson.dumps(m).decode() for m in messages]

    def snapshot_requests(self) -> list[RestRequest]:
        # Every stream order-book message is a full snapshot, so no REST resync is needed.
        return []

    def poll_requests(self) -> list[RestRequest]:
        return []

    def backlogged(self) -> bool:
        return False

    # -- REST: resolution and history ---------------------------------------------------------

    def _rest(self, method: str, tag: str, native: str, body: dict[str, Any]) -> RestRequest:
        assert self.settings.rest_url is not None and self.settings.rest_service_prefix
        url = f"{self.settings.rest_url}{self.settings.rest_service_prefix}.{method}"
        return RestRequest(
            url,
            tag,
            native,
            headers=self._auth(),
            method="POST",
            body=orjson.dumps(body).decode(),
        )

    def resolve_requests(self) -> list[RestRequest]:
        """``ShareBy`` per symbol; run once before streaming, so uid-only messages can be mapped.

        The answers go through :meth:`parse_rest` (tag ``share``) like any REST frame, so the raw
        log and offline replays carry the resolution too.
        """
        return [
            self._rest(
                "InstrumentsService/ShareBy",
                "share",
                native,
                {"idType": "INSTRUMENT_ID_TYPE_TICKER", "classCode": sym.board, "id": native},
            )
            for native, sym in self.settings.symbols.items()
        ]

    def apply_resolution(self, request: RestRequest, payload: str) -> None:
        sym = self.symbols.resolve(request.native_symbol)
        doc = snake_keys(orjson.loads(payload)).get("instrument") or {}
        if sym is None or not doc:
            return
        for key in (doc.get("uid"), doc.get("figi")):
            if key:
                self._by_key[str(key)] = sym
        lot = doc.get("lot")
        if lot is not None and float(lot) != sym.contract_size:
            log.warning(
                "lot_size_mismatch",
                venue=self.venue,
                symbol=sym.symbol,
                configured=sym.contract_size,
                exchange=float(lot),
            )
            self._lot[sym.symbol] = float(lot)
        tick = quotation(doc.get("min_price_increment"))
        if tick is not None and sym.tick is not None and abs(tick - sym.tick) > 1e-12:
            log.warning("tick_mismatch", venue=self.venue, symbol=sym.symbol, exchange=tick)

    def candles_request(
        self, native: str, start_ns: int, end_ns: int, interval: str = "CANDLE_INTERVAL_1_MIN"
    ) -> RestRequest:
        sym = self.settings.symbols[native]
        return self._rest(
            "MarketDataService/GetCandles",
            "candles",
            native,
            {
                "instrumentId": self._instrument_id(native, sym),
                "from": ns_to_iso(start_ns),
                "to": ns_to_iso(end_ns),
                "interval": interval,
            },
        )

    def last_trades_request(self, native: str, start_ns: int, end_ns: int) -> RestRequest:
        sym = self.settings.symbols[native]
        return self._rest(
            "MarketDataService/GetLastTrades",
            "last_trades",
            native,
            {
                "instrumentId": self._instrument_id(native, sym),
                "from": ns_to_iso(start_ns),
                "to": ns_to_iso(end_ns),
                "tradeSource": "TRADE_SOURCE_EXCHANGE",
            },
        )

    def orderbook_request(self, native: str) -> RestRequest:
        sym = self.settings.symbols[native]
        return self._rest(
            "MarketDataService/GetOrderBook",
            "orderbook",
            native,
            {"instrumentId": self._instrument_id(native, sym), "depth": self.settings.book_depth},
        )

    # -- parsing ------------------------------------------------------------------------------

    def _symbol_of(self, msg: dict[str, Any]) -> SymbolSettings | None:
        for key in (
            msg.get("instrument_uid"),
            msg.get("figi"),
            f"{msg.get('ticker')}_{msg.get('class_code')}".upper() if msg.get("ticker") else None,
        ):
            if key and key in self._by_key:
                return self._by_key[key]
        return None

    def _levels(self, raw: list[dict[str, Any]], lot: float) -> list[Level]:
        out: list[Level] = []
        for lv in raw:
            price = quotation(lv.get("price"))
            if price is not None:
                out.append((price, float(lv.get("quantity", 0)) * lot))
        return out

    def _orderbook(
        self, msg: dict[str, Any], sym: SymbolSettings, f: RecordFactory, ts_key: str
    ) -> list[ParsedEvent]:
        ts = rfc3339_to_ns(msg[ts_key]) if msg.get(ts_key) else None
        lot = self._lot[sym.symbol]
        bids = self._levels(msg.get("bids", []), lot)
        asks = self._levels(msg.get("asks", []), lot)
        snap_id = f"{sym.board}-{ts if ts is not None else f.ts_local}"
        rows = f.snapshot(sym.symbol, ts, None, snap_id, bids, asks)
        if msg.get("is_consistent") is False:
            log.warning("inconsistent_orderbook", venue=self.venue, symbol=sym.symbol)
        return [ParsedEvent(("ob", sym.symbol, ts), rows)]

    def _trade(self, msg: dict[str, Any], sym: SymbolSettings, f: RecordFactory) -> ParsedEvent:
        ts = rfc3339_to_ns(msg["time"])
        price = quotation(msg.get("price")) or 0.0
        lots = int(msg.get("quantity", 0))
        direction = str(msg.get("direction", ""))
        side = Side.BUY if direction == "TRADE_DIRECTION_BUY" else Side.SELL
        trade_id = f"{ts}-{price}-{lots}-{side}"
        rec = f.trade(sym.symbol, ts, None, trade_id, price, lots * self._lot[sym.symbol], side)
        return ParsedEvent(("trade", sym.symbol, trade_id), (rec,))

    def _candle(
        self,
        msg: dict[str, Any],
        sym: SymbolSettings,
        f: RecordFactory,
        interval: tuple[str, int],
    ) -> ParsedEvent:
        label, seconds = interval
        ts_open = rfc3339_to_ns(msg["time"])
        ohlc = (
            quotation(msg.get("open")) or 0.0,
            quotation(msg.get("high")) or 0.0,
            quotation(msg.get("low")) or 0.0,
            quotation(msg.get("close")) or 0.0,
        )
        volume = float(msg.get("volume", 0)) * self._lot[sym.symbol]
        rec = f.candle(
            sym.symbol, label, ts_open, ts_open + seconds * NS_PER_S - 1, ohlc, volume, None
        )
        return ParsedEvent(("candle", sym.symbol, label, ts_open, ohlc[3], volume), (rec,))

    def parse_ws(self, payload: str, factory: RecordFactory) -> list[ParsedEvent]:
        doc = snake_keys(orjson.loads(payload))
        if not isinstance(doc, dict):
            return []
        for key, body in doc.items():
            if key.endswith("_response") and isinstance(body, dict):
                self._check_subscription(key, body)
        if (msg := doc.get("orderbook")) and (sym := self._symbol_of(msg)):
            return self._orderbook(msg, sym, factory, "time")
        if (msg := doc.get("trade")) and (sym := self._symbol_of(msg)):
            return [self._trade(msg, sym, factory)]
        if (msg := doc.get("candle")) and (sym := self._symbol_of(msg)):
            interval = STREAM_INTERVALS.get(str(msg.get("interval")))
            return [] if interval is None else [self._candle(msg, sym, factory, interval)]
        if (msg := doc.get("trading_status")) and (sym := self._symbol_of(msg)):
            ts = rfc3339_to_ns(msg["time"]) if msg.get("time") else None
            status = str(msg.get("trading_status", ""))
            rec = factory.trading_status(
                sym.symbol,
                ts,
                status,
                msg.get("limit_order_available_flag"),
                msg.get("market_order_available_flag"),
            )
            return [ParsedEvent(("status", sym.symbol, status, ts), (rec,))]
        return []

    def _check_subscription(self, kind: str, body: dict[str, Any]) -> None:
        for items in body.values():
            if not isinstance(items, list):
                continue
            for item in items:
                status = item.get("subscription_status")
                if status is not None and status != SUCCESS:
                    log.warning(
                        "subscription_failed",
                        venue=self.venue,
                        kind=kind,
                        status=status,
                        instrument=item.get("instrument_uid") or item.get("figi"),
                    )

    def parse_rest(
        self, request: RestRequest, payload: str, factory: RecordFactory
    ) -> list[ParsedEvent]:
        if request.tag == "share":
            self.apply_resolution(request, payload)
            return []
        sym = self.symbols.resolve(request.native_symbol)
        if sym is None:
            return []
        doc = snake_keys(orjson.loads(payload))
        match request.tag:
            case "orderbook":
                return self._orderbook(doc, sym, factory, "orderbook_ts")
            case "last_trades":
                return [self._trade(t, sym, factory) for t in doc.get("trades", [])]
            case "candles":
                body = snake_keys(orjson.loads(request.body or "{}"))
                interval = HISTORY_INTERVALS.get(str(body.get("interval")))
                if interval is None:
                    return []
                return [
                    self._candle(c, sym, factory, interval)
                    for c in doc.get("candles", [])
                    if c.get("is_complete", True)
                ]
        return []
