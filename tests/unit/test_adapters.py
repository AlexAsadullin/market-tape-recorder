from __future__ import annotations

from collections.abc import Callable
from datetime import date

import orjson
import pytest

from tape_recorder.adapters import ParsedEvent, RecordFactory, VenueAdapter, build_adapter
from tape_recorder.adapters.moex import MoexIssAdapter
from tape_recorder.adapters.tbank import TBankAdapter, quotation, snake_keys
from tape_recorder.frames import Frame
from tape_recorder.schema import (
    BookSnapshotLevel,
    Candle,
    Channel,
    Record,
    StatsBar,
    Trade,
    TradingStatus,
)
from tape_recorder.settings import Settings
from tape_recorder.timeutil import NS_PER_S, rfc3339_to_ns

Frames = Callable[[str], list[Frame]]


def parse_all(adapter: VenueAdapter, frames: list[Frame]) -> list[ParsedEvent]:
    out: list[ParsedEvent] = []
    for f in frames:
        factory = RecordFactory(adapter.venue, "msk", f.connection_id, f.ts_local)
        if f.source == "rest":
            assert f.request is not None
            out.extend(adapter.parse_rest(f.request, f.payload, factory))
        else:
            out.extend(adapter.parse_ws(f.payload, factory))
    return out


def records[R: Record](events: list[ParsedEvent], kind: type[R]) -> list[R]:
    return [r for e in events for r in e.records if isinstance(r, kind)]


def adapter_for(settings: Settings, venue: str) -> VenueAdapter:
    return build_adapter(venue, settings.venues[venue])


# -- T-Bank -----------------------------------------------------------------------------------


def test_quotation_and_keys() -> None:
    assert quotation({"units": "178", "nano": 970000000}) == pytest.approx(178.97)
    assert quotation({"units": "-1", "nano": -500000000}) == pytest.approx(-1.5)
    assert quotation(None) is None
    assert snake_keys({"orderBookType": 1, "bids": [{"lastTradeTs": 2}]}) == {
        "order_book_type": 1,
        "bids": [{"last_trade_ts": 2}],
    }


def test_rfc3339_keeps_nanoseconds() -> None:
    assert rfc3339_to_ns("1970-01-01T00:00:01.000000123Z") == NS_PER_S + 123
    assert rfc3339_to_ns("1970-01-01T00:00:02Z") == 2 * NS_PER_S


def test_tbank_subscriptions_and_auth(make_settings: Callable[..., Settings]) -> None:
    s = make_settings(TBANK_TOKEN="secret-token")
    a = build_adapter("moex-tbank", s.venues["moex-tbank"])
    assert a.ws_headers() == (("Authorization", "Bearer secret-token"),)
    assert a.ws_subprotocols() == ("json",)
    assert a.ws_url().endswith("MarketDataStreamService/MarketDataStream")
    msgs = [orjson.loads(m) for m in a.subscribe_messages()]
    kinds = [next(iter(m)) for m in msgs]
    assert kinds == [
        "subscribeOrderBookRequest",
        "subscribeTradesRequest",
        "subscribeCandlesRequest",
        "subscribeInfoRequest",
    ]
    book = msgs[0]["subscribeOrderBookRequest"]
    assert book["subscriptionAction"] == "SUBSCRIPTION_ACTION_SUBSCRIBE"
    assert {"instrumentId": "SBER_TQBR", "depth": 20} in book["instruments"]
    assert "secret-token" not in "".join(a.subscribe_messages())


def test_tbank_stream_needs_resolution_for_uid_only_messages(
    settings: Settings, venue_frames: Frames
) -> None:
    frames = venue_frames("moex-tbank")
    a = adapter_for(settings, "moex-tbank")
    # Without the ShareBy answer the uid-only messages cannot be mapped to a symbol.
    assert parse_all(a, frames[1:]) == []
    events = parse_all(adapter_for(settings, "moex-tbank"), frames)
    assert records(events, BookSnapshotLevel)


def test_tbank_parses_every_stream_message(settings: Settings, venue_frames: Frames) -> None:
    frames = venue_frames("moex-tbank")
    events = parse_all(adapter_for(settings, "moex-tbank"), frames)
    snaps = records(events, BookSnapshotLevel)
    first_book = [s for s in snaps if s.snapshot_id == snaps[0].snapshot_id]
    bids = sorted((s for s in first_book if s.side == "bid"), key=lambda s: s.level)
    asks = sorted((s for s in first_book if s.side == "ask"), key=lambda s: s.level)
    assert bids[0].price < asks[0].price
    assert bids[0].price == pytest.approx(285.39) and bids[0].size == 10  # SBER lot is 1
    assert snaps[0].seq is None and snaps[0].symbol == "SBER-RUB-TQBR"
    trades = records(events, Trade)
    assert [t.side for t in trades] == ["buy", "sell", "buy"]
    assert trades[0].size == 32 and trades[0].ts_exchange == rfc3339_to_ns(
        "2026-10-20T12:00:00.250000Z"
    )
    (candle,) = records(events, Candle)
    assert candle.interval == "1m" and candle.volume == 4279
    assert candle.ts_close - candle.ts_open == 60 * NS_PER_S - 1
    (status,) = records(events, TradingStatus)
    assert status.status == "SECURITY_TRADING_STATUS_NORMAL_TRADING"
    assert status.limit_order_allowed is True


def test_tbank_lot_size_from_resolution_wins(settings: Settings, venue_frames: Frames) -> None:
    frames = venue_frames("moex-tbank")
    share = orjson.loads(frames[0].payload)
    share["instrument"]["lot"] = 10
    patched = [frames[0].with_(payload=orjson.dumps(share).decode()), *frames[1:]]
    events = parse_all(adapter_for(settings, "moex-tbank"), patched)
    assert records(events, Trade)[0].size == 320  # 32 lots of 10 shares


def test_tbank_duplicate_trade_has_equal_key(settings: Settings, venue_frames: Frames) -> None:
    frames = venue_frames("moex-tbank")
    a = adapter_for(settings, "moex-tbank")
    parse_all(a, frames[:1])
    trade = next(f for f in frames if "trade" in orjson.loads(f.payload))
    one = a.parse_ws(trade.payload, RecordFactory("moex-tbank", "msk", "c1", 1))
    two = a.parse_ws(trade.payload, RecordFactory("moex-tbank", "msk", "c2", 2))
    assert one[0].key == two[0].key


def test_tbank_snake_case_messages(settings: Settings, venue_frames: Frames) -> None:
    frames = venue_frames("moex-tbank")
    a = adapter_for(settings, "moex-tbank")
    parse_all(a, frames[:1])
    book = next(f for f in frames if "orderbook" in orjson.loads(f.payload))
    snake = orjson.dumps(snake_keys(orjson.loads(book.payload))).decode()
    assert a.parse_ws(snake, RecordFactory("moex-tbank", "msk", "c1", 1))


def test_tbank_subscription_failure_is_not_data(settings: Settings) -> None:
    a = adapter_for(settings, "moex-tbank")
    msg = {
        "subscribeTradesResponse": {
            "trackingId": "x",
            "tradeSubscriptions": [
                {"subscriptionStatus": "SUBSCRIPTION_STATUS_INSTRUMENT_NOT_FOUND"}
            ],
        }
    }
    assert a.parse_ws(orjson.dumps(msg).decode(), RecordFactory("v", "r", "c", 1)) == []
    assert (
        a.parse_ws('{"ping": {"time": "2026-10-20T12:00:00Z"}}', RecordFactory("v", "r", "c", 1))
        == []
    )


def test_tbank_history_requests_and_parsing(settings: Settings) -> None:
    a = adapter_for(settings, "moex-tbank")
    assert isinstance(a, TBankAdapter)
    req = a.candles_request("SBER", 0, NS_PER_S)
    assert req.method == "POST"
    assert req.url.endswith("tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles")
    body = orjson.loads(req.body or "{}")
    assert body["instrumentId"] == "SBER_TQBR" and body["interval"] == "CANDLE_INTERVAL_1_MIN"
    reply = {
        "candles": [
            {
                "open": {"units": "285", "nano": 300000000},
                "high": {"units": "285", "nano": 450000000},
                "low": {"units": "285", "nano": 280000000},
                "close": {"units": "285", "nano": 390000000},
                "volume": "100",
                "time": "2026-10-20T12:00:00Z",
                "isComplete": True,
            },
            {"close": {"units": "1"}, "time": "2026-10-20T12:01:00Z", "isComplete": False},
        ]
    }
    events = a.parse_rest(req, orjson.dumps(reply).decode(), RecordFactory("v", "r", "c", 1))
    (candle,) = records(events, Candle)
    assert candle.close == pytest.approx(285.39) and candle.volume == 100
    book_req = a.orderbook_request("SBER")
    book = {
        "bids": [{"price": {"units": "285", "nano": 0}, "quantity": "3"}],
        "asks": [{"price": {"units": "285", "nano": 10000000}, "quantity": "4"}],
        "orderbookTs": "2026-10-20T12:00:00Z",
    }
    snaps = records(
        a.parse_rest(book_req, orjson.dumps(book).decode(), RecordFactory("v", "r", "c", 1)),
        BookSnapshotLevel,
    )
    assert len(snaps) == 2
    trades_req = a.last_trades_request("SBER", 0, NS_PER_S)
    reply_t = {
        "trades": [
            {
                "direction": "TRADE_DIRECTION_SELL",
                "price": {"units": "285"},
                "quantity": "2",
                "time": "2026-10-20T12:00:00.5Z",
            }
        ]
    }
    (trade,) = records(
        a.parse_rest(trades_req, orjson.dumps(reply_t).decode(), RecordFactory("v", "r", "c", 1)),
        Trade,
    )
    assert trade.side == "sell" and trade.size == 2


def test_tbank_requires_class_code(settings: Settings) -> None:
    cfg = settings.venues["moex-tbank"]
    bad = cfg.model_copy(
        update={"symbols": {"SBER": cfg.symbols["SBER"].model_copy(update={"board": None})}}
    )
    with pytest.raises(ValueError, match="class code"):
        TBankAdapter("moex-tbank", bad)


# -- MOEX ISS / AlgoPack ------------------------------------------------------------------------


def test_algopack_poll_rounds_and_auth(make_settings: Callable[..., Settings]) -> None:
    s = make_settings(ALGOPACK_TOKEN="tok")
    a = build_adapter("moex-algopack", s.venues["moex-algopack"])
    first = a.poll_requests()
    tags = {r.tag for r in first if r.native_symbol == "SBER"}
    assert tags == {
        "trades",
        "orderbook",
        "candles",
        "marketdata",
        "tradestats",
        "orderstats",
        "obstats",
    }
    assert first[0].url.startswith("https://apim.moex.com/iss/engines/stock/markets/shares")
    assert ("Authorization", "Bearer tok") in first[0].headers
    assert "tok" not in repr(first[0])
    second = {r.tag for r in a.poll_requests() if r.native_symbol == "SBER"}
    assert second == {"trades", "orderbook"}  # slow resources wait for their round


def test_iss_without_token_has_no_auth_header(settings: Settings) -> None:
    a = adapter_for(settings, "moex-iss")
    reqs = a.poll_requests()
    assert reqs and all(not r.headers for r in reqs)
    assert all(r.url.startswith("https://iss.moex.com/iss/") for r in reqs)


def test_algopack_parses_every_resource(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "moex-algopack")
    events = parse_all(a, venue_frames("moex-algopack"))
    trade = records(events, Trade)[0]
    # Moscow is UTC+3: 14:59:56 MSK on 2026-10-20 is 11:59:56 UTC.
    assert trade.ts_exchange == rfc3339_to_ns("2026-10-20T11:59:56Z")
    assert trade.trade_id == "13000000842" and trade.seq == 13000000842
    assert trade.side == "sell" and trade.size == 32 and trade.trading_session == "1"
    snaps = records(events, BookSnapshotLevel)
    assert snaps[0].snapshot_id.startswith("TQBR-") and snaps[0].seq == 20261020145958
    candles = records(events, Candle)
    assert [c.interval for c in candles] == ["1m", "1m"]
    assert candles[0].value == pytest.approx(1221136.1)
    (status,) = records(events, TradingStatus)
    assert status.status == "T"
    stats = records(events, StatsBar)
    assert {s.channel for s in stats} == {Channel.TRADESTATS, Channel.ORDERSTATS, Channel.OBSTATS}
    ob = next(s for s in stats if s.channel is Channel.OBSTATS)
    assert ob.value("spread_bbo") == pytest.approx(0.4)
    assert ob.ts_published is not None and ob.ts_published > (ob.ts_exchange or 0)


def test_algopack_cursors_advance(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "moex-algopack")
    parse_all(a, venue_frames("moex-algopack"))
    reqs = a.poll_requests()
    trades = next(r for r in reqs if r.tag == "trades" and r.native_symbol == "SBER")
    assert ("tradeno", "13000002800") in trades.params
    candles = next(r for r in reqs if r.tag == "candles" and r.native_symbol == "SBER")
    assert ("from", "2026-10-20 15:00:00") in candles.params


def test_algopack_full_page_is_backlogged(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "moex-algopack")
    frame = venue_frames("moex-algopack")[0]
    assert frame.request is not None
    req = frame.request.__class__(frame.request.url, "trades", "SBER", (("limit", "2"),))
    a.parse_rest(req, frame.payload, RecordFactory("moex-algopack", "msk", "c", frame.ts_local))
    assert a.backlogged()


def test_algopack_history_request(settings: Settings) -> None:
    a = adapter_for(settings, "moex-algopack")
    assert isinstance(a, MoexIssAdapter)
    req = a.history_request("candles", "SBER", date(2026, 10, 20), start="500")
    params = dict(req.params)
    assert params["from"] == "2026-10-20 00:00:00" and params["start"] == "500"
    stats = dict(a.history_request("obstats", "SBER", date(2026, 10, 20)).params)
    assert stats["from"] == stats["till"] == "2026-10-20" and stats["latest"] == "0"


def test_bad_payload_raises_for_pipeline_to_count(settings: Settings) -> None:
    a = adapter_for(settings, "moex-algopack")
    frame_req = a.poll_requests()[0]
    bad = '{"trades": {"columns": ["TRADENO"], "data": [[1]]}}'
    with pytest.raises(KeyError):
        a.parse_rest(frame_req, bad, RecordFactory("moex-algopack", "msk", "c", 1))
