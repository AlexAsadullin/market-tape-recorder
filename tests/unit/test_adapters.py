from __future__ import annotations

from collections.abc import Callable

import orjson
import pytest

from tape_recorder.adapters import ParsedEvent, RecordFactory, VenueAdapter, build_adapter
from tape_recorder.frames import Frame
from tape_recorder.schema import (
    BookDelta,
    BookSnapshotLevel,
    Funding,
    Liquidation,
    OpenInterest,
    Record,
    Trade,
)
from tape_recorder.settings import Settings
from tape_recorder.timeutil import NS_PER_MS

Frames = Callable[[str], list[Frame]]


def parse_all(adapter: VenueAdapter, frames: list[Frame]) -> list[ParsedEvent]:
    out: list[ParsedEvent] = []
    for f in frames:
        factory = RecordFactory(adapter.venue, "eu", f.connection_id, f.ts_local)
        if f.source == "rest":
            assert f.request is not None
            out.extend(adapter.parse_rest(f.request, f.payload, factory))
        else:
            out.extend(adapter.parse_ws(f.payload, factory))
    return out


def records(events: list[ParsedEvent], kind: type[Record]) -> list[Record]:
    return [r for e in events for r in e.records if isinstance(r, kind)]


def adapter_for(settings: Settings, venue: str) -> VenueAdapter:
    return build_adapter(venue, settings.venues[venue])


def test_binance_spot(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "binance")
    sub = orjson.loads(a.subscribe_messages()[0])
    assert sub["method"] == "SUBSCRIBE"
    assert "btcusdt@depth@100ms" in sub["params"] and "ethusdt@trade" in sub["params"]
    (req, *_) = a.snapshot_requests()
    assert req.url == "https://api.binance.com/api/v3/depth"
    assert ("limit", "1000") in req.params
    events = parse_all(a, venue_frames("binance"))
    deltas = records(events, BookDelta)
    first = deltas[0]
    assert isinstance(first, BookDelta)
    assert first.symbol == "BTC-USDT-SPOT" and first.prev_seq is None
    assert first.first_seq is not None and first.seq is not None and first.first_seq < first.seq
    trades = records(events, Trade)
    assert all(isinstance(t, Trade) and t.seq == int(t.trade_id) for t in trades)
    snaps = records(events, BookSnapshotLevel)
    assert {s.snapshot_id for s in snaps if isinstance(s, BookSnapshotLevel)} == {
        "rest-41000",
        f"rest-{snaps[-1].seq}",
    }


def test_binance_trade_side_is_aggressor(settings: Settings) -> None:
    a = adapter_for(settings, "binance")
    f = RecordFactory("binance", "eu", "c1", 5)
    msg = {"e": "trade", "s": "BTCUSDT", "t": 1, "p": "10", "q": "2", "T": 3, "m": True}
    (event,) = a.parse_ws(orjson.dumps(msg).decode(), f)
    trade = event.records[0]
    assert isinstance(trade, Trade)
    assert trade.side == "sell"  # buyer is maker, so the seller hit the bid
    assert trade.ts_exchange == 3 * NS_PER_MS
    assert a.parse_ws('{"result": null, "id": 1}', f) == []
    assert a.parse_ws(orjson.dumps({**msg, "s": "XRPUSDT"}).decode(), f) == []


def test_binance_usdm(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "binance-usdm")
    params = orjson.loads(a.subscribe_messages()[0])["params"]
    assert {"btcusdt@aggTrade", "btcusdt@markPrice@1s", "btcusdt@forceOrder"} <= set(params)
    events = parse_all(a, venue_frames("binance-usdm"))
    delta = records(events, BookDelta)[1]
    assert isinstance(delta, BookDelta) and delta.prev_seq is not None
    funding = records(events, Funding)[0]
    assert isinstance(funding, Funding)
    assert funding.rate == pytest.approx(1e-4) and funding.index_price is not None
    liq = records(events, Liquidation)[0]
    assert isinstance(liq, Liquidation)
    assert liq.side == "sell" and liq.price == pytest.approx(66995.3)


def test_bybit(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "bybit")
    topics = [t for m in a.subscribe_messages() for t in orjson.loads(m)["args"]]
    assert topics == [
        "orderbook.50.BTCUSDT",
        "publicTrade.BTCUSDT",
        "tickers.BTCUSDT",
        "allLiquidation.BTCUSDT",
    ]
    events = parse_all(a, venue_frames("bybit"))
    deltas = [r for r in records(events, BookDelta) if isinstance(r, BookDelta)]
    assert all(d.first_seq == d.seq for d in deltas)
    funding = [r for r in records(events, Funding) if isinstance(r, Funding)]
    # Ticker deltas carry only changed fields; the rate is kept from the snapshot.
    assert len(funding) > 1 and all(f.rate == pytest.approx(1e-4) for f in funding)
    oi = records(events, OpenInterest)
    assert oi and isinstance(oi[0], OpenInterest) and oi[0].open_interest_value is not None
    liq = records(events, Liquidation)[0]
    assert isinstance(liq, Liquidation) and liq.side == "sell"  # a long ("Buy") was liquidated
    assert a.parse_ws('{"success": true, "op": "subscribe"}', RecordFactory("b", "r", "c", 1)) == []


def test_okx(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "okx")
    args = orjson.loads(a.subscribe_messages()[0])["args"]
    assert {"channel": "liquidation-orders", "instType": "SWAP"} in args
    events = parse_all(a, venue_frames("okx"))
    snaps = [r for r in records(events, BookSnapshotLevel) if isinstance(r, BookSnapshotLevel)]
    raw = orjson.loads(venue_frames("okx")[0].payload)["data"][0]
    top_bid = max(raw["bids"], key=lambda lv: float(lv[0]))
    best = next(s for s in snaps if s.side == "bid" and s.level == 0)
    assert best.size == pytest.approx(float(top_bid[1]) * 0.01)  # contracts -> BTC
    delta = next(r for r in records(events, BookDelta) if isinstance(r, BookDelta))
    assert delta.prev_seq == snaps[0].seq
    funding = records(events, Funding)
    assert funding and isinstance(funding[0], Funding) and funding[0].mark_price == 67002.3
    assert records(events, Liquidation) and records(events, OpenInterest)
    assert a.parse_ws("pong", RecordFactory("okx", "eu", "c", 1)) == []


def test_okx_funding_waits_for_mark_price(settings: Settings) -> None:
    a = adapter_for(settings, "okx")
    msg = {
        "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
        "data": [{"fundingRate": "0.0001", "fundingTime": "5", "ts": "4"}],
    }
    assert a.parse_ws(orjson.dumps(msg).decode(), RecordFactory("okx", "eu", "c", 1)) == []


def test_hyperliquid(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "hyperliquid")
    subs = [orjson.loads(m)["subscription"]["type"] for m in a.subscribe_messages()]
    assert subs == ["l2Book", "trades", "activeAssetCtx"]
    events = parse_all(a, venue_frames("hyperliquid"))
    trade = records(events, Trade)[0]
    assert isinstance(trade, Trade)
    assert trade.buyer is not None and trade.buyer.startswith("0x") and trade.seller
    funding = records(events, Funding)[0]
    assert isinstance(funding, Funding) and funding.ts_exchange is None
    oi = records(events, OpenInterest)[0]
    assert isinstance(oi, OpenInterest)
    assert oi.open_interest_value == pytest.approx(oi.open_interest * funding.mark_price)
    assert not records(events, BookDelta)


def test_moex_polling_cursor_and_timezone(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "moex")
    reqs = a.poll_requests()
    assert {r.tag for r in reqs} == {"trades", "orderbook"}
    assert reqs[0].url.endswith("/engines/stock/markets/shares/boards/TQBR/securities/SBER/trades.json")
    assert not any(k == "tradeno" for k, _ in reqs[0].params)
    frames = venue_frames("moex")
    events = parse_all(a, frames[:2])
    trade = records(events, Trade)[0]
    assert isinstance(trade, Trade)
    raw = orjson.loads(frames[0].payload)["trades"]
    systime = raw["data"][0][raw["columns"].index("SYSTIME")]
    # Moscow is UTC+3: the ISS wall-clock time is three hours ahead of UTC.
    hour_utc = (trade.ts_exchange or 0) // (3600 * 10**9) % 24
    assert int(systime[11:13]) == (hour_utc + 3) % 24
    assert trade.size == raw["data"][0][raw["columns"].index("QUANTITY")] * 10  # lots -> shares
    next_trades = next(r for r in a.poll_requests() if r.tag == "trades")
    assert ("tradeno", trade.trade_id) in next_trades.params or any(
        k == "tradeno" for k, _ in next_trades.params
    )
    assert not a.backlogged()
    snap = records(events, BookSnapshotLevel)
    assert snap and isinstance(snap[0], BookSnapshotLevel) and snap[0].snapshot_id.startswith("TQBR-")


def test_moex_full_page_is_backlogged(settings: Settings, venue_frames: Frames) -> None:
    a = adapter_for(settings, "moex")
    frame = venue_frames("moex")[0]
    assert frame.request is not None
    req = frame.request.__class__(
        frame.request.url, "trades", "SBER", (("limit", "2"),)
    )
    a.parse_rest(req, frame.payload, RecordFactory("moex", "eu", "c", frame.ts_local))
    assert a.backlogged()


def test_moex_algopack_uses_bearer_token(make_settings: Callable[..., Settings]) -> None:
    s = make_settings(TAPE_VENUES__MOEX__API_KEY="tok")
    a = build_adapter("moex", s.venues["moex"])
    req = a.poll_requests()[0]
    assert req.url.startswith("https://apim.moex.com/iss")
    assert ("Authorization", "Bearer tok") in req.headers
    assert "tok" not in repr(req)


def test_bad_payload_raises_for_pipeline_to_count(settings: Settings) -> None:
    a = adapter_for(settings, "binance")
    with pytest.raises(KeyError):
        a.parse_ws('{"e": "trade", "s": "BTCUSDT"}', RecordFactory("binance", "eu", "c", 1))
