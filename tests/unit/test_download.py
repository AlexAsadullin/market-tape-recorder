"""``tape download`` / ``tape universe``: ranges, checkpoints, pacing, retries, universe."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from tape_recorder.download import (
    NS_PER_DAY,
    Checkpoint,
    FetchError,
    HttpReply,
    LimitedFetcher,
    RateLimiter,
    StoppedError,
    backoff_s,
    daily_chunks,
    day_window,
    header_int,
    is_throttled,
    merge,
    minute_chunks,
    plan_tbank,
    requested_window,
    split_span,
    stable_until,
    subtract,
)
from tape_recorder.frames import RestRequest
from tape_recorder.settings import DownloadSettings
from tape_recorder.universe import i2_rows, select_universe

REQ = RestRequest(url="https://invest-public-api.tbank.ru/x", tag="candles", native_symbol="SBER")


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def mono(self) -> float:
        return self.now

    async def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.now += s


# -- ranges ------------------------------------------------------------------------------------


def test_merge_joins_overlapping_and_adjacent() -> None:
    assert merge([(5, 9), (0, 2), (3, 4), (20, 30), (25, 26)]) == [(0, 9), (20, 30)]


def test_subtract_returns_uncovered_parts() -> None:
    assert subtract(0, 100, [(10, 19), (50, 200)]) == [(0, 9), (20, 49)]
    assert subtract(0, 10, []) == [(0, 10)]
    assert subtract(0, 10, [(0, 10)]) == []


def test_split_span_respects_maximum() -> None:
    assert split_span(0, 9, 4) == [(0, 3), (4, 7), (8, 9)]
    assert daily_chunks([(0, 3 * NS_PER_DAY - 1)], 2) == [
        (0, 2 * NS_PER_DAY - 1),
        (2 * NS_PER_DAY, 3 * NS_PER_DAY - 1),
    ]


def test_minute_chunks_one_request_per_moscow_day_only_on_given_days() -> None:
    d1, d3 = date(2026, 11, 2), date(2026, 11, 4)
    lo = day_window(d1)[0]
    hi = day_window(d3)[1]
    chunks = minute_chunks([(lo, hi)], [d1, d3])
    assert chunks == [day_window(d1), day_window(d3)]


def test_stable_until_excludes_current_and_previous_bar() -> None:
    bar = 60
    assert stable_until(1000, bar) == 960 - 60 - 1


def test_plan_tbank_skips_done_ranges_and_respects_first_candle(tmp_path: Path) -> None:
    cfg = DownloadSettings(daily_span_days=2190)
    first = day_window(date(2020, 1, 1))[0]
    now = day_window(date(2026, 11, 6))[0] + 12 * 3600 * 10**9
    entry = {"symbol": "SBER-RUB-TQBR", "first_1day_candle_date": "2020-01-01T07:00:00Z"}
    cp = Checkpoint(tmp_path / "cp.json")
    window = requested_window("1d", cfg, now, None, None)
    chunks = plan_tbank(entry, "1d", window, cp, cfg, None, now)
    assert chunks[0][0] == first and chunks[-1][1] == now
    cp.add(first, now - NS_PER_DAY, parsed=10, rows=10)
    rest = plan_tbank(entry, "1d", window, cp, cfg, None, now)
    assert rest == [(now - NS_PER_DAY + 1, now)]
    assert plan_tbank({"symbol": "X"}, "1d", window, cp, cfg, None, now) == []


# -- checkpoint --------------------------------------------------------------------------------


def test_checkpoint_round_trip_and_empty_ranges(tmp_path: Path) -> None:
    cp = Checkpoint(tmp_path / "a" / "cp.json")
    cp.add(0, 99, parsed=0, rows=0)
    cp.add(100, 199, parsed=5, rows=5)
    cp.add(50, 59, parsed=3, rows=3)  # a refetch inside an empty range removes that part
    cp.save()
    back = Checkpoint.load(cp.path)
    assert back.done == [(0, 199)]
    assert back.empty == [(0, 49), (60, 99)]
    assert back.rows == 8
    assert back.missing(0, 250) == [(200, 250)]


# -- pacing and retries ------------------------------------------------------------------------


def test_header_int_parses_tbank_forms() -> None:
    assert header_int({"x-ratelimit-limit": "200;w=60"}, "x-ratelimit-limit") == 200
    assert header_int({"x-ratelimit-limit": "600, 600;w=60"}, "x-ratelimit-limit") == 600
    assert header_int({"x-ratelimit-limit": "n/a"}, "x-ratelimit-limit") is None
    assert header_int({}, "x-ratelimit-limit") is None


async def test_rate_limiter_paces_at_fraction_of_reported_limit() -> None:
    clock = FakeClock()
    lim = RateLimiter(0.5, 600, clock.sleep, clock.mono)
    lim.observe({"x-ratelimit-limit": "120"})
    for _ in range(3):
        await lim.acquire()
    assert clock.sleeps == pytest.approx([1.0, 1.0])  # 60 / (120 * 0.5)


async def test_rate_limiter_waits_for_reset_when_remaining_is_low() -> None:
    clock = FakeClock()
    lim = RateLimiter(1.0, 600, clock.sleep, clock.mono)
    lim.observe(
        {"x-ratelimit-limit": "600", "x-ratelimit-remaining": "1", "x-ratelimit-reset": "7"}
    )
    await lim.acquire()
    assert clock.sleeps == [pytest.approx(8.0)]
    assert lim.waits == 1


def test_throttle_detection_and_backoff() -> None:
    assert is_throttled(HttpReply(429, {}, ""))
    assert is_throttled(HttpReply(400, {}, '{"code": 8, "message": "RESOURCE_EXHAUSTED"}'))
    assert not is_throttled(HttpReply(400, {}, '{"code": 3}'))
    assert [backoff_s(a, 1.0, 5.0) for a in range(4)] == [1.0, 2.0, 4.0, 5.0]


def _fetcher(replies: list[HttpReply | Exception], clock: FakeClock, **kw: Any) -> LimitedFetcher:
    async def send(_: RestRequest) -> HttpReply:
        item = replies.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    cfg = DownloadSettings(max_attempts=4, backoff_initial_s=1.0, backoff_max_s=10.0)
    lim = RateLimiter(1.0, 6000, clock.sleep, clock.mono)
    return LimitedFetcher(send, lim, cfg, clock.sleep, **kw)


async def test_fetcher_retries_429_and_5xx_then_succeeds() -> None:
    import httpx

    clock = FakeClock()
    replies: list[HttpReply | Exception] = [
        HttpReply(429, {"x-ratelimit-reset": "3"}, ""),
        HttpReply(503, {}, "busy"),
        httpx.RemoteProtocolError("peer closed"),
        HttpReply(200, {}, '{"candles": []}'),
    ]
    f = _fetcher(replies, clock)
    assert await f(REQ) == '{"candles": []}'
    assert f.stats.requests == 1 and f.stats.retries == 3 and f.stats.throttled == 1


async def test_fetcher_final_on_4xx_and_gives_up_after_max_attempts() -> None:
    clock = FakeClock()
    f = _fetcher([HttpReply(400, {}, "bad")], clock)
    with pytest.raises(FetchError) as exc:
        await f(REQ)
    assert exc.value.status == 400
    f = _fetcher([HttpReply(502, {}, "")] * 4, clock)
    with pytest.raises(FetchError, match="gave up"):
        await f(REQ)


async def test_fetcher_stops_when_asked() -> None:
    clock = FakeClock()
    f = _fetcher([HttpReply(200, {}, "{}")], clock, stopped=lambda: True)
    with pytest.raises(StoppedError):
        await f(REQ)


# -- universe ----------------------------------------------------------------------------------


def _share(ticker: str, board: str = "TQBR", **kw: Any) -> dict[str, Any]:
    doc = {
        "ticker": ticker,
        "class_code": board,
        "uid": f"uid-{ticker}",
        "figi": f"F{ticker}",
        "lot": 10,
        "currency": "rub",
        "min_price_increment": {"units": "0", "nano": 10000000},
        "first1min_candle_date": "2018-03-07T18:33:00Z",
        "first1day_candle_date": "2000-01-04T07:00:00Z",
        "trading_status": "SECURITY_TRADING_STATUS_NORMAL_TRADING",
        "name": ticker,
        "sector": "financial",
    }
    doc.update(kw)
    return doc


def test_select_universe_shares_on_board_plus_zero_fee_funds_on_tqtf() -> None:
    shares = [_share("SBER"), _share("AAPL", board="SPBXM"), _share("GAZP")]
    etfs = [_share("TMOS", board="TQTF"), _share("TMOS@", board="SPBRU"), _share("OTHER", "TQTF")]
    zero_fee = {
        "funds": [
            {"instrument_uid": "uid-TMOS", "class_code": "TQTF"},
            {"instrument_uid": "uid-TMOS@", "class_code": "SPBRU"},
        ]
    }
    entries = select_universe(shares, etfs, zero_fee, DownloadSettings())
    assert [e["symbol"] for e in entries] == ["GAZP-RUB-TQBR", "SBER-RUB-TQBR", "TMOS-RUB-TQTF"]
    sber = entries[1]
    assert sber["min_price_increment"] == pytest.approx(0.01)
    assert sber["first_1day_candle_date"] == "2000-01-04T07:00:00Z"
    with pytest.raises(ValueError, match="not in Etfs"):
        select_universe(shares, [], zero_fee, DownloadSettings())


def test_i2_rows_keep_times_when_rules_unchanged() -> None:
    entries = select_universe([_share("SBER")], [], {"funds": []}, DownloadSettings())
    first = i2_rows(entries, 1_000_000_000)
    again = i2_rows(entries, 9_000_000_000, {r["symbol"]: r for r in first})
    assert again[0]["published_at"] == first[0]["published_at"]
    changed = [dict(entries[0], lot=100)]
    moved = i2_rows(changed, 9_000_000_000, {r["symbol"]: r for r in first})
    assert moved[0]["published_at"] != first[0]["published_at"]
    assert moved[0]["lot_size"] == 100.0
