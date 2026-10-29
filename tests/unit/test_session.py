from __future__ import annotations

import asyncio
import random
from collections.abc import Callable, Sequence

import httpx
import orjson
import pytest
import respx

from tape_recorder.adapters import build_adapter
from tape_recorder.dedup import Deduplicator
from tape_recorder.frames import Frame, RestRequest
from tape_recorder.monitor import StreamMonitor
from tape_recorder.pipeline import Pipeline
from tape_recorder.schema import Record
from tape_recorder.session import (
    Backoff,
    PollingVenueSession,
    WsVenueSession,
    counter_ids,
    next_daily_rotation,
)
from tape_recorder.settings import BackoffSettings, Settings
from tape_recorder.timeutil import NS_PER_S, wall_clock_ns
from tape_recorder.transport import HttpxFetcher

Frames = Callable[[str], list[Frame]]


class Sink:
    def __init__(self) -> None:
        self.records: list[Record] = []

    def write(self, records: Sequence[Record]) -> None:
        self.records.extend(records)


class FakeWs:
    """Plays ``messages``; then closes (``close_after``) or idles, repeating if ``loop``."""

    def __init__(
        self,
        name: str,
        messages: list[str],
        log: list[str],
        close_after: bool,
        interval_s: float = 0.0,
        loop: bool = False,
    ) -> None:
        self.name, self.messages, self.log = name, list(messages), log
        self.close_after, self.interval_s, self.loop = close_after, interval_s, loop
        self.sent: list[str] = []
        self._i = 0
        self.closed = asyncio.Event()

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        if self.interval_s:
            await asyncio.sleep(self.interval_s)
        if self._i >= len(self.messages):
            if self.loop:
                self._i = 0
            elif self.close_after:
                raise ConnectionError("server closed")
            else:
                await self.closed.wait()
                raise ConnectionError("closed")
        msg = self.messages[self._i]
        self._i += 1
        return msg

    async def close(self) -> None:
        self.log.append(f"close {self.name}")
        self.closed.set()


class FakeConnector:
    def __init__(self, scripts: list[Callable[[str], FakeWs]], fail_first: int = 0) -> None:
        self.scripts, self.log, self.fail_first = scripts, [], fail_first
        self.opened: list[FakeWs] = []
        self.handshakes: list[tuple[tuple[tuple[str, str], ...], tuple[str, ...]]] = []

    async def __call__(
        self,
        url: str,
        headers: tuple[tuple[str, str], ...] = (),
        subprotocols: tuple[str, ...] = (),
    ) -> FakeWs:
        self.handshakes.append((headers, subprotocols))
        if self.fail_first:
            self.fail_first -= 1
            raise OSError("refused")
        idx = min(len(self.opened), len(self.scripts) - 1)
        ws = self.scripts[idx](f"ws{len(self.opened)}")
        self.opened.append(ws)
        self.log.append(f"open {ws.name}")
        return ws


async def no_sleep(_: float) -> None:
    await asyncio.sleep(0)


def payloads(frames: list[Frame], source: str = "ws") -> list[str]:
    return [f.payload for f in frames if f.source == source]


def with_uid(settings: Settings, frames: list[Frame]) -> Settings:
    """T-Bank settings that already know the fixture's uid (the ShareBy answer of frame 0)."""
    uid = orjson.loads(frames[0].payload)["instrument"]["uid"]
    venue = settings.venues["moex-tbank"]
    symbols = dict(venue.symbols)
    symbols["SBER"] = symbols["SBER"].model_copy(update={"instrument_uid": uid})
    venues = dict(settings.venues)
    venues["moex-tbank"] = venue.model_copy(update={"symbols": symbols})
    return settings.model_copy(update={"venues": venues})


def looping_book_stream(frames: list[Frame]) -> list[str]:
    """Distinct order books, so a looping fake stream never repeats a dedup key."""
    books = [m for m in payloads(frames) if "orderbook" in m]
    out: list[str] = []
    for i in range(200):
        doc = orjson.loads(books[i % len(books)])
        doc["orderbook"]["time"] = f"2026-10-20T12:{i // 60:02d}:{i % 60:02d}.000000Z"
        out.append(orjson.dumps(doc).decode())
    return out


def session_for(
    settings: Settings,
    venue: str,
    connector: FakeConnector,
    fetcher: Callable[[RestRequest], object],
    **session_overrides: object,
) -> tuple[WsVenueSession, Sink, StreamMonitor]:
    adapter = build_adapter(venue, settings.venues[venue])
    sink = Sink()
    monitor = StreamMonitor(wall_clock_ns, 30.0)
    pipeline = Pipeline({venue: adapter}, "eu", sink, Deduplicator(60, 10_000), monitor)
    cfg = settings.session.model_copy(update={"daily_rotation_utc": None, **session_overrides})
    sess = WsVenueSession(
        adapter,
        pipeline,
        connector,
        fetcher,
        cfg,  # type: ignore[arg-type]
        wall_clock_ns,
        monitor,
        counter_ids("t"),
        no_sleep,
    )
    return sess, sink, monitor


async def run_until(sess: WsVenueSession, cond: Callable[[], bool], timeout: float = 5.0) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(sess.run(stop))
    try:
        async with asyncio.timeout(timeout):
            while not cond():
                await asyncio.sleep(0.005)
    finally:
        stop.set()
        await task


def test_backoff_grows_with_cap_and_resets() -> None:
    b = Backoff(BackoffSettings(initial_s=1, max_s=5, multiplier=2, jitter=0), random.Random(1))
    assert [b.next_delay() for _ in range(5)] == [1, 2, 4, 5, 5]
    b.reset()
    assert b.next_delay() == 1
    j = Backoff(BackoffSettings(initial_s=1, max_s=5, multiplier=2, jitter=0.2), random.Random(1))
    assert all(0.8 <= j.next_delay() / base <= 1.2 for base in (1, 2, 4))


def test_next_daily_rotation() -> None:
    noon = 1_792_497_600 * NS_PER_S  # 2026-10-20T12:00:00Z
    assert next_daily_rotation(noon, "00:00:05") == (1_792_540_800 + 5) * NS_PER_S
    assert next_daily_rotation(noon, "12:30:00") == (1_792_497_600 + 1800) * NS_PER_S


async def test_reconnects_after_drop_and_after_connect_failure(
    settings: Settings, venue_frames: Frames
) -> None:
    frames = venue_frames("moex-tbank")
    msgs = payloads(frames)
    conn = FakeConnector(
        [
            lambda n: FakeWs(n, msgs[:5], [], close_after=True),
            lambda n: FakeWs(n, msgs[5:], [], close_after=False),
        ],
        fail_first=1,
    )
    sess, sink, monitor = session_for(
        with_uid(settings, frames), "moex-tbank", conn, lambda r: "", redundant_connections=1
    )
    await run_until(sess, lambda: len({r.connection_id for r in sink.records}) == 2)
    assert conn.opened[0].sent == sess.adapter.subscribe_messages()
    (venue,) = monitor.venues()
    assert venue.connects == 2 and venue.disconnects >= 1


async def test_handshake_carries_token_and_subprotocol(
    make_settings: Callable[..., Settings], venue_frames: Frames
) -> None:
    frames = venue_frames("moex-tbank")
    s = with_uid(make_settings(TBANK_TOKEN="tok"), frames)
    conn = FakeConnector([lambda n: FakeWs(n, payloads(frames), [], close_after=False)])
    sess, sink, _ = session_for(s, "moex-tbank", conn, lambda r: "", redundant_connections=1)
    await run_until(sess, lambda: bool(sink.records))
    headers, subprotocols = conn.handshakes[0]
    assert ("Authorization", "Bearer tok") in headers and subprotocols == ("json",)


async def test_redundant_connections_are_deduplicated(
    settings: Settings, venue_frames: Frames
) -> None:
    frames = venue_frames("moex-tbank")
    s = with_uid(settings, frames)
    msgs = payloads(frames)
    conn = FakeConnector([lambda n: FakeWs(n, msgs, [], close_after=False)])
    sess, sink, _ = session_for(s, "moex-tbank", conn, lambda r: "", redundant_connections=2)
    single = Sink()
    adapter = build_adapter("moex-tbank", s.venues["moex-tbank"])
    ref = Pipeline({"moex-tbank": adapter}, "eu", single, None)
    for i, m in enumerate(msgs):
        ref.process(Frame("moex-tbank", i, "x", "ws", m))
    await run_until(
        sess, lambda: len(conn.opened) == 2 and len(sink.records) >= len(single.records)
    )
    await asyncio.sleep(0.05)
    assert len(sink.records) == len(single.records)
    assert sess.pipeline.stats.duplicates > 0


async def test_rotation_overlaps_old_and_new_connection(
    settings: Settings, venue_frames: Frames
) -> None:
    frames = venue_frames("moex-tbank")
    msgs = looping_book_stream(frames)
    log: list[str] = []
    conn = FakeConnector(
        [lambda n: FakeWs(n, msgs, log, close_after=False, interval_s=0.002, loop=True)]
    )
    conn.log = log
    sess, _, _ = session_for(
        with_uid(settings, frames),
        "moex-tbank",
        conn,
        lambda r: "",
        redundant_connections=1,
        rotate_after_s=0.1,
    )
    await run_until(sess, lambda: sess.rotations >= 2)
    # The replacement is opened before the connection it replaces is closed.
    assert log.index("open ws1") < log.index("close ws0")
    assert log.index("open ws2") < log.index("close ws1")


async def test_polling_session_with_mocked_algopack(
    make_settings: Callable[..., Settings], venue_frames: Frames
) -> None:
    s = make_settings(ALGOPACK_TOKEN="tok", TAPE_QUALITY__COVERAGE_WINDOW="observed")
    frames = venue_frames("moex-algopack")
    by_tag = {f.request.tag: f.payload for f in frames if f.request is not None}
    adapter = build_adapter("moex-algopack", s.venues["moex-algopack"])
    sink = Sink()
    pipeline = Pipeline({"moex-algopack": adapter}, "eu", sink, Deduplicator(60, 1000))
    base = "https://apim.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities"
    with respx.mock(assert_all_called=False) as mock:
        mock.get(f"{base}/SBER/trades.json").respond(200, text=by_tag["trades"])
        mock.get(f"{base}/SBER/orderbook.json").respond(200, text=by_tag["orderbook"])
        mock.get(f"{base}/SBER/candles.json").respond(200, text=by_tag["candles"])
        mock.get(f"{base}/SBER.json").respond(200, text=by_tag["marketdata"])
        for tag in ("tradestats", "orderstats", "obstats"):
            mock.get(f"https://apim.moex.com/iss/datashop/algopack/eq/{tag}/SBER.json").respond(
                200, text=by_tag[tag]
            )
        mock.get(url__regex=r".*/(GAZP|LKOH).*").respond(503)
        async with httpx.AsyncClient() as client:
            sess = PollingVenueSession(
                adapter,
                pipeline,
                HttpxFetcher(client),
                s.session,
                wall_clock_ns,
                counter_ids("p"),
                no_sleep,
            )
            stop = asyncio.Event()
            task = asyncio.create_task(sess.run(stop))
            async with asyncio.timeout(5):
                while sess.failures < 2:
                    await asyncio.sleep(0.001)
            stop.set()
            await task
        sent = mock.calls[0].request
    assert sent.headers["Authorization"] == "Bearer tok"
    trades = [r for r in sink.records if r.channel == "trades"]
    n_rows = len(orjson.loads(by_tag["trades"])["trades"]["data"])
    assert len(trades) == n_rows  # repeated polls are deduplicated by trade number
    assert sess.new_id() == f"p-{sess.failures + 2}"


async def test_post_requests_send_json_body() -> None:
    req = RestRequest(
        "https://example.test/rest/Svc/Method", "candles", "SBER", method="POST", body='{"a": 1}'
    )
    with respx.mock() as mock:
        route = mock.post("https://example.test/rest/Svc/Method").respond(200, text="{}")
        async with httpx.AsyncClient() as client:
            assert await HttpxFetcher(client)(req) == "{}"
    sent = route.calls[0].request
    assert sent.content == b'{"a": 1}'
    assert sent.headers["Content-Type"] == "application/json"
    assert RestRequest.from_json(req.to_json()) == req


def test_polling_requires_interval(settings: Settings) -> None:
    cfg = settings.venues["moex-algopack"].model_copy(update={"poll_interval_s": None})
    adapter = build_adapter("moex-algopack", cfg)
    with pytest.raises(ValueError):
        PollingVenueSession(
            adapter,
            Pipeline({}, "eu", Sink(), None),
            lambda r: "",  # type: ignore[arg-type]
            settings.session,
            wall_clock_ns,
            counter_ids("p"),
        )


async def test_naive_rotation_closes_before_opening(
    settings: Settings, venue_frames: Frames
) -> None:
    frames = venue_frames("moex-tbank")
    msgs = looping_book_stream(frames)
    log: list[str] = []
    conn = FakeConnector(
        [lambda n: FakeWs(n, msgs, log, close_after=False, interval_s=0.002, loop=True)]
    )
    conn.log = log
    sess, _, _ = session_for(
        with_uid(settings, frames),
        "moex-tbank",
        conn,
        lambda r: "",
        redundant_connections=1,
        rotate_after_s=0.1,
        overlap_rotation=False,
    )
    await run_until(sess, lambda: sess.rotations >= 1 and len(conn.opened) >= 2)
    assert log.index("close ws0") < log.index("open ws1")
