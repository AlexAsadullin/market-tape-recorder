"""History backfill through fake APIs, and the certificate's delta path (ISS+ STOMP ``updates``)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import date
from pathlib import Path

import orjson
import pyarrow as pa
from typer.testing import CliRunner

from tape_recorder.adapters.base import RecordFactory
from tape_recorder.cli import app
from tape_recorder.frames import Frame, RestRequest
from tape_recorder.history import backfill, days
from tape_recorder.quality.certify import certify_instrument_day
from tape_recorder.quality.reader import list_partitions, read_partition
from tape_recorder.schema import Channel, records_to_table
from tape_recorder.settings import Settings
from tape_recorder.timeutil import NS_PER_S

Frames = Callable[[str], list[Frame]]
T0 = 1_792_497_600 * NS_PER_S  # 2026-10-20T12:00:00Z


def candle(minute: int) -> dict[str, object]:
    q = {"units": "285", "nano": 400000000}
    return {
        "open": q,
        "high": q,
        "low": q,
        "close": q,
        "volume": "7",
        "time": f"2026-10-20T12:{minute:02d}:00Z",
        "isComplete": True,
    }


def test_days_are_inclusive() -> None:
    assert list(days(date(2026, 10, 19), date(2026, 10, 21))) == [
        date(2026, 10, 19),
        date(2026, 10, 20),
        date(2026, 10, 21),
    ]


async def test_tbank_backfill_writes_candles(settings: Settings, venue_frames: Frames) -> None:
    share = venue_frames("moex-tbank")[0].payload
    seen: list[RestRequest] = []

    async def fetcher(req: RestRequest) -> str:
        seen.append(req)
        if req.tag == "share":
            return share if req.native_symbol == "SBER" else "{}"
        if req.native_symbol == "GAZP":
            raise ValueError("bad gateway answer")
        return orjson.dumps({"candles": [candle(0), candle(1)]}).decode()

    s = settings.model_copy(
        update={"backfill": settings.backfill.model_copy(update={"min_request_interval_s": 0})}
    )
    stats = await backfill(
        s, "moex-tbank", date(2026, 10, 20), date(2026, 10, 21), s.tape_root, fetcher, lambda: T0
    )
    candles = [r for r in seen if r.tag == "candles"]
    assert len(candles) == 3 * 2  # three symbols, two days
    # SBER and LKOH, two bars each; the fake answers the same bars on day two, which are deduped.
    assert stats.failures == 2 and stats.rows == 2 * 2
    (key,) = [k for k in list_partitions(s.tape_root) if k.symbol == "SBER-RUB-TQBR"]
    table = read_partition(s.tape_root, key)
    assert key.channel == Channel.CANDLES and table.num_rows == 2  # same bars both days: deduped


async def test_algopack_backfill_pages_candles(make_settings: Callable[..., Settings]) -> None:
    s = make_settings(TAPE_QUALITY__COVERAGE_WINDOW="observed", TAPE_BACKFILL__ISS_PAGE_ROWS="2")
    s = s.model_copy(
        update={"backfill": s.backfill.model_copy(update={"min_request_interval_s": 0})}
    )
    cols = ["open", "close", "high", "low", "value", "volume", "begin", "end"]

    def page(start: int) -> dict[str, object]:
        rows = [
            [1, 1, 1, 1, 10.0, 10, f"2026-10-20 10:{m:02d}:00", f"2026-10-20 10:{m:02d}:59"]
            for m in range(start, min(start + 2, 3))
        ]
        return {"candles": {"columns": cols, "data": rows}}

    starts: list[str] = []

    async def fetcher(req: RestRequest) -> str:
        params = dict(req.params)
        if req.tag == "candles":
            starts.append(params["start"])
            return orjson.dumps(page(int(params["start"]))).decode()
        return orjson.dumps({"data": {"columns": [], "data": []}}).decode()

    stats = await backfill(
        s,
        "moex-algopack",
        date(2026, 10, 20),
        date(2026, 10, 20),
        s.tape_root,
        fetcher,
        lambda: T0,
    )
    assert starts[:2] == ["0", "2"]  # a full page is followed by the next page
    assert stats.rows == 3 * 3  # three symbols, three bars each


def test_backfill_cli_needs_token(settings: Settings, monkeypatch: object) -> None:
    res = CliRunner().invoke(
        app, ["backfill", "--venue", "moex-algopack", "--from", "2026-10-20", "--to", "2026-10-20"]
    )
    assert res.exit_code != 0 and "ALGOPACK_TOKEN" in res.output


def test_sequenced_deltas_are_reconstructed_and_gaps_found(settings: Settings) -> None:
    """The delta path that an ISS+ STOMP ``updates`` feed will use."""
    f = RecordFactory("moex-algopack", "msk", "c1", T0)
    snap = f.snapshot("SBER-RUB-TQBR", T0, 10, "s10", [(285.0, 5.0)], [(285.1, 5.0)])
    deltas = [
        *RecordFactory("moex-algopack", "msk", "c1", T0 + 1).deltas(
            "SBER-RUB-TQBR", T0 + 1, 11, 10, None, [(285.05, 3.0)], []
        ),
        *RecordFactory("moex-algopack", "msk", "c1", T0 + 2).deltas(
            "SBER-RUB-TQBR", T0 + 2, 13, 12, None, [], [(285.2, 1.0)]
        ),
    ]
    tables: dict[Channel, pa.Table] = {
        Channel.BOOK_SNAPSHOT: records_to_table(Channel.BOOK_SNAPSHOT, list(snap)),
        Channel.BOOK_DELTA: records_to_table(Channel.BOOK_DELTA, deltas),
    }
    cfg = settings.venues["moex-algopack"].model_copy(update={"book_sequenced": True})
    cert = certify_instrument_day(
        tables, "moex-algopack", "SBER-RUB-TQBR", date(2026, 10, 20), cfg, settings.quality
    )
    assert cert.check("sequence_gaps").value == 1  # update 12 is missing
    assert cert.check("crossed_book_events").value == 0


async def test_backfill_unknown_venue(settings: Settings, tmp_path: Path) -> None:
    async def fetcher(req: RestRequest) -> str:
        return "{}"

    try:
        await backfill(
            settings, "nowhere", date(2026, 1, 1), date(2026, 1, 1), tmp_path, fetcher, lambda: 0
        )
    except ValueError as exc:
        assert "nowhere" in str(exc)
    else:
        raise AssertionError("expected ValueError")
    await asyncio.sleep(0)
