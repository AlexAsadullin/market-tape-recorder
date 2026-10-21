"""Live exchange checks (network): ``pytest -m live``. Skipped by the default offline run."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from tape_recorder.quality.certify import certify_tree
from tape_recorder.recorder import record
from tape_recorder.settings import Settings

pytestmark = pytest.mark.live

LIVE_SECONDS = 20.0


@pytest.mark.parametrize("venue", ["binance", "binance-usdm", "bybit", "okx", "hyperliquid", "moex"])
async def test_live_capture_certifies(make_settings: Callable[..., Settings], venue: str) -> None:
    s = make_settings(TAPE_QUALITY__COVERAGE_WINDOW="observed", TAPE_SERVICE__ENABLED="false")
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(LIVE_SECONDS, stop.set)
    await record(s, s.tape_root, [venue], stop, raw_log=False)
    certs = [c for c, _ in certify_tree(s, s.tape_root, s.certificates_root)]
    assert certs, f"no data recorded from {venue}"
    for c in certs:
        assert c.check("sequence_gaps").value == 0
        assert c.check("crossed_book_events").value == 0
