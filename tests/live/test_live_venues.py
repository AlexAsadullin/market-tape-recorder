"""Live checks against the real APIs: ``pytest -m live`` with TBANK_TOKEN / ALGOPACK_TOKEN set.

Skipped by the default offline run, and skipped per venue when its token is missing.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from tape_recorder.quality.certify import certify_tree
from tape_recorder.recorder import record
from tape_recorder.settings import Settings

pytestmark = pytest.mark.live

LIVE_SECONDS = 30.0


@pytest.mark.parametrize("venue", ["moex-tbank", "moex-algopack", "moex-iss"])
async def test_live_capture_certifies(make_settings: Callable[..., Settings], venue: str) -> None:
    s = make_settings(TAPE_QUALITY__COVERAGE_WINDOW="observed", TAPE_SERVICE__ENABLED="false")
    cfg = s.venues[venue]
    if cfg.token_required and cfg.token() is None:
        pytest.skip(f"{venue}: ${cfg.token_env} is not set")
    venues = dict(s.venues)
    venues[venue] = cfg.model_copy(update={"enabled": True})
    s = s.model_copy(update={"venues": venues})
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(LIVE_SECONDS, stop.set)
    await record(s, s.tape_root, [venue], stop, raw_log=True)
    certs = [c for c, _ in certify_tree(s, s.tape_root, s.certificates_root)]
    assert certs, f"no data recorded from {venue} (is the market open?)"
    for c in certs:
        assert c.check("crossed_book_events").value == 0
