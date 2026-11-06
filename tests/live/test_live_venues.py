"""Live checks against the real APIs: ``pytest -m live`` with TBANK_TOKEN / ALGOPACK_TOKEN set.

Skipped by the default offline run, and skipped per venue when its token is missing.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from pathlib import Path

import pytest
from dotenv import dotenv_values

from tape_recorder.quality.certify import certify_tree
from tape_recorder.recorder import record
from tape_recorder.settings import Settings

pytestmark = pytest.mark.live

LIVE_SECONDS = 30.0
TOKEN_VARS = ("TBANK_TOKEN", "ALGOPACK_TOKEN")
REPO_ENV = Path(__file__).resolve().parents[2] / ".env"
# Captured before the data_dir fixture clears the variables: the process env, else the repo .env.
_FILE = dotenv_values(REPO_ENV) if REPO_ENV.exists() else {}
_TOKENS = {n: os.environ.get(n) or _FILE.get(n) for n in TOKEN_VARS}


@pytest.fixture
def live_tokens(data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in _TOKENS.items():
        if value:
            monkeypatch.setenv(name, value)


@pytest.mark.parametrize("venue", ["moex-tbank", "moex-algopack", "moex-iss"])
@pytest.mark.usefixtures("live_tokens")
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
