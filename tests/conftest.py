"""Shared fixtures: settings bound to a temporary data root and the venue fixture files."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from tape_recorder.frames import Frame, load_frames
from tape_recorder.settings import Settings, load_settings

FIXTURES = Path(__file__).parent / "fixtures" / "venues"


@pytest.fixture
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "data"
    monkeypatch.setenv("TRADEQ_DATA_DIR", str(root))
    monkeypatch.delenv("TAPE_CONFIG_FILE", raising=False)
    # Tests never see real tokens: no repository .env, and no inherited token variables.
    monkeypatch.chdir(tmp_path)
    for name in ("TBANK_TOKEN", "ALGOPACK_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    return root


@pytest.fixture
def make_settings(data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., Settings]:
    def factory(**env: Any) -> Settings:
        for key, value in env.items():
            monkeypatch.setenv(key, str(value))
        return load_settings()

    return factory


@pytest.fixture
def settings(make_settings: Callable[..., Settings]) -> Settings:
    return make_settings(TAPE_QUALITY__COVERAGE_WINDOW="observed")


@pytest.fixture
def venue_frames() -> Callable[[str], list[Frame]]:
    def load(venue: str) -> list[Frame]:
        return load_frames(FIXTURES / f"{venue}.json")

    return load
