from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from tape_recorder.schema import Channel
from tape_recorder.settings import Settings


def test_yaml_defaults_and_data_root(make_settings: Callable[..., Settings], data_dir: Path) -> None:
    s = make_settings()
    assert s.data_dir == data_dir
    assert s.project_root == data_dir / "market-tape-recorder"
    assert s.tape_root == s.project_root / "tape"
    assert s.service.port == 8602
    assert Channel.BOOK_DELTA in s.venues["binance"].channels
    assert s.capture_root("tokyo") == s.project_root / "captures" / "region=tokyo"


def test_env_overrides_nested_values_and_secrets(make_settings: Callable[..., Settings]) -> None:
    s = make_settings(
        TAPE_SESSION__REDUNDANT_CONNECTIONS="3",
        TAPE_REGION="tokyo",
        TAPE_VENUES__MOEX__API_KEY="secret-token",
    )
    assert s.session.redundant_connections == 3
    assert s.region == "tokyo"
    key = s.venues["moex"].api_key
    assert key is not None and key.get_secret_value() == "secret-token"
    assert "secret-token" not in repr(s.venues["moex"])
    # Other values of the overridden sections still come from YAML.
    assert s.session.backoff.max_s == 30.0


def test_venue_quality_overrides(make_settings: Callable[..., Settings]) -> None:
    q = make_settings().quality
    assert q.checks_for("moex")["clock_skew_p50_abs_ms"].threshold == 15000
    assert q.checks_for("binance")["clock_skew_p50_abs_ms"].threshold == 1000


def test_enabled_venues_filter(make_settings: Callable[..., Settings]) -> None:
    s = make_settings(TAPE_VENUES__OKX__ENABLED="false")
    assert "okx" not in s.enabled_venues()
    assert "binance" in s.enabled_venues()
