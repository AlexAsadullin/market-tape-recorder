"""Fixtures replayed through adapters with injected faults; certificates must catch each one."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from tape_recorder.faults import apply_fault
from tape_recorder.frames import Frame
from tape_recorder.quality.certificate import Certificate, read_certificate
from tape_recorder.quality.certify import certify_tree
from tape_recorder.replay import replay_frames
from tape_recorder.settings import FaultSpec, Settings

Frames = Callable[[str], list[Frame]]
VENUES = ["moex-tbank", "moex-algopack"]


def certify(settings: Settings, frames: list[Frame], root: Path) -> dict[str, Certificate]:
    replay_frames(settings, frames, "msk", root)
    return {c.venue: c for c, _ in certify_tree(settings, root, root / "certs")}


def failed(cert: Certificate) -> set[str]:
    return {c.name for c in cert.checks if not c.passed}


@pytest.mark.parametrize("venue", VENUES)
def test_clean_fixture_is_certified(
    settings: Settings, venue_frames: Frames, tmp_path: Path, venue: str
) -> None:
    cert = certify(settings, venue_frames(venue), tmp_path)[venue]
    assert cert.status == "certified", failed(cert)
    assert cert.coverage_ratio == pytest.approx(1.0)
    assert cert.schema_version == "1"
    assert cert.capture_regions == ["msk"]
    path = tmp_path / "certs" / f"venue={venue}" / f"symbol={cert.symbol}" / "date=2026-10-20.json"
    assert read_certificate(path) == cert


def test_lost_poll_is_recovered_by_the_trade_cursor(
    settings: Settings, venue_frames: Frames, tmp_path: Path
) -> None:
    clean = certify(settings, venue_frames("moex-algopack"), tmp_path / "a")["moex-algopack"]
    fault = FaultSpec(kind="drop", venue="moex-algopack", index=2)
    frames = apply_fault(venue_frames("moex-algopack"), fault)
    lost = certify(settings, frames, tmp_path / "b")["moex-algopack"]
    # The next poll starts from the last TRADENO seen, so the dropped answer loses no trade.
    assert lost.rows["trades"] == clean.rows["trades"]
    assert lost.status == "certified"


def test_trade_numbers_are_not_consecutive_on_moex(
    settings: Settings, venue_frames: Frames, tmp_path: Path
) -> None:
    # MOEX TRADENO is exchange-wide, so jumps between one security's trades are not gaps.
    c = certify(settings, venue_frames("moex-algopack"), tmp_path)["moex-algopack"]
    assert c.check("trade_id_gaps").value == 0


@pytest.mark.parametrize(("venue", "index"), [("moex-tbank", 5), ("moex-algopack", 3)])
def test_crossed_book(
    settings: Settings, venue_frames: Frames, tmp_path: Path, venue: str, index: int
) -> None:
    frames = apply_fault(venue_frames(venue), FaultSpec(kind="cross", venue=venue, index=index))
    c = certify(settings, frames, tmp_path)[venue]
    assert c.check("crossed_book_events").value == 1
    assert c.status == "degraded"


def test_duplicates_are_removed_by_dedup(
    settings: Settings, venue_frames: Frames, tmp_path: Path
) -> None:
    fault = FaultSpec(kind="duplicate", venue="moex-tbank", index=4, delay_ms=3)
    c = certify(settings, apply_fault(venue_frames("moex-tbank"), fault), tmp_path)["moex-tbank"]
    assert c.check("duplicate_messages").value == 0 and c.status == "certified"


def test_duplicates_without_dedup_are_flagged(
    make_settings: Callable[..., Settings], venue_frames: Frames, tmp_path: Path
) -> None:
    s = make_settings(TAPE_QUALITY__COVERAGE_WINDOW="observed", TAPE_DEDUP__ENABLED="false")
    frames = venue_frames("moex-algopack")
    frames = apply_fault(frames, FaultSpec(kind="duplicate", venue="moex-algopack", index=0))
    c = certify(s, frames, tmp_path)["moex-algopack"]
    assert c.check("duplicate_messages").value >= 1


def test_blind_resubscription_is_measured(
    settings: Settings, venue_frames: Frames, tmp_path: Path
) -> None:
    fault = FaultSpec(kind="resubscribe", venue="moex-tbank", index=7, gap_ms=1500)
    frames = apply_fault(venue_frames("moex-tbank"), fault)
    c = certify(settings, frames, tmp_path)["moex-tbank"]
    assert c.resubscription.gap_ms_max >= 1500
    assert not c.check("resubscription_gap_ms_max").passed
    assert any(g.kind == "resubscription" for g in c.gaps)


def test_silence_and_day_window(
    make_settings: Callable[..., Settings], venue_frames: Frames, tmp_path: Path
) -> None:
    s = make_settings(TAPE_QUALITY__SILENCE_S__BOOK_SNAPSHOT="0.4")
    c = certify(s, venue_frames("moex-tbank"), tmp_path)["moex-tbank"]
    assert any(g.kind == "silence" for g in c.gaps)
    # With the full session window a few seconds of data covers almost nothing: hard failure.
    assert c.coverage_ratio < 0.01 and c.status == "failed"


def test_moex_session_window_and_event_skew(
    make_settings: Callable[..., Settings], venue_frames: Frames, tmp_path: Path
) -> None:
    c = certify(make_settings(), venue_frames("moex-algopack"), tmp_path)["moex-algopack"]
    hours = (c.window_end_ns - c.window_start_ns) / 3.6e12
    assert hours == pytest.approx(17.0)  # 06:50-23:50 Moscow time
    # Candles and Super Candles carry bar times; only event channels enter the skew.
    assert c.clock_skew is not None and 0 < c.clock_skew.p50_ms < 5000
