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
VENUES = ["binance", "binance-usdm", "bybit", "okx", "hyperliquid", "moex"]


def certify(settings: Settings, frames: list[Frame], root: Path) -> dict[str, Certificate]:
    replay_frames(settings, frames, "eu", root)
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
    assert cert.capture_regions == ["eu"]
    path = tmp_path / "certs" / f"venue={venue}" / f"symbol={cert.symbol}" / "date=2026-10-20.json"
    assert read_certificate(path) == cert


@pytest.mark.parametrize("venue", ["binance", "bybit", "okx"])
def test_snapshots_match_reconstruction(
    settings: Settings, venue_frames: Frames, tmp_path: Path, venue: str
) -> None:
    cert = certify(settings, venue_frames(venue), tmp_path)[venue]
    assert cert.book.snapshots_compared >= 1 and cert.book.snapshot_mismatches == 0


def test_dropped_delta_is_a_sequence_gap(
    settings: Settings, venue_frames: Frames, tmp_path: Path
) -> None:
    frames = apply_fault(venue_frames("binance"), FaultSpec(kind="drop", venue="binance", index=6))
    cert = certify(settings, frames, tmp_path)["binance"]
    assert cert.check("sequence_gaps").value == 1
    gap = next(g for g in cert.gaps if g.kind == "sequence")
    assert gap.detail and gap.detail.startswith("resync")  # healed by the REST re-snapshot
    assert cert.coverage_ratio < 0.99 and cert.status != "certified"


def test_dropped_trade_is_a_trade_id_gap(
    settings: Settings, venue_frames: Frames, tmp_path: Path
) -> None:
    frames = apply_fault(venue_frames("binance"), FaultSpec(kind="drop", venue="binance", index=5))
    cert = certify(settings, frames, tmp_path)["binance"]
    assert cert.check("trade_id_gaps").value == 1
    assert any(g.kind == "trade_id" for g in cert.gaps)


def test_out_of_order_arrival(settings: Settings, venue_frames: Frames, tmp_path: Path) -> None:
    fault = FaultSpec(kind="reorder", venue="binance-usdm", index=4)
    cert = certify(settings, apply_fault(venue_frames("binance-usdm"), fault), tmp_path)
    c = cert["binance-usdm"]
    assert c.check("out_of_order_messages").value == 1
    assert c.check("sequence_gaps").value == 0  # replay in venue order still reconstructs


def test_crossed_book(settings: Settings, venue_frames: Frames, tmp_path: Path) -> None:
    frames = apply_fault(venue_frames("bybit"), FaultSpec(kind="cross", venue="bybit", index=5))
    c = certify(settings, frames, tmp_path)["bybit"]
    assert c.check("crossed_book_events").value >= 1
    assert c.check("snapshot_mismatches").value == 1  # corruption persists until the snapshot


def test_crossed_snapshot_on_unsequenced_venue(
    settings: Settings, venue_frames: Frames, tmp_path: Path
) -> None:
    fault = FaultSpec(kind="cross", venue="hyperliquid", index=0)
    c = certify(settings, apply_fault(venue_frames("hyperliquid"), fault), tmp_path)
    assert c["hyperliquid"].check("crossed_book_events").value == 1


def test_duplicates_are_removed_by_dedup(
    settings: Settings, venue_frames: Frames, tmp_path: Path
) -> None:
    fault = FaultSpec(kind="duplicate", venue="bybit", index=2, delay_ms=3)
    c = certify(settings, apply_fault(venue_frames("bybit"), fault), tmp_path)["bybit"]
    assert c.check("duplicate_messages").value == 0 and c.status == "certified"


def test_duplicates_without_dedup_are_flagged(
    make_settings: Callable[..., Settings], venue_frames: Frames, tmp_path: Path
) -> None:
    s = make_settings(TAPE_QUALITY__COVERAGE_WINDOW="observed", TAPE_DEDUP__ENABLED="false")
    frames = venue_frames("binance")
    for idx in (5, 1):  # a trade, then a depth update
        frames = apply_fault(frames, FaultSpec(kind="duplicate", venue="binance", index=idx))
    c = certify(s, frames, tmp_path)["binance"]
    assert c.check("duplicate_messages").value == 2
    assert c.book.stale_messages >= 1


def test_blind_resubscription_is_measured(
    settings: Settings, venue_frames: Frames, tmp_path: Path
) -> None:
    fault = FaultSpec(kind="resubscribe", venue="okx", index=8, gap_ms=1500)
    c = certify(settings, apply_fault(venue_frames("okx"), fault), tmp_path)["okx"]
    assert c.resubscription.gap_ms_max >= 1500
    assert not c.check("resubscription_gap_ms_max").passed
    assert any(g.kind == "resubscription" for g in c.gaps)
    assert c.check("sequence_gaps").value >= 1


def test_silence_and_day_window(
    make_settings: Callable[..., Settings], venue_frames: Frames, tmp_path: Path
) -> None:
    s = make_settings(TAPE_QUALITY__SILENCE_S__BOOK_DELTA="0.15")
    c = certify(s, venue_frames("okx"), tmp_path)["okx"]
    assert any(g.kind == "silence" for g in c.gaps)
    # With the full-day window a few seconds of data covers almost nothing: hard failure.
    assert c.coverage_ratio < 0.01 and c.status == "failed"


def test_moex_session_window(
    make_settings: Callable[..., Settings], venue_frames: Frames, tmp_path: Path
) -> None:
    c = certify(make_settings(), venue_frames("moex"), tmp_path)["moex"]
    hours = (c.window_end_ns - c.window_start_ns) / 3.6e12
    assert hours == pytest.approx(17.0)  # 06:50-23:50 Moscow time
    assert c.clock_skew is not None and c.clock_skew.p50_ms > 0
