from __future__ import annotations

import random
import tarfile
from collections.abc import Callable
from pathlib import Path

import orjson
import pyarrow.compute as pc
import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from tape_recorder.archive import build_archive
from tape_recorder.cli import app
from tape_recorder.demo import run_demo, summary
from tape_recorder.faults import apply_fault, as_region
from tape_recorder.frames import Frame
from tape_recorder.merge import LatencyReport, merge_regions
from tape_recorder.monitor import StreamMonitor
from tape_recorder.quality.certify import certify_tree
from tape_recorder.quality.reader import list_partitions, read_partition
from tape_recorder.replay import merge_streams, replay_frames
from tape_recorder.schema import Channel
from tape_recorder.service import create_app
from tape_recorder.settings import FaultSpec, Settings
from tape_recorder.timeutil import NS_PER_MS

Frames = Callable[[str], list[Frame]]
FIXTURES = Path(__file__).parents[1] / "fixtures" / "venues"


def two_regions(
    settings: Settings, venue_frames: Frames, venue: str, eu_faults: list[FaultSpec]
) -> tuple[Path, Path]:
    base = venue_frames(venue)
    eu = base
    for f in eu_faults:
        eu = apply_fault(eu, f)
    roots = []
    for region, frames, lat in (("eu", eu, 5.0), ("tokyo", base, 40.0)):
        root = settings.capture_root(region)
        replay_frames(settings, as_region(frames, region, lat, 2.0, random.Random(3)), region, root)
        roots.append(root)
    return roots[0], roots[1]


def test_merge_keeps_earliest_and_fills_gaps(settings: Settings, venue_frames: Frames) -> None:
    eu, tokyo = two_regions(
        settings, venue_frames, "binance", [FaultSpec(kind="drop", venue="binance", index=6)]
    )
    eu_cert = certify_tree(settings, eu, eu / "certs")[0][0]
    assert eu_cert.check("sequence_gaps").value == 1
    result = merge_regions(settings, eu, tokyo, settings.tape_root)
    assert result.rows_out < result.rows_in
    ((cert, _),) = certify_tree(settings, settings.tape_root, settings.certificates_root)
    assert cert.status == "certified" and cert.capture_regions == ["eu", "tokyo"]
    key = next(k for k in list_partitions(settings.tape_root) if k.channel == Channel.BOOK_DELTA)
    merged = read_partition(settings.tape_root, key)
    regions = merged.column("capture_region").to_pylist()
    assert regions.count("tokyo") >= 1 and regions.count("eu") > regions.count("tokyo")
    # A book message is never split across regions.
    by_seq: dict[int, set[str]] = {}
    for seq, region in zip(merged.column("seq").to_pylist(), regions, strict=True):
        by_seq.setdefault(seq, set()).add(region)
    assert all(len(v) == 1 for v in by_seq.values())
    report = LatencyReport.model_validate_json(result.latency_reports[0].read_bytes())
    assert report.win_share["eu"] > 0.9
    lead = report.lead_ms["eu-minus-tokyo"]
    assert lead.quantiles_ms["0.5"] == pytest.approx(-35.0, abs=3.0)
    assert set(report.exchange_latency_ms) == {"eu", "tokyo"}
    assert result.latency_reports[0].with_name("date=2026-10-20.arrivals.parquet").exists()


def test_unmatchable_rows_come_from_primary_region(
    make_settings: Callable[..., Settings], venue_frames: Frames
) -> None:
    s = make_settings(TAPE_QUALITY__COVERAGE_WINDOW="observed", TAPE_MERGE__PRIMARY_REGION="tokyo")
    eu, tokyo = two_regions(s, venue_frames, "hyperliquid", [])
    merge_regions(s, eu, tokyo, s.tape_root)
    key = next(k for k in list_partitions(s.tape_root) if k.channel == Channel.FUNDING)
    t = read_partition(s.tape_root, key)
    assert set(t.column("capture_region").to_pylist()) == {"tokyo"}
    assert pc.all(pc.is_null(t.column("ts_exchange"))).as_py()


def test_archive_bundle(settings: Settings, venue_frames: Frames) -> None:
    frames = merge_streams([venue_frames(v) for v in ("binance", "okx", "moex")])
    replay_frames(settings, frames, "eu", settings.tape_root)
    result = build_archive(settings, "2026-10", settings.tape_root, settings.certificates_root)
    assert result.instrument_days == 3
    report = result.report_path.read_text()
    assert "| binance | BTC-USDT-SPOT | 2026-10-20 | certified |" in report
    manifest = orjson.loads(result.manifest_path.read_bytes())
    with tarfile.open(result.tar_path) as tar:
        names = set(tar.getnames())
    assert {"QUALITY.md", "manifest.json"} <= names
    assert {f["path"] for f in manifest["files"]} <= names
    assert any(n.startswith("certificates/venue=okx/") for n in names)
    assert not list(result.tar_path.parent.glob(".*inprogress"))
    empty = build_archive(settings, "2026-09", settings.tape_root, settings.certificates_root)
    assert empty.instrument_days == 0


def test_cli_end_to_end(
    settings: Settings, data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TAPE_QUALITY__COVERAGE_WINDOW", "observed")
    runner = CliRunner()
    for region in ("eu", "tokyo"):
        res = runner.invoke(
            app,
            [
                "replay",
                str(FIXTURES / "okx.json"),
                "--region",
                region,
                "--out",
                f"captures/region={region}",
            ],
        )
        assert res.exit_code == 0, res.output
        assert "parse_errors=0" in res.output
    res = runner.invoke(app, ["merge-regions", "eu", "tokyo"])
    assert res.exit_code == 0 and "partitions=" in res.output, res.output
    res = runner.invoke(app, ["certify", "--date", "2026-10-20"])
    assert res.exit_code == 0 and "okx" in res.output and "certified" in res.output
    res = runner.invoke(app, ["certify", "--venue", "nowhere"])
    assert "no instrument-days found" in res.output
    res = runner.invoke(app, ["archive", "--month", "2026-10"])
    assert res.exit_code == 0 and "instrument_days=1" in res.output


def test_status_service() -> None:
    clock_now = [10_000 * NS_PER_MS]
    monitor = StreamMonitor(lambda: clock_now[0], stale_after_s=5)
    client = TestClient(create_app(monitor, "eu"))
    assert client.get("/health").json()["status"] == "starting"
    from tape_recorder.adapters.base import RecordFactory

    f = RecordFactory("okx", "eu", "c1", 9_000 * NS_PER_MS)
    monitor.observe([f.trade("BTC-USDT-PERP", 8_990 * NS_PER_MS, None, "1", 1.0, 1.0, "buy")])
    monitor.connection_opened("okx", "c1")
    body = client.get("/v1/streams").json()
    assert body[0]["lag_ms"] == pytest.approx(10.0) and body[0]["stale"] is False
    assert client.get("/health").json()["status"] == "ok"
    clock_now[0] += 10_000 * NS_PER_MS
    health = client.get("/health").json()
    assert (health["status"], health["stale_streams"]) == ("degraded", 1)
    assert client.get("/v1/venues").json()[0]["active_connections"] == ["c1"]


def test_reproduce_demo(settings: Settings) -> None:
    results = run_demo(settings)
    assert {c.status for c in results["merged"]} == {"certified"}
    eu = {c.venue: c for c in results["eu"]}
    tokyo = {c.venue: c for c in results["tokyo"]}
    assert not eu["binance"].check("sequence_gaps").passed
    assert not eu["okx"].check("resubscription_gap_ms_max").passed
    assert not tokyo["bybit"].check("crossed_book_events").passed
    assert not tokyo["binance-usdm"].check("out_of_order_messages").passed
    text = summary(settings, results)
    assert "merged" in text and "first arrival" in text


def test_fault_validation(venue_frames: Frames) -> None:
    frames = venue_frames("moex")
    with pytest.raises(IndexError):
        apply_fault(frames, FaultSpec(kind="drop", venue="moex", index=99))
    with pytest.raises(IndexError):
        apply_fault(frames, FaultSpec(kind="reorder", venue="moex", index=len(frames) - 1))
    with pytest.raises(ValueError):
        apply_fault(frames, FaultSpec(kind="cross", venue="moex", index=0))
