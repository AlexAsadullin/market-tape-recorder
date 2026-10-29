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
from tape_recorder.monitor import LoadMonitor, StreamMonitor
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
        settings,
        venue_frames,
        "moex-tbank",
        [FaultSpec(kind="resubscribe", venue="moex-tbank", index=7, gap_ms=1500)],
    )
    eu_cert = certify_tree(settings, eu, eu / "certs")[0][0]
    assert not eu_cert.check("resubscription_gap_ms_max").passed
    result = merge_regions(settings, eu, tokyo, settings.tape_root)
    assert result.rows_out < result.rows_in
    ((cert, _),) = certify_tree(settings, settings.tape_root, settings.certificates_root)
    assert cert.status == "certified" and cert.capture_regions == ["eu", "tokyo"]
    key = next(k for k in list_partitions(settings.tape_root) if k.channel == Channel.BOOK_SNAPSHOT)
    merged = read_partition(settings.tape_root, key)
    regions = merged.column("capture_region").to_pylist()
    assert regions.count("tokyo") >= 1 and regions.count("eu") > regions.count("tokyo")
    # A book snapshot is never split across regions.
    by_id: dict[str, set[str]] = {}
    for snap_id, region in zip(merged.column("snapshot_id").to_pylist(), regions, strict=True):
        by_id.setdefault(snap_id, set()).add(region)
    assert all(len(v) == 1 for v in by_id.values())
    report = LatencyReport.model_validate_json(result.latency_reports[0].read_bytes())
    assert report.win_share["eu"] > 0.9
    lead = report.lead_ms["eu-minus-tokyo"]
    assert lead.quantiles_ms["0.5"] == pytest.approx(-35.0, abs=3.0)
    assert result.latency_reports[0].with_name("date=2026-10-20.arrivals.parquet").exists()


def test_unmatchable_rows_come_from_primary_region(
    make_settings: Callable[..., Settings], venue_frames: Frames
) -> None:
    s = make_settings(TAPE_QUALITY__COVERAGE_WINDOW="observed", TAPE_MERGE__PRIMARY_REGION="tokyo")

    def without_status_time(frames: list[Frame]) -> list[Frame]:
        out = []
        for f in frames:
            if f.request is not None and f.request.tag == "marketdata":
                doc = orjson.loads(f.payload)
                table = doc["marketdata"]
                keep = [i for i, c in enumerate(table["columns"]) if c != "UPDATETIME"]
                table["columns"] = [table["columns"][i] for i in keep]
                table["data"] = [[row[i] for i in keep] for row in table["data"]]
                f = f.with_(payload=orjson.dumps(doc).decode())
            out.append(f)
        return out

    eu, tokyo = two_regions(s, lambda v: without_status_time(venue_frames(v)), "moex-algopack", [])
    merge_regions(s, eu, tokyo, s.tape_root)
    key = next(k for k in list_partitions(s.tape_root) if k.channel == Channel.TRADING_STATUS)
    t = read_partition(s.tape_root, key)
    assert set(t.column("capture_region").to_pylist()) == {"tokyo"}
    assert pc.all(pc.is_null(t.column("ts_exchange"))).as_py()


def test_archive_bundle(settings: Settings, venue_frames: Frames) -> None:
    frames = merge_streams([venue_frames(v) for v in ("moex-tbank", "moex-algopack")])
    replay_frames(settings, frames, "msk", settings.tape_root)
    result = build_archive(settings, "2026-10", settings.tape_root, settings.certificates_root)
    assert result.instrument_days == 2
    report = result.report_path.read_text()
    assert "| moex-tbank | SBER-RUB-TQBR | 2026-10-20 | certified |" in report
    manifest = orjson.loads(result.manifest_path.read_bytes())
    with tarfile.open(result.tar_path) as tar:
        names = set(tar.getnames())
    assert {"QUALITY.md", "manifest.json"} <= names
    assert {f["path"] for f in manifest["files"]} <= names
    assert any(n.startswith("certificates/venue=moex-algopack/") for n in names)
    assert any("channel=obstats" in n for n in names)
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
                str(FIXTURES / "moex-algopack.json"),
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
    assert res.exit_code == 0 and "moex-algopack" in res.output and "certified" in res.output
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

    f = RecordFactory("moex-tbank", "msk", "c1", 9_000 * NS_PER_MS)
    monitor.observe([f.trade("SBER-RUB-TQBR", 8_990 * NS_PER_MS, None, "1", 1.0, 1.0, "buy")])
    monitor.connection_opened("moex-tbank", "c1")
    body = client.get("/v1/streams").json()
    assert body[0]["lag_ms"] == pytest.approx(10.0) and body[0]["stale"] is False
    assert client.get("/health").json()["status"] == "ok"
    clock_now[0] += 10_000 * NS_PER_MS
    health = client.get("/health").json()
    assert (health["status"], health["stale_streams"]) == ("degraded", 1)
    assert client.get("/v1/venues").json()[0]["active_connections"] == ["c1"]
    assert client.get("/v1/load").status_code == 404  # no load monitor wired


def test_load_endpoint_reports_pressure() -> None:
    backlog = [0]
    load = LoadMonitor(lambda: backlog[0], lag_limit_ms=100.0, buffered_limit_rows=10, window=4)
    client = TestClient(create_app(StreamMonitor(lambda: 0, 5), "msk", load))
    assert client.get("/v1/load").json()["pressure"] == "ok"
    load.observe_lag(150 * NS_PER_MS)
    body = client.get("/v1/load").json()
    assert body["pressure"] == "high" and body["loop_lag_ms_max"] == pytest.approx(150.0)
    for _ in range(4):  # the late sample leaves the window
        load.observe_lag(0)
    assert client.get("/v1/load").json()["pressure"] == "ok"
    backlog[0] = 11
    assert client.get("/v1/load").json()["pressure"] == "high"


def test_reproduce_demo(settings: Settings) -> None:
    results = run_demo(settings)
    assert {c.status for c in results["merged"]} == {"certified"}
    msk = {c.venue: c for c in results["msk"]}
    eu = {c.venue: c for c in results["eu"]}
    assert not msk["moex-tbank"].check("resubscription_gap_ms_max").passed
    assert msk["moex-algopack"].status == "certified"  # the lost poll is recovered
    assert not eu["moex-tbank"].check("crossed_book_events").passed
    assert not eu["moex-algopack"].check("crossed_book_events").passed
    text = summary(settings, results)
    assert "merged" in text and "first arrival" in text


def test_fault_validation(venue_frames: Frames) -> None:
    frames = venue_frames("moex-algopack")
    with pytest.raises(IndexError):
        apply_fault(frames, FaultSpec(kind="drop", venue="moex-algopack", index=99))
    with pytest.raises(IndexError):
        last = len(frames) - 1
        apply_fault(frames, FaultSpec(kind="reorder", venue="moex-algopack", index=last))
    with pytest.raises(ValueError):  # a trades answer has no order book
        apply_fault(frames, FaultSpec(kind="cross", venue="moex-algopack", index=0))
