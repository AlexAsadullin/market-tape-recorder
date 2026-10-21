"""``make reproduce``: offline end-to-end demo with two simulated capture regions.

The venue fixtures are replayed as two regions, each with its own latency, jitter and injected
faults (``demo.regions`` in settings). Each region is written to its own capture tree and
certified; then ``merge-regions`` builds the merged tape, which is certified again, and the
monthly archive is built. Outputs go to ``$TRADEQ_DATA_DIR/market-tape-recorder/demo/`` so a real
recording under ``tape/`` is never touched.
"""

from __future__ import annotations

import random
import shutil
from collections import defaultdict
from pathlib import Path

import typer

from tape_recorder.archive import build_archive
from tape_recorder.faults import apply_fault, as_region
from tape_recorder.frames import Frame, iter_frame_files, load_frames
from tape_recorder.logging_setup import configure_logging
from tape_recorder.merge import LatencyReport, merge_regions
from tape_recorder.quality.certificate import Certificate
from tape_recorder.quality.certify import certify_tree
from tape_recorder.replay import merge_streams, replay_frames
from tape_recorder.settings import DEFAULT_CONFIG_FILE, Settings, load_settings
from tape_recorder.timeutil import ns_to_date

DEMO_SUBDIR = "demo"
REPO_ROOT = DEFAULT_CONFIG_FILE.parents[1]


def demo_settings(settings: Settings) -> Settings:
    """Same settings with every output path moved under ``demo/`` and an observed window."""
    paths = settings.paths.model_copy(
        update={
            name: f"{DEMO_SUBDIR}/{getattr(settings.paths, name)}"
            for name in (
                "tape",
                "captures",
                "staging",
                "certificates",
                "latency",
                "archives",
                "raw",
            )
        }
    )
    quality = settings.quality.model_copy(update={"coverage_window": "observed"})
    return settings.model_copy(update={"paths": paths, "quality": quality})


def region_frames(settings: Settings, fixtures: list[Path], region: str) -> list[Frame]:
    cfg = settings.demo.regions[region]
    rng = random.Random(f"{settings.demo.seed}-{region}")
    streams: list[list[Frame]] = []
    for path in fixtures:
        frames = load_frames(path)
        venue = frames[0].venue
        for fault in cfg.faults:
            if fault.venue == venue:
                frames = apply_fault(frames, fault)
        streams.append(as_region(frames, region, cfg.latency_ms, cfg.jitter_ms, rng))
    return merge_streams(streams)


def run_demo(settings: Settings) -> dict[str, list[Certificate]]:
    s = demo_settings(settings)
    shutil.rmtree(s.project_root / DEMO_SUBDIR, ignore_errors=True)
    fixtures_dir = settings.demo.fixtures_dir
    if not fixtures_dir.is_absolute():
        fixtures_dir = REPO_ROOT / fixtures_dir
    fixtures = list(iter_frame_files([fixtures_dir]))
    regions = list(settings.demo.regions)
    if len(regions) != 2:
        raise ValueError("the demo needs exactly two regions")
    results: dict[str, list[Certificate]] = {}
    months: set[str] = set()
    for region in regions:
        frames = region_frames(s, fixtures, region)
        months.update(ns_to_date(f.ts_local).strftime("%Y-%m") for f in frames)
        root = s.capture_root(region)
        replay_frames(s, frames, region, root)
        results[region] = [c for c, _ in certify_tree(s, root, root / "certificates")]
    merge_regions(s, s.capture_root(regions[0]), s.capture_root(regions[1]), s.tape_root)
    results["merged"] = [c for c, _ in certify_tree(s, s.tape_root, s.certificates_root)]
    for month in sorted(months):
        build_archive(s, month, s.tape_root, s.certificates_root)
    return results


def summary(settings: Settings, results: dict[str, list[Certificate]]) -> str:
    s = demo_settings(settings)
    by_instrument: dict[tuple[str, str], dict[str, Certificate]] = defaultdict(dict)
    for name, certs in results.items():
        for one in certs:
            by_instrument[(one.venue, one.symbol)][name] = one
    lines: list[str] = []
    for (venue, symbol), per_run in sorted(by_instrument.items()):
        lines.append(f"{venue} {symbol}")
        for name in results:
            c = per_run.get(name)
            if c is None:
                continue
            failed = ", ".join(ch.name for ch in c.checks if not ch.passed) or "all checks pass"
            lines.append(
                f"  {name:<7} {c.status:<9} coverage={c.coverage_ratio:.3f} "
                f"gaps={len(c.gaps)}  {failed}"
            )
    lines.append("")
    latency_root = s.data_path(s.paths.latency)
    for path in sorted(latency_root.rglob("date=*.json")):
        rep = LatencyReport.model_validate_json(path.read_bytes())
        share = ", ".join(f"{r} {v:.0%}" for r, v in rep.win_share.items())
        pair, lead = next(iter(rep.lead_ms.items()), ("", None))
        median = lead.quantiles_ms.get("0.5") if lead else None
        lead_txt = f"median {pair} {median:+.1f} ms" if median is not None else "no matches"
        lines.append(f"latency {rep.venue:<13} {rep.symbol:<15} first arrival: {share}; {lead_txt}")
    lines.append("")
    lines.append(f"outputs: {s.project_root / DEMO_SUBDIR}")
    return "\n".join(lines)


def main() -> None:
    settings = load_settings()
    configure_logging(settings.logging.level, settings.logging.json_output)
    typer.echo(summary(settings, run_demo(settings)))


if __name__ == "__main__":
    main()
