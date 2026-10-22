"""Experiment 3: two capture regions with independent losses, merged by earliest arrival.

For every run one synthetic stream (:mod:`experiments.synthetic`) is seen by two regions. Each
region loses every delta frame independently with ``drop_probability`` and has one blind
re-subscription of ``outage_gap_ms`` somewhere in ``outage_window``; then the region adds its own
latency and jitter (:func:`tape_recorder.faults.as_region`). Each region is replayed and certified
on its own (the single-region baseline), then :func:`tape_recorder.merge.merge_regions` builds the
merged tape, which is certified again.
"""

from __future__ import annotations

import random
import shutil
from dataclasses import dataclass
from pathlib import Path

from experiments.config import RegionsConfig
from experiments.synthetic import is_delta, venue_frames
from tape_recorder.faults import apply_fault, as_region
from tape_recorder.frames import Frame
from tape_recorder.merge import merge_regions
from tape_recorder.quality.certificate import Certificate
from tape_recorder.quality.certify import certify_tree
from tape_recorder.replay import replay_frames
from tape_recorder.settings import FaultSpec, Settings


@dataclass(frozen=True, slots=True)
class RegionRun:
    run: int
    capture: str  # a region name or "merged"
    sequence_gaps: int
    blind_ms_max: float
    certified: bool
    dropped_frames: int


def region_view(
    frames: list[Frame], region: str, cfg: RegionsConfig, rng: random.Random
) -> tuple[list[Frame], int]:
    """The stream as one region receives it, and the number of frames it lost to drops."""
    kept = [f for f in frames if not (is_delta(f) and rng.random() < cfg.drop_probability)]
    dropped = len(frames) - len(kept)
    lo, hi = (round(x * len(kept)) for x in cfg.outage_window)
    index = rng.choice([i for i in range(lo, hi) if is_delta(kept[i])])
    outage = FaultSpec(
        kind="resubscribe",
        venue=cfg.venue,
        index=index,
        gap_ms=rng.uniform(*cfg.outage_gap_ms),
        delay_ms=0.0,
    )
    lat = cfg.regions[region]
    seen = as_region(apply_fault(kept, outage), region, lat.latency_ms, lat.jitter_ms, rng)
    return seen, dropped


def _one(cert: Certificate, run: int, capture: str, dropped: int) -> RegionRun:
    return RegionRun(
        run=run,
        capture=capture,
        sequence_gaps=round(cert.check("sequence_gaps").value),
        blind_ms_max=cert.resubscription.gap_ms_max,
        certified=cert.status == "certified",
        dropped_frames=dropped,
    )


def run_regions(settings: Settings, cfg: RegionsConfig, seed: int, work: Path) -> list[RegionRun]:
    names = list(cfg.regions)
    if len(names) != 2:
        raise ValueError("the regions experiment needs exactly two regions")
    out: list[RegionRun] = []
    for run in range(cfg.runs):
        base = venue_frames(cfg.venue, cfg.stream, seed + run)
        roots: list[Path] = []
        for name in names:
            rng = random.Random(f"{seed}-regions-{name}-{run}")
            frames, dropped = region_view(base, name, cfg, rng)
            root = work / f"regions-{run}-{name}"
            replay_frames(settings, frames, name, root)
            (cert,) = [c for c, _ in certify_tree(settings, root, root / "certs", venue=cfg.venue)]
            out.append(_one(cert, run, name, dropped))
            roots.append(root)
        merged = work / f"regions-{run}-merged"
        merge_regions(settings, roots[0], roots[1], merged)
        (cert,) = [c for c, _ in certify_tree(settings, merged, merged / "certs", venue=cfg.venue)]
        out.append(_one(cert, run, "merged", 0))
        for root in [*roots, merged]:
            shutil.rmtree(root)
    return out
