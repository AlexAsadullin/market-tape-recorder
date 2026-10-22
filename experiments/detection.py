"""Experiment 1: does each method notice one injected fault per synthetic stream?

For every (venue, fault, run) the stream of :mod:`experiments.synthetic` gets one fault from
:mod:`tape_recorder.faults`, and three methods look at it:

1. The certificate: the frames are replayed through the recorder (adapters, dedup, writer) and
   certified; the fault counts as detected when the check named in ``EXPECTED`` fails. For a clean
   stream, any status other than ``certified`` is a false alarm.
2. A gap monitor, the simple baseline: it flags a stream whose longest inter-arrival gap exceeds
   ``gap_monitor_ratio`` times the median gap, which is what a recorder without sequence audit
   can see.
3. cryptofeed 3.0.1 (Binance only), see :mod:`experiments.cryptofeed_probe`.

Duplicates are injected with dedup disabled, because with dedup on they never reach the tape.
"""

from __future__ import annotations

import itertools
import random
import shutil
import statistics
from dataclasses import dataclass
from pathlib import Path

from experiments.config import DetectionConfig, FaultName
from experiments.cryptofeed_probe import run_cryptofeed
from experiments.synthetic import StreamSpec, is_delta, venue_frames
from tape_recorder.faults import apply_fault
from tape_recorder.frames import Frame
from tape_recorder.quality.certificate import Certificate
from tape_recorder.quality.certify import certify_tree
from tape_recorder.replay import replay_frames
from tape_recorder.settings import FaultSpec, Settings

EXPECTED: dict[str, str] = {
    "drop": "sequence_gaps",
    "reorder": "out_of_order_messages",
    "cross": "crossed_book_events",
    "resubscribe": "resubscription_gap_ms_max",
    "duplicate": "duplicate_messages",
}


@dataclass(frozen=True, slots=True)
class Outcome:
    venue: str
    fault: str
    run: int
    certificate: bool
    certificate_status: str
    gap_monitor: bool
    cryptofeed: bool | None
    resubscription_gap_ms: float
    coverage_ratio: float
    # Duplicates only: with dedup on, the tape holds no duplicate and the day is certified.
    dedup_clean: bool | None


def inject(
    frames: list[Frame], venue: str, fault: FaultName, cfg: DetectionConfig, rng: random.Random
) -> list[Frame]:
    if fault == "clean":
        return frames
    lo, hi = (round(x * len(frames)) for x in cfg.fault_window)
    candidates = [
        i
        for i in range(lo, hi)
        if is_delta(frames[i]) and (fault != "reorder" or is_delta(frames[i + 1]))
    ]
    spec = FaultSpec(
        kind=fault,
        venue=venue,
        index=rng.choice(candidates),
        gap_ms=rng.uniform(*cfg.resubscribe_gap_ms),
        delay_ms=rng.uniform(*cfg.duplicate_delay_ms),
    )
    return apply_fault(frames, spec)


def gap_monitor(frames: list[Frame], ratio: float) -> bool:
    ts = sorted(f.ts_local for f in frames if f.source == "ws")
    gaps = [b - a for a, b in itertools.pairwise(ts)]
    return max(gaps) > ratio * statistics.median(gaps)


def run_detection(
    settings: Settings,
    streams: dict[str, StreamSpec],
    cfg: DetectionConfig,
    seed: int,
    work: Path,
) -> list[Outcome]:
    no_dedup = settings.model_copy(
        update={"dedup": settings.dedup.model_copy(update={"enabled": False})}
    )
    out: list[Outcome] = []
    for venue in cfg.venues:
        for fault in cfg.faults:
            s = no_dedup if fault == "duplicate" else settings
            for run in range(cfg.runs):
                rng = random.Random(f"{seed}-{venue}-{fault}-{run}")
                frames = inject(
                    venue_frames(venue, streams[venue], seed + run), venue, fault, cfg, rng
                )
                cert = _certify(s, frames, venue, work / f"{venue}-{fault}-{run}")
                dedup_clean = None
                if fault == "duplicate":
                    deduped = _certify(settings, frames, venue, work / f"{venue}-dedup-{run}")
                    dedup_clean = (
                        deduped.check("duplicate_messages").value == 0
                        and deduped.status == "certified"
                    )
                if fault == "clean":
                    detected = cert.status != "certified"
                else:
                    detected = not cert.check(EXPECTED[fault]).passed
                out.append(
                    Outcome(
                        venue=venue,
                        fault=fault,
                        run=run,
                        certificate=detected,
                        certificate_status=cert.status,
                        gap_monitor=gap_monitor(frames, cfg.gap_monitor_ratio),
                        cryptofeed=run_cryptofeed(frames).detected if venue == "binance" else None,
                        resubscription_gap_ms=cert.resubscription.gap_ms_max,
                        coverage_ratio=cert.coverage_ratio,
                        dedup_clean=dedup_clean,
                    )
                )
    return out


def _certify(settings: Settings, frames: list[Frame], venue: str, root: Path) -> Certificate:
    replay_frames(settings, frames, "eu", root)
    (cert,) = [c for c, _ in certify_tree(settings, root, root / "certs", venue=venue)]
    shutil.rmtree(root)
    return cert
