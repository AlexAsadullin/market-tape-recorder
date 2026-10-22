"""``make report``: run the report experiments and write their tables.

Outputs go to ``$TRADEQ_DATA_DIR/market-tape-recorder/reports/``:

- ``tables.md``: the tables that docs/report.md cites, every rate with its 95% Wilson interval and
  ``n``, every mean with its standard error and 95% bootstrap interval, and the named tests;
- ``results.json``: the same numbers in machine-readable form;
- ``detection.csv``, ``rotation.csv``, ``regions.csv``: one row per run.

``--smoke`` runs a few runs per cell, for a quick check that everything works.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import shutil
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import orjson

from experiments.config import ReportConfig, load_report_config
from experiments.detection import EXPECTED, Outcome, run_detection
from experiments.regions import RegionRun, run_regions
from experiments.rotation import RotationRun, run_rotation
from experiments.stats import Mean, Rate, bootstrap_mean, mann_whitney, two_proportion_z, wilson
from tape_recorder.logging_setup import configure_logging
from tape_recorder.settings import Settings, load_settings

WORK_SUBDIR = "report-work"
PATH_FIELDS = ("tape", "captures", "staging", "certificates", "latency", "archives", "raw")


def report_settings(settings: Settings) -> Settings:
    """Same settings with every output path under ``report-work/`` and an observed window."""
    paths = settings.paths.model_copy(
        update={name: f"{WORK_SUBDIR}/{getattr(settings.paths, name)}" for name in PATH_FIELDS}
    )
    quality = settings.quality.model_copy(update={"coverage_window": "observed"})
    return settings.model_copy(update={"paths": paths, "quality": quality})


def fmt_rate(r: Rate) -> str:
    return f"{r.p:.3f} [{r.lo:.3f}, {r.hi:.3f}]"


def fmt_mean(m: Mean, digits: int = 1) -> str:
    return f"{m.mean:.{digits}f} [{m.lo:.{digits}f}, {m.hi:.{digits}f}]"


def rate_of(rows: Sequence[Any], pick: Callable[[Any], bool | None]) -> Rate:
    values = [v for v in (pick(r) for r in rows) if v is not None]
    return wilson(sum(values), len(values))


def detection_tables(rows: list[Outcome], cfg: ReportConfig) -> tuple[list[str], dict[str, Any]]:
    faults = cfg.detection.faults
    lines = [
        "## Table 1. Detection rate per fault, all venues pooled",
        "",
        "Share of runs in which the method noticed the injected fault; for clean streams, the",
        "share of false alarms. Each cell: rate [95% Wilson interval]; n runs per cell below.",
        "",
        "| Fault | n | Certificate | Gap monitor (3x median) | Two-proportion z, cert vs gap |",
        "|---|---|---|---|---|",
    ]
    data: dict[str, Any] = {"pooled": {}, "binance": {}, "per_venue": {}}
    for fault in faults:
        sel = [r for r in rows if r.fault == fault]
        cert = rate_of(sel, lambda r: r.certificate)
        gap = rate_of(sel, lambda r: r.gap_monitor)
        test = two_proportion_z(cert.k, cert.n, gap.k, gap.n)
        lines.append(
            f"| {fault} | {cert.n} | {fmt_rate(cert)} | {fmt_rate(gap)} | "
            f"z = {test.statistic:.2f}, p = {test.p_value:.3g} |"
        )
        data["pooled"][fault] = {
            "certificate": cert.to_json(),
            "gap_monitor": gap.to_json(),
            "z_test": test.to_json(),
        }
    lines += [
        "",
        "## Table 2. Binance spot: certificate, gap monitor and cryptofeed 3.0.1, same frames",
        "",
        "| Fault | n | Certificate | Gap monitor | cryptofeed (cross_check) | "
        "Two-proportion z, cert vs cryptofeed |",
        "|---|---|---|---|---|---|",
    ]
    for fault in faults:
        sel = [r for r in rows if r.fault == fault and r.venue == "binance"]
        if not sel:
            continue
        cert = rate_of(sel, lambda r: r.certificate)
        gap = rate_of(sel, lambda r: r.gap_monitor)
        cf = rate_of(sel, lambda r: r.cryptofeed)
        test = two_proportion_z(cert.k, cert.n, cf.k, cf.n)
        lines.append(
            f"| {fault} | {cert.n} | {fmt_rate(cert)} | {fmt_rate(gap)} | {fmt_rate(cf)} | "
            f"z = {test.statistic:.2f}, p = {test.p_value:.3g} |"
        )
        data["binance"][fault] = {
            "certificate": cert.to_json(),
            "gap_monitor": gap.to_json(),
            "cryptofeed": cf.to_json(),
            "z_test": test.to_json(),
        }
    lines += [
        "",
        "## Table 3. Certificate detection rate per venue",
        "",
        "| Fault | " + " | ".join(cfg.detection.venues) + " |",
        "|---|" + "---|" * len(cfg.detection.venues),
    ]
    for fault in faults:
        cells = []
        for venue in cfg.detection.venues:
            r = rate_of([o for o in rows if o.fault == fault and o.venue == venue], _cert)
            cells.append(f"{fmt_rate(r)} (n={r.n})")
            data["per_venue"].setdefault(fault, {})[venue] = r.to_json()
        lines.append(f"| {fault} | " + " | ".join(cells) + " |")
    dup = [r for r in rows if r.fault == "duplicate"]
    dedup = rate_of(dup, lambda r: r.dedup_clean)
    resub = [r.resubscription_gap_ms for r in rows if r.fault == "resubscribe"]
    resub_mean = bootstrap_mean(resub, cfg.bootstrap_resamples, cfg.seed)
    lines += [
        "",
        "## Table 4. Side measurements",
        "",
        "| Measurement | Value [95% interval] | n |",
        "|---|---|---|",
        f"| Duplicate runs that the deduplicator removed fully (day certified) | "
        f"{fmt_rate(dedup)} | {dedup.n} |",
        f"| Measured blind interval of an injected re-subscription, mean ms (SE "
        f"{resub_mean.se:.1f}) | {fmt_mean(resub_mean)} | {resub_mean.n} |",
        "",
        "Expected check per fault: "
        + ", ".join(f"{k} -> {v}" for k, v in EXPECTED.items())
        + "; clean: any status other than certified.",
    ]
    data["dedup_clean"] = dedup.to_json()
    data["resubscribe_gap_ms"] = resub_mean.to_json()
    return lines, data


def _cert(r: Outcome) -> bool:
    return r.certificate


def rotation_tables(rows: list[RotationRun], cfg: ReportConfig) -> tuple[list[str], dict[str, Any]]:
    modes = list(cfg.rotation.modes)
    lines = [
        "## Table 5. Planned reconnects of a live session (fake broadcast venue)",
        "",
        f"{cfg.rotation.runs} runs per mode, each {cfg.rotation.duration_s:g} s with a rotation "
        f"every {cfg.rotation.rotate_after_s:g} s and a connect latency of "
        f"{cfg.rotation.connect_ms[0]:g} to {cfg.rotation.connect_ms[1]:g} ms. Means over runs "
        "with standard error (SE) and 95% bootstrap interval.",
        "",
        "| Mode | Runs | Rotations per run | Blind ms per rotation | Missing trades per rotation "
        "| Runs with zero missing trades |",
        "|---|---|---|---|---|---|",
    ]
    data: dict[str, Any] = {"modes": {}, "tests": {}}
    per_mode: dict[str, list[RotationRun]] = {m: [r for r in rows if r.mode == m] for m in modes}
    for i, m in enumerate(modes):
        sel = per_mode[m]
        seed = cfg.seed + i
        rot = bootstrap_mean([float(r.rotations) for r in sel], cfg.bootstrap_resamples, seed)
        blind = bootstrap_mean(
            [r.blind_ms_per_rotation for r in sel], cfg.bootstrap_resamples, seed
        )
        miss = bootstrap_mean([r.missing_per_rotation for r in sel], cfg.bootstrap_resamples, seed)
        zero = rate_of(sel, lambda r: r.missing_trades == 0)
        lines.append(
            f"| {m} | {len(sel)} | {fmt_mean(rot)} | {fmt_mean(blind)} (SE {blind.se:.1f}) | "
            f"{fmt_mean(miss)} (SE {miss.se:.1f}) | {fmt_rate(zero)} |"
        )
        data["modes"][m] = {
            "rotations": rot.to_json(),
            "blind_ms_per_rotation": blind.to_json(),
            "missing_per_rotation": miss.to_json(),
            "zero_missing": zero.to_json(),
        }
    if "naive" in per_mode:
        lines += [
            "",
            "Mann-Whitney U test against the naive mode, H0: the two modes have the same "
            "distribution of blind ms per rotation (critical |z| = 1.96 at 95%).",
            "",
            "| Mode | z (blind ms) | p | z (missing trades) | p |",
            "|---|---|---|---|---|",
        ]
        base = per_mode["naive"]
        for m in modes:
            if m == "naive":
                continue
            tb = mann_whitney(
                [r.blind_ms_per_rotation for r in per_mode[m]],
                [r.blind_ms_per_rotation for r in base],
            )
            tm = mann_whitney(
                [r.missing_per_rotation for r in per_mode[m]],
                [r.missing_per_rotation for r in base],
            )
            lines.append(
                f"| {m} | {tb.statistic:.2f} | {tb.p_value:.3g} | {tm.statistic:.2f} | "
                f"{tm.p_value:.3g} |"
            )
            data["tests"][m] = {"blind_ms": tb.to_json(), "missing": tm.to_json()}
    return lines, data


def regions_tables(rows: list[RegionRun], cfg: ReportConfig) -> tuple[list[str], dict[str, Any]]:
    captures = [*cfg.regions.regions, "merged"]
    lines = [
        "## Table 6. Two capture regions merged by earliest arrival",
        "",
        f"{cfg.regions.runs} runs of a {cfg.regions.venue} stream with "
        f"{cfg.regions.stream.messages} deltas; each region drops each delta with probability "
        f"{cfg.regions.drop_probability:g} and has one blind re-subscription of "
        f"{cfg.regions.outage_gap_ms[0]:g} to {cfg.regions.outage_gap_ms[1]:g} ms.",
        "",
        "| Capture | Runs | Sequence gaps per day | Longest blind interval, ms | Days certified |",
        "|---|---|---|---|---|",
    ]
    data: dict[str, Any] = {}
    for i, c in enumerate(captures):
        sel = [r for r in rows if r.capture == c]
        gaps = bootstrap_mean(
            [float(r.sequence_gaps) for r in sel], cfg.bootstrap_resamples, cfg.seed + i
        )
        blind = bootstrap_mean([r.blind_ms_max for r in sel], cfg.bootstrap_resamples, cfg.seed + i)
        cert = rate_of(sel, lambda r: r.certified)
        lines.append(
            f"| {c} | {len(sel)} | {fmt_mean(gaps, 2)} (SE {gaps.se:.2f}) | "
            f"{fmt_mean(blind)} (SE {blind.se:.1f}) | {fmt_rate(cert)} |"
        )
        data[c] = {
            "sequence_gaps": gaps.to_json(),
            "blind_ms_max": blind.to_json(),
            "certified": cert.to_json(),
        }
    first = captures[0]
    a = rate_of([r for r in rows if r.capture == "merged"], lambda r: r.certified)
    b = rate_of([r for r in rows if r.capture == first], lambda r: r.certified)
    test = two_proportion_z(a.k, a.n, b.k, b.n)
    lines += [
        "",
        f"Two-proportion z-test, days certified, merged vs {first}: z = {test.statistic:.2f}, "
        f"p = {test.p_value:.3g} (critical |z| = 1.96).",
    ]
    data["z_test_certified_merged_vs_" + first] = test.to_json()
    return lines, data


def write_csv(path: Path, rows: Sequence[Any]) -> None:
    if not rows:
        return
    fields = [f.name for f in dataclasses.fields(rows[0])]
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(fields)
        for r in rows:
            w.writerow([getattr(r, f) for f in fields])


def smoke(cfg: ReportConfig) -> ReportConfig:
    return cfg.model_copy(
        update={
            "bootstrap_resamples": 200,
            "detection": cfg.detection.model_copy(update={"runs": 3}),
            "rotation": cfg.rotation.model_copy(update={"runs": 2, "duration_s": 1.0}),
            "regions": cfg.regions.model_copy(update={"runs": 3}),
        }
    )


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--smoke", action="store_true", help="a few runs per cell only")
    parser.add_argument(
        "--only", choices=["detection", "rotation", "regions"], action="append", default=None
    )
    args = parser.parse_args(argv)
    base = load_settings()
    configure_logging("WARNING", False)
    cfg = load_report_config()
    if args.smoke:
        cfg = smoke(cfg)
    settings = report_settings(base)
    work = settings.project_root / WORK_SUBDIR
    out = settings.project_root / cfg.reports_subdir
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    out.mkdir(parents=True, exist_ok=True)
    selected = args.only or ["detection", "rotation", "regions"]
    tables: list[str] = [
        "# market-tape-recorder report tables",
        "",
        f"Generated by make report{' --smoke' if args.smoke else ''}; seed {cfg.seed}; "
        f"{cfg.bootstrap_resamples} bootstrap resamples. All data are synthetic.",
        "",
    ]
    results: dict[str, Any] = {"seed": cfg.seed, "smoke": args.smoke, "config": cfg.model_dump()}
    timings: dict[str, float] = {}
    if "detection" in selected:
        t0 = time.monotonic()
        det = run_detection(settings, cfg.streams, cfg.detection, cfg.seed, work)
        timings["detection"] = time.monotonic() - t0
        write_csv(out / "detection.csv", det)
        lines, results["detection"] = detection_tables(det, cfg)
        tables += [*lines, ""]
        print(f"detection: {len(det)} runs in {timings['detection']:.0f} s")
    if "rotation" in selected:
        t0 = time.monotonic()
        rot = run_rotation(settings, cfg.rotation, cfg.seed, work)
        timings["rotation"] = time.monotonic() - t0
        write_csv(out / "rotation.csv", rot)
        lines, results["rotation"] = rotation_tables(rot, cfg)
        tables += [*lines, ""]
        print(f"rotation: {len(rot)} runs in {timings['rotation']:.0f} s")
    if "regions" in selected:
        t0 = time.monotonic()
        reg = run_regions(settings, cfg.regions, cfg.seed, work)
        timings["regions"] = time.monotonic() - t0
        write_csv(out / "regions.csv", reg)
        lines, results["regions"] = regions_tables(reg, cfg)
        tables += [*lines, ""]
        print(f"regions: {len(reg)} rows in {timings['regions']:.0f} s")
    results["timings_s"] = timings
    shutil.rmtree(work, ignore_errors=True)
    (out / "tables.md").write_text("\n".join(tables))
    (out / "results.json").write_bytes(orjson.dumps(results, option=orjson.OPT_INDENT_2))
    print("\n".join(tables))
    print(f"outputs: {out}")


if __name__ == "__main__":
    main()
