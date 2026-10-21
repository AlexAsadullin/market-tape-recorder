"""Monthly archive: tape parts and certificates for a small universe, plus a quality report.

The bundle is an uncompressed tar (Parquet parts are already zstd-compressed) that contains the
contract 6.2 tree for the universe, the certificates, ``QUALITY.md`` and ``manifest.json`` with the
SHA-256 of every member. It is written to a temporary name and renamed when complete.
"""

from __future__ import annotations

import hashlib
import io
import tarfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import orjson

from tape_recorder.quality.certificate import Certificate, certificate_path, read_certificate
from tape_recorder.quality.certify import certify_tree
from tape_recorder.quality.reader import instrument_days, part_files
from tape_recorder.schema import SCHEMA_VERSION
from tape_recorder.settings import Settings
from tape_recorder.writer import IN_PROGRESS_SUFFIX, PartitionKey, atomic_write_bytes, fsync_dir


@dataclass(frozen=True, slots=True)
class ArchiveResult:
    tar_path: Path
    report_path: Path
    manifest_path: Path
    instrument_days: int
    files: int


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def quality_report(month: str, certs: list[Certificate]) -> str:
    """Markdown table with one row per instrument-day."""
    lines = [
        f"# Tape quality report, {month}",
        "",
        f"Instrument-days: {len(certs)}. Certified: "
        f"{sum(c.status == 'certified' for c in certs)}, degraded: "
        f"{sum(c.status == 'degraded' for c in certs)}, failed: "
        f"{sum(c.status == 'failed' for c in certs)}.",
        "",
        "| venue | symbol | date | status | coverage | gaps | resub gap max (ms) "
        "| skew p50 (ms) | failed checks |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for c in sorted(certs, key=lambda c: (c.venue, c.symbol, c.date)):
        failed = ", ".join(ch.name for ch in c.checks if not ch.passed) or "none"
        skew = f"{c.clock_skew.p50_ms:.1f}" if c.clock_skew else "n/a"
        lines.append(
            f"| {c.venue} | {c.symbol} | {c.date} | {c.status} | {c.coverage_ratio:.4f} "
            f"| {len(c.gaps)} | {c.resubscription.gap_ms_max:.1f} | {skew} | {failed} |"
        )
    return "\n".join(lines) + "\n"


def build_archive(
    settings: Settings, month: str, tape_root: Path, cert_root: Path
) -> ArchiveResult:
    first = date.fromisoformat(f"{month}-01")
    members: list[tuple[Path, str]] = []
    certs: list[Certificate] = []
    for item in settings.archive.universe:
        for venue, symbol, day in instrument_days(tape_root, None, item.venue, item.symbol):
            d = date.fromisoformat(day)
            if (d.year, d.month) != (first.year, first.month):
                continue
            cpath = certificate_path(cert_root, venue, symbol, day)
            if not cpath.exists():
                certify_tree(settings, tape_root, cert_root, d, venue, symbol)
            certs.append(read_certificate(cpath))
            members.append((cpath, f"certificates/{cpath.relative_to(cert_root)}"))
            for channel in settings.archive.channels:
                for part in part_files(tape_root, PartitionKey(venue, channel, symbol, day)):
                    members.append((part, f"tape/{part.relative_to(tape_root)}"))
    out_dir = settings.data_path(settings.paths.archives) / month
    report = quality_report(month, certs)
    report_path = out_dir / settings.archive.report_name
    atomic_write_bytes(report_path, report.encode())
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "month": month,
        "instrument_days": len(certs),
        "files": [
            {"path": arc, "bytes": src.stat().st_size, "sha256": _sha256(src)}
            for src, arc in members
        ],
    }
    manifest_bytes = orjson.dumps(manifest, option=orjson.OPT_INDENT_2)
    manifest_path = out_dir / settings.archive.manifest_name
    atomic_write_bytes(manifest_path, manifest_bytes)

    tar_path = out_dir / settings.archive.name_template.format(month=month)
    tmp = tar_path.with_name(f".{tar_path.name}{IN_PROGRESS_SUFFIX}")
    with tarfile.open(tmp, "w") as tar:
        for src, arc in members:
            tar.add(src, arcname=arc)
        for name, data in (
            (settings.archive.report_name, report.encode()),
            (settings.archive.manifest_name, manifest_bytes),
        ):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    tmp.replace(tar_path)
    fsync_dir(out_dir)
    return ArchiveResult(tar_path, report_path, manifest_path, len(certs), len(members))
