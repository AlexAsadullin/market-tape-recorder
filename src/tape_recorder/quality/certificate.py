"""Contract 6.2 quality certificate (wire model) and its atomic JSON file."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from tape_recorder.schema import SCHEMA_VERSION
from tape_recorder.writer import atomic_write_bytes

Status = Literal["certified", "degraded", "failed"]


class Check(BaseModel):
    name: str
    passed: bool
    value: float
    threshold: float
    direction: Literal["max", "min"]
    severity: Literal["soft", "hard"]


class Gap(BaseModel):
    start_ns: int
    end_ns: int
    kind: str
    channel: str | None = None
    detail: str | None = None


class SkewStats(BaseModel):
    """Statistics of ``ts_local - ts_exchange`` in milliseconds."""

    n: int
    mean_ms: float
    std_ms: float
    min_ms: float
    p50_ms: float
    p90_ms: float
    p99_ms: float
    max_ms: float
    negative_ratio: float


class ConnectionSpan(BaseModel):
    connection_id: str
    capture_region: str
    first_ns: int
    last_ns: int
    rows: int


class Resubscription(BaseModel):
    connections: int
    handovers: int
    gap_ms_max: float
    gap_ms_total: float
    overlap_ms_min: float | None


class BookStats(BaseModel):
    delta_messages: int = 0
    snapshots: int = 0
    snapshots_compared: int = 0
    snapshot_mismatches: int = 0
    snapshots_uncomparable: int = 0
    crossed_events: int = 0
    locked_events: int = 0
    stale_messages: int = 0
    unsynced_ms: float = 0.0


class Certificate(BaseModel):
    schema_version: str = SCHEMA_VERSION
    venue: str
    symbol: str
    date: str
    status: Status
    checks: list[Check]
    gaps: list[Gap]
    coverage_ratio: float
    # Optional extensions of contract 6.2 (documented in the README):
    generated_at: str
    window_start_ns: int
    window_end_ns: int
    capture_regions: list[str]
    rows: dict[str, int]
    connections: list[ConnectionSpan]
    resubscription: Resubscription
    book: BookStats
    clock_skew: SkewStats | None
    clock_skew_by_region: dict[str, SkewStats] = Field(default_factory=dict)

    def check(self, name: str) -> Check:
        return next(c for c in self.checks if c.name == name)


def certificate_path(cert_root: Path, venue: str, symbol: str, day: str) -> Path:
    return cert_root / f"venue={venue}" / f"symbol={symbol}" / f"date={day}.json"


def write_certificate(cert_root: Path, cert: Certificate) -> Path:
    path = certificate_path(cert_root, cert.venue, cert.symbol, cert.date)
    atomic_write_bytes(path, cert.model_dump_json(indent=2).encode())
    return path


def read_certificate(path: Path) -> Certificate:
    return Certificate.model_validate_json(path.read_bytes())
