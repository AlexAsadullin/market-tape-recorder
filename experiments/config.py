"""Typed parameters of the report experiments, read from ``config/report.yaml``."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

from experiments.synthetic import StreamSpec
from tape_recorder.settings import DEFAULT_CONFIG_FILE

REPORT_CONFIG_ENV = "TAPE_REPORT_CONFIG_FILE"
DEFAULT_REPORT_CONFIG = DEFAULT_CONFIG_FILE.with_name("report.yaml")

FaultName = Literal["clean", "drop", "reorder", "cross", "resubscribe", "duplicate"]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DetectionConfig(_Model):
    runs: int = Field(gt=0)
    venues: list[str]
    faults: list[FaultName]
    fault_window: tuple[float, float]
    resubscribe_gap_ms: tuple[float, float]
    duplicate_delay_ms: tuple[float, float]
    gap_monitor_ratio: float = Field(gt=1)


class RotationMode(_Model):
    redundant_connections: int = Field(ge=1)
    overlap_rotation: bool


class RotationConfig(_Model):
    runs: int = Field(gt=0)
    duration_s: float = Field(gt=0)
    message_interval_ms: float = Field(gt=0)
    connect_ms: tuple[float, float]
    rotate_after_s: float = Field(gt=0)
    modes: dict[str, RotationMode]


class RegionLatency(_Model):
    latency_ms: float = Field(ge=0)
    jitter_ms: float = Field(ge=0)


class RegionsConfig(_Model):
    runs: int = Field(gt=0)
    venue: str
    stream: StreamSpec
    drop_probability: float = Field(ge=0, lt=1)
    outage_gap_ms: tuple[float, float]
    outage_window: tuple[float, float]
    regions: dict[str, RegionLatency]


class ReportConfig(_Model):
    seed: int
    bootstrap_resamples: int = Field(gt=0)
    reports_subdir: str
    streams: dict[str, StreamSpec]
    detection: DetectionConfig
    rotation: RotationConfig
    regions: RegionsConfig


def load_report_config(path: Path | None = None) -> ReportConfig:
    p = path or Path(os.environ.get(REPORT_CONFIG_ENV, str(DEFAULT_REPORT_CONFIG)))
    return ReportConfig.model_validate(yaml.safe_load(p.read_text()))
