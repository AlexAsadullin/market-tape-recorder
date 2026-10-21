"""Typed settings: defaults from ``config/settings.yaml``, overridden by ``TAPE_*`` env variables.

Precedence (highest first): explicit keyword overrides, environment, ``.env``, YAML file.
Nested fields use ``__`` as delimiter, e.g. ``TAPE_SESSION__REDUNDANT_CONNECTIONS=1``.
The platform data root comes from ``TRADEQ_DATA_DIR``.
"""

from __future__ import annotations

import os
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, SecretStr
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from tape_recorder.schema import Channel

CONFIG_FILE_ENV = "TAPE_CONFIG_FILE"
DEFAULT_CONFIG_FILE = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"

_yaml_file: ContextVar[Path] = ContextVar("tape_yaml_file", default=DEFAULT_CONFIG_FILE)


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PathsSettings(_Model):
    """Locations inside ``$TRADEQ_DATA_DIR/<project_subdir>/``; ``{region}`` is substituted."""

    project_subdir: str
    tape: str
    captures: str
    staging: str
    certificates: str
    latency: str
    archives: str
    raw: str


class WriterSettings(_Model):
    flush_rows: int = Field(gt=0)
    flush_interval_s: float = Field(gt=0)
    rotate_rows: int = Field(gt=0)
    rotate_interval_s: float = Field(gt=0)
    compression: str


class BackoffSettings(_Model):
    initial_s: float = Field(gt=0)
    max_s: float = Field(gt=0)
    multiplier: float = Field(ge=1)
    jitter: float = Field(ge=0, le=1)


class SessionSettings(_Model):
    backoff: BackoffSettings
    redundant_connections: int = Field(ge=1)
    rotate_after_s: float = Field(gt=0)
    daily_rotation_utc: str | None
    overlap_timeout_s: float = Field(gt=0)
    connect_timeout_s: float = Field(gt=0)
    resnapshot_interval_s: float = Field(gt=0)
    max_message_bytes: int = Field(gt=0)
    seed: int


class DedupSettings(_Model):
    enabled: bool
    window_s: float = Field(gt=0)
    max_keys: int = Field(gt=0)


class HttpSettings(_Model):
    timeout_s: float = Field(gt=0)
    user_agent: str


class SymbolSettings(_Model):
    """One instrument; ``contract_size`` converts venue units (contracts, lots) to base units."""

    symbol: str
    contract_size: float = Field(default=1.0, gt=0)
    engine: str | None = None
    market: str | None = None
    board: str | None = None


class SessionWindow(_Model):
    """Expected trading window for coverage, in the venue's local time zone."""

    start: str
    end: str
    timezone: str


class VenueSettings(_Model):
    adapter: Literal["binance_spot", "binance_usdm", "bybit", "okx", "hyperliquid", "moex_iss"]
    enabled: bool = True
    ws_url: str | None = None
    rest_url: str | None = None
    algopack_url: str | None = None
    api_key: SecretStr | None = None
    channels: list[Channel]
    symbols: dict[str, SymbolSettings]
    book_depth: int | None = None
    depth_interval: str | None = None
    snapshot_path: str | None = None
    snapshot_limit: int | None = None
    mark_price_interval: str | None = None
    heartbeat_interval_s: float | None = None
    heartbeat_payload: str | None = None
    book_sequenced: bool = False
    trade_ids_consecutive: bool = False
    poll_interval_s: float | None = None
    rest_paths: dict[str, str] = Field(default_factory=dict)
    rest_params: dict[str, dict[str, str]] = Field(default_factory=dict)
    timezone: str | None = None
    session_window: SessionWindow | None = None


class CheckSpec(_Model):
    """A check limit: ``max`` passes if value <= threshold, ``min`` if value >= threshold.

    A failed ``hard`` check makes the certificate ``failed``; a failed ``soft`` one ``degraded``.
    """

    threshold: float
    direction: Literal["max", "min"]
    severity: Literal["soft", "hard"]


class QualitySettings(_Model):
    coverage_window: Literal["day", "observed"]
    alive_gap_s: float = Field(gt=0)
    book_compare_depth: int = Field(gt=0)
    price_tolerance: float = Field(ge=0)
    size_rel_tolerance: float = Field(ge=0)
    silence_s: dict[Channel, float]
    checks: dict[str, CheckSpec]
    venue_overrides: dict[str, dict[str, CheckSpec]] = Field(default_factory=dict)

    def checks_for(self, venue: str) -> dict[str, CheckSpec]:
        merged = dict(self.checks)
        merged.update(self.venue_overrides.get(venue, {}))
        return merged


class MergeSettings(_Model):
    primary_region: str | None
    latency_quantiles: list[float]


class UniverseItem(_Model):
    venue: str
    symbol: str


class ArchiveSettings(_Model):
    universe: list[UniverseItem]
    channels: list[Channel]
    name_template: str
    report_name: str
    manifest_name: str


class ServiceSettings(_Model):
    enabled: bool
    host: str
    port: int = Field(gt=0, lt=65536)
    stale_after_s: float = Field(gt=0)


class LoggingSettings(_Model):
    level: str
    json_output: bool


class FaultSpec(_Model):
    """A fault injected into a replayed frame stream (see :mod:`tape_recorder.faults`)."""

    kind: Literal["drop", "duplicate", "reorder", "cross", "resubscribe"]
    venue: str
    index: int = Field(ge=0)
    gap_ms: float = 0.0
    delay_ms: float = 0.0


class DemoRegion(_Model):
    latency_ms: float = Field(ge=0)
    jitter_ms: float = Field(ge=0)
    faults: list[FaultSpec] = Field(default_factory=list)


class DemoSettings(_Model):
    fixtures_dir: Path
    seed: int
    regions: dict[str, DemoRegion]


class Settings(BaseSettings):
    """Root settings object; build it with :func:`load_settings`."""

    model_config = SettingsConfigDict(
        env_prefix="TAPE_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
        frozen=True,
    )

    data_dir: Path = Field(validation_alias=AliasChoices("TRADEQ_DATA_DIR", "TAPE_DATA_DIR"))
    region: str
    paths: PathsSettings
    writer: WriterSettings
    session: SessionSettings
    dedup: DedupSettings
    http: HttpSettings
    venues: dict[str, VenueSettings]
    quality: QualitySettings
    merge: MergeSettings
    archive: ArchiveSettings
    service: ServiceSettings
    logging: LoggingSettings
    demo: DemoSettings

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        yaml_source = YamlConfigSettingsSource(settings_cls, yaml_file=_yaml_file.get())
        return init_settings, env_settings, dotenv_settings, yaml_source

    @property
    def project_root(self) -> Path:
        """``$TRADEQ_DATA_DIR/market-tape-recorder``: the only subtree this project writes."""
        return self.data_dir / self.paths.project_subdir

    def data_path(self, relative: str, region: str | None = None) -> Path:
        return self.project_root / relative.format(region=region or self.region)

    @property
    def tape_root(self) -> Path:
        return self.data_path(self.paths.tape)

    @property
    def certificates_root(self) -> Path:
        return self.data_path(self.paths.certificates)

    def capture_root(self, region: str | None = None) -> Path:
        return self.data_path(self.paths.captures, region)

    def enabled_venues(self) -> dict[str, VenueSettings]:
        return {name: v for name, v in self.venues.items() if v.enabled}


def load_settings(config_file: Path | None = None, **overrides: Any) -> Settings:
    """Load settings from YAML (``TAPE_CONFIG_FILE`` or the repository default) plus env."""
    path = config_file or Path(os.environ.get(CONFIG_FILE_ENV, str(DEFAULT_CONFIG_FILE)))
    token = _yaml_file.set(path)
    try:
        return Settings(**overrides)
    finally:
        _yaml_file.reset(token)
