"""``tape`` command line: record | backfill | certify | merge-regions | archive | replay."""

from __future__ import annotations

import asyncio
import signal
from datetime import date
from pathlib import Path
from typing import Annotated

import httpx
import typer

from tape_recorder.archive import build_archive
from tape_recorder.history import backfill as backfill_async
from tape_recorder.logging_setup import configure_logging
from tape_recorder.merge import merge_regions
from tape_recorder.quality.certificate import Certificate
from tape_recorder.quality.certify import certify_tree
from tape_recorder.recorder import record as record_async
from tape_recorder.replay import replay_files
from tape_recorder.settings import Settings, load_settings
from tape_recorder.timeutil import wall_clock_ns
from tape_recorder.transport import HttpxFetcher

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)

ConfigOpt = Annotated[Path | None, typer.Option("--config", help="Settings YAML file.")]


def _settings(config: Path | None) -> Settings:
    settings = load_settings(config)
    configure_logging(settings.logging.level, settings.logging.json_output)
    return settings


def _root(settings: Settings, value: str | None, default: Path) -> Path:
    """Absolute paths are used as given; relative ones are inside the project data subtree."""
    if value is None:
        return default
    path = Path(value)
    return path if path.is_absolute() else settings.project_root / path


def certificate_line(cert: Certificate) -> str:
    failed = [c.name for c in cert.checks if not c.passed]
    return (
        f"{cert.venue:<13} {cert.symbol:<15} {cert.date}  {cert.status:<9} "
        f"coverage={cert.coverage_ratio:.4f} gaps={len(cert.gaps):<3} "
        f"failed={','.join(failed) or '-'}"
    )


@app.command()
def record(
    venue: Annotated[list[str] | None, typer.Option(help="Venue(s) to record.")] = None,
    out: Annotated[str | None, typer.Option(help="Output tape root.")] = None,
    duration_s: Annotated[float | None, typer.Option(help="Stop after this many seconds.")] = None,
    raw_log: Annotated[bool, typer.Option(help="Also log raw frames for replay.")] = False,
    config: ConfigOpt = None,
) -> None:
    """Record live market data from the configured venues."""
    settings = _settings(config)
    out_root = _root(settings, out, settings.tape_root)

    async def main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        if duration_s is not None:
            loop.call_later(duration_s, stop.set)
        await record_async(settings, out_root, venue, stop, raw_log)

    asyncio.run(main())


@app.command()
def backfill(
    venue: Annotated[str, typer.Option(help="Venue to backfill (moex-tbank, moex-algopack).")],
    start: Annotated[str, typer.Option("--from", help="First trade date, YYYY-MM-DD.")],
    end: Annotated[str, typer.Option("--to", help="Last trade date, YYYY-MM-DD.")],
    out: Annotated[str | None, typer.Option(help="Output tape root.")] = None,
    config: ConfigOpt = None,
) -> None:
    """Fetch historical candles (and Super Candles) into the tape."""
    settings = _settings(config)
    if settings.venues[venue].token_required and settings.venues[venue].token() is None:
        raise typer.BadParameter(f"{venue} needs its token in ${settings.venues[venue].token_env}")

    async def main() -> None:
        async with httpx.AsyncClient(
            timeout=settings.http.timeout_s, headers={"User-Agent": settings.http.user_agent}
        ) as client:
            stats = await backfill_async(
                settings,
                venue,
                date.fromisoformat(start),
                date.fromisoformat(end),
                _root(settings, out, settings.tape_root),
                HttpxFetcher(client),
                wall_clock_ns,
            )
        typer.echo(f"requests={stats.requests} rows={stats.rows} failures={stats.failures}")

    asyncio.run(main())


@app.command()
def certify(
    day: Annotated[str | None, typer.Option("--date", help="YYYY-MM-DD (default: all).")] = None,
    venue: Annotated[str | None, typer.Option()] = None,
    symbol: Annotated[str | None, typer.Option()] = None,
    tape: Annotated[str | None, typer.Option(help="Tape root to certify.")] = None,
    certs: Annotated[str | None, typer.Option(help="Certificate output root.")] = None,
    config: ConfigOpt = None,
) -> None:
    """Write a quality certificate per instrument-day."""
    settings = _settings(config)
    results = certify_tree(
        settings,
        _root(settings, tape, settings.tape_root),
        _root(settings, certs, settings.certificates_root),
        date.fromisoformat(day) if day else None,
        venue,
        symbol,
    )
    for cert, _ in results:
        typer.echo(certificate_line(cert))
    if not results:
        typer.echo("no instrument-days found")


@app.command("merge-regions")
def merge_regions_cmd(
    region_a: Annotated[str, typer.Argument(help="Region name or capture root path.")],
    region_b: Annotated[str, typer.Argument(help="Region name or capture root path.")],
    out: Annotated[str | None, typer.Option(help="Merged tape root.")] = None,
    day: Annotated[str | None, typer.Option("--date")] = None,
    config: ConfigOpt = None,
) -> None:
    """Merge two regional captures, keeping the earliest arrival of every message."""
    settings = _settings(config)

    def capture(arg: str) -> Path:
        path = Path(arg)
        return path if path.is_absolute() else settings.capture_root(arg)

    result = merge_regions(
        settings,
        capture(region_a),
        capture(region_b),
        _root(settings, out, settings.tape_root),
        day,
    )
    typer.echo(
        f"partitions={result.partitions} rows_in={result.rows_in} rows_out={result.rows_out} "
        f"latency_reports={len(result.latency_reports)}"
    )


@app.command()
def archive(
    month: Annotated[str, typer.Option(help="YYYY-MM")],
    tape: Annotated[str | None, typer.Option()] = None,
    certs: Annotated[str | None, typer.Option()] = None,
    config: ConfigOpt = None,
) -> None:
    """Pack the configured universe for one month with certificates and a quality report."""
    settings = _settings(config)
    result = build_archive(
        settings,
        month,
        _root(settings, tape, settings.tape_root),
        _root(settings, certs, settings.certificates_root),
    )
    typer.echo(f"{result.tar_path} instrument_days={result.instrument_days} files={result.files}")


@app.command()
def replay(
    paths: Annotated[list[Path], typer.Argument(help="Fixture/raw-log files or directories.")],
    region: Annotated[str | None, typer.Option(help="capture_region label.")] = None,
    out: Annotated[str | None, typer.Option(help="Output tape root.")] = None,
    config: ConfigOpt = None,
) -> None:
    """Replay recorded frames through the adapters into a tape tree (offline)."""
    settings = _settings(config)
    result = replay_files(
        settings, paths, region or settings.region, _root(settings, out, settings.tape_root)
    )
    s = result.stats
    typer.echo(
        f"frames={s.frames} events={s.events} rows={result.rows} duplicates={s.duplicates} "
        f"parse_errors={s.parse_errors} parts={len(result.files)}"
    )
