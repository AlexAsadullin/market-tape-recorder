"""``tape`` command line: record | backfill | certify | merge-regions | archive | replay |
instruments-ref | universe | download | pin-show."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import orjson
import typer

from tape_recorder.adapters.tbank import TBankAdapter
from tape_recorder.archive import build_archive
from tape_recorder.download import (
    ISS,
    TBANK,
    Downloader,
    DownloadSummary,
    LimitedFetcher,
    PressureGate,
    RateLimiter,
    http_load_probe,
    httpx_send,
    quality_report,
    requested_window,
)
from tape_recorder.history import backfill as backfill_async
from tape_recorder.logging_setup import configure_logging
from tape_recorder.merge import merge_regions
from tape_recorder.pinning import describe, fetch_leaf
from tape_recorder.quality.certificate import Certificate
from tape_recorder.quality.certify import certify_tree
from tape_recorder.recorder import record as record_async
from tape_recorder.reference import (
    ZERO_FEE_FILE,
    IncompleteReferenceError,
    build_zero_fee_reference,
    write_reference,
)
from tape_recorder.replay import replay_files
from tape_recorder.settings import Settings, load_settings
from tape_recorder.timeutil import wall_clock_ns
from tape_recorder.transport import HttpxFetcher, http_client
from tape_recorder.universe import build_universe, i2_rows, read_i2_rows, write_jsonl

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
        async with http_client(settings.http) as client:
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


@app.command("instruments-ref")
def instruments_ref(
    venue: Annotated[str, typer.Option(help="T-Bank venue whose token is used.")] = "moex-tbank",
    out: Annotated[str | None, typer.Option(help="Output file.")] = None,
    config: ConfigOpt = None,
) -> None:
    """Regenerate the zero-fee fund list (T-Bank's own funds) from the instruments service."""
    settings = _settings(config)
    cfg = settings.venues[venue]
    if cfg.adapter != "tbank":
        raise typer.BadParameter(f"{venue} is not a T-Bank venue")
    if cfg.token() is None:
        raise typer.BadParameter(f"{venue} needs its token in ${cfg.token_env}")
    path = _root(settings, out, settings.project_root / settings.reference.dir / ZERO_FEE_FILE)

    async def main() -> None:
        async with http_client(settings.http) as client:
            doc, selection = await build_zero_fee_reference(
                TBankAdapter(venue, cfg), HttpxFetcher(client), settings.reference, wall_clock_ns
            )
        write_reference(path, doc)
        top = ", ".join(f"{name} ({n})" for name, n in selection.brands.most_common(8))
        typer.echo(f"etfs={selection.etfs} assets={selection.assets} brands: {top}")
        typer.echo(f"{path} funds={doc['count']}")

    try:
        asyncio.run(main())
    except IncompleteReferenceError as exc:
        typer.echo(f"not written: {exc}", err=True)
        raise typer.Exit(1) from exc


@app.command()
def calendar(
    venue: Annotated[str, typer.Option(help="T-Bank venue whose token is used.")] = "moex-tbank",
    since: Annotated[
        str, typer.Option(help="First date of the calendar, YYYY-MM-DD.")
    ] = "2024-01-01",
    horizon_days: Annotated[int, typer.Option(help="Days ahead for scheduled events.")] = 365,
    config: ConfigOpt = None,
) -> None:
    """Dividends and report dates of funds and IMOEX constituents -> reference/calendar_i2.jsonl."""
    from datetime import date as _date

    from tape_recorder.calendar_ref import build_calendar, imoex_constituents

    settings = _settings(config)
    cfg = settings.venues[venue]
    if cfg.token() is None:
        raise typer.BadParameter(f"{venue} needs its token in ${cfg.token_env}")
    uni_path, _ = _universe_paths(settings)
    entries: list[dict[str, Any]] = orjson.loads(uni_path.read_bytes())["instruments"]
    start = _date.fromisoformat(since)
    out_path = settings.project_root / settings.reference.dir / "calendar_i2.jsonl"

    async def main() -> None:
        async with http_client(settings.http) as client:
            iss = str(settings.venues["moex-iss"].rest_url or "https://iss.moex.com")
            held = await imoex_constituents(client, iss, start, _date.today())
            wanted = [
                e
                for e in entries
                if (e["kind"] == "fund" and e["class_code"] == "TQBR")
                or (e["kind"] == "share" and e["ticker"] in held)
            ]
            rows, stats = await build_calendar(
                TBankAdapter(venue, cfg),
                HttpxFetcher(client),
                wanted,
                start,
                horizon_days,
                settings.reference.min_request_interval_s,
            )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = out_path.with_suffix(".jsonl.tmp")
        tmp.write_bytes(b"".join(orjson.dumps(r) + b"\n" for r in rows))
        tmp.replace(out_path)
        typer.echo(f"{out_path} rows={len(rows)} imoex_constituents={len(held)} {stats}")

    asyncio.run(main())


def _universe_paths(settings: Settings) -> tuple[Path, Path]:
    ref = settings.project_root / settings.reference.dir
    return ref / settings.download.universe_file, ref / settings.download.i2_instruments_file


@app.command()
def universe(
    venue: Annotated[str, typer.Option(help="T-Bank venue whose token is used.")] = "moex-tbank",
    config: ConfigOpt = None,
) -> None:
    """Write the download universe (TQBR shares + MOEX-listed T-Capital funds) and I2's file."""
    settings = _settings(config)
    cfg = settings.venues[venue]
    if cfg.adapter != "tbank" or cfg.token() is None:
        raise typer.BadParameter(f"{venue} must be a T-Bank venue with its token set")
    ref_dir = settings.project_root / settings.reference.dir
    zero_fee_path = ref_dir / ZERO_FEE_FILE
    if not zero_fee_path.exists():
        raise typer.BadParameter(f"{zero_fee_path} is missing: run `tape instruments-ref` first")
    zero_fee = orjson.loads(zero_fee_path.read_bytes())
    uni_path, i2_path = _universe_paths(settings)

    async def main() -> dict[str, Any]:
        async with http_client(settings.http) as client:
            # Listings are 3 MB uncompressed and T-Bank cuts slow transfers off after ~30 s,
            # so they go through the retrying fetcher (transport errors back off and retry).
            limiter = RateLimiter(1.0, 15, asyncio.sleep)  # listing methods: 15 calls/min
            fetcher = LimitedFetcher(httpx_send(client), limiter, settings.download, asyncio.sleep)
            return await build_universe(
                TBankAdapter(venue, cfg),
                fetcher,
                zero_fee,
                settings.download,
                wall_clock_ns,
            )

    doc = asyncio.run(main())
    uni_path.parent.mkdir(parents=True, exist_ok=True)
    uni_path.write_bytes(orjson.dumps(doc, option=orjson.OPT_INDENT_2))
    write_jsonl(i2_path, i2_rows(doc["instruments"], wall_clock_ns(), read_i2_rows(i2_path)))
    typer.echo(f"{uni_path} instruments={doc['count']} by_kind={doc['count_by_kind']}")
    typer.echo(f"{i2_path} (I2 moex.instruments)")


IntervalOpt = Annotated[str, typer.Option(help="day or minute.")]


@app.command()
def download(
    interval: IntervalOpt = "day",
    since: Annotated[str | None, typer.Option(help="First date, YYYY-MM-DD.")] = None,
    until: Annotated[str | None, typer.Option(help="Last date, YYYY-MM-DD.")] = None,
    symbols: Annotated[
        str | None, typer.Option(help="Comma-separated tickers (default: whole universe).")
    ] = None,
    iss: Annotated[bool, typer.Option(help="Public ISS fallback for what T-Bank lacks.")] = True,
    out: Annotated[str | None, typer.Option(help="Output tape root.")] = None,
    config: ConfigOpt = None,
) -> None:
    """Bulk candle history for the universe into the tape (resumable; Ctrl-C stops cleanly)."""
    settings = _settings(config)
    label = {"day": "1d", "minute": "1m", "1d": "1d", "1m": "1m"}.get(interval)
    if label is None:
        raise typer.BadParameter("--interval must be day or minute")
    if settings.venues[TBANK].token() is None:
        raise typer.BadParameter(f"{TBANK} needs its token in ${settings.venues[TBANK].token_env}")
    uni_path, _ = _universe_paths(settings)
    if not uni_path.exists():
        raise typer.BadParameter(f"{uni_path} is missing: run `tape universe` first")
    entries: list[dict[str, Any]] = orjson.loads(uni_path.read_bytes())["instruments"]
    if symbols:
        wanted = {s.strip().upper() for s in symbols.split(",") if s.strip()}
        # A ticker selects every listing of it; a full symbol (TMOS-RUB-TQBR) selects one board.
        entries = [
            e
            for e in entries
            if str(e["ticker"]).upper() in wanted or str(e["symbol"]).upper() in wanted
        ]
        unknown = (
            wanted
            - {str(e["ticker"]).upper() for e in entries}
            - {str(e["symbol"]).upper() for e in entries}
        )
        if unknown:
            raise typer.BadParameter(f"not in the universe: {sorted(unknown)}")
    cfg = settings.download
    with contextlib.suppress(OSError):
        os.nice(cfg.nice)
    tape_root = _root(settings, out, settings.tape_root)
    since_d = date.fromisoformat(since) if since else None
    until_d = date.fromisoformat(until) if until else None

    async def main() -> list[DownloadSummary]:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        async with http_client(settings.http) as client:
            gate = PressureGate(
                http_load_probe(client, cfg.recorder_load_url) if cfg.recorder_load_url else None,
                cfg.probe_interval_s,
                cfg.yield_sleep_s,
                asyncio.sleep,
            )
            send = httpx_send(client)
            tb_limiter = RateLimiter(cfg.rate_fraction, cfg.default_limit_per_min, asyncio.sleep)
            iss_per_min = max(1, round(60.0 / max(cfg.iss_min_request_interval_s, 0.01)))
            iss_limiter = RateLimiter(1.0, iss_per_min, asyncio.sleep)
            fetchers = {
                TBANK: LimitedFetcher(send, tb_limiter, cfg, asyncio.sleep, gate, stop.is_set),
                ISS: LimitedFetcher(send, iss_limiter, cfg, asyncio.sleep, gate, stop.is_set),
            }
            dl = Downloader(settings, entries, tape_root, fetchers, wall_clock_ns, stop)
            summaries = [await dl.run(dl.plan(label, since_d, until_d, TBANK), label)]
            if iss and cfg.iss_fallback and not stop.is_set():
                summaries.append(await dl.run(dl.plan(label, since_d, until_d, ISS), label))
        return summaries

    summaries = asyncio.run(main())
    now = wall_clock_ns()
    window = requested_window(label, cfg, now, since_d, until_d)
    report = quality_report(tape_root, entries, label, window, cfg.calendar_threshold, now)
    report_dir = settings.project_root / cfg.dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"download-{label}-{date.today().isoformat()}.json"
    report_path.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    for s in summaries:
        typer.echo(
            f"{label} tasks={s.tasks} chunks={s.chunks_done}/{s.chunks_total} rows={s.rows} "
            f"failed_chunks={s.failed_chunks} stopped={s.stopped} "
            f"by_venue={dict(s.rows_by_venue)}"
        )
    typer.echo(
        f"report {report_path}: instruments={report['instruments']} rows={report['rows']} "
        f"with_gaps={report['instruments_with_gaps']}"
    )
    if any(s.stopped for s in summaries):
        raise typer.Exit(130)
    if any(s.failed_chunks for s in summaries):
        raise typer.Exit(2)


@app.command("pin-show")
def pin_show(
    host: Annotated[str, typer.Argument(help="Host name, e.g. invest-public-api.tbank.ru.")],
    port: Annotated[int, typer.Option(help="TLS port.")] = 443,
    connect: Annotated[str | None, typer.Option(help="Address to dial instead of host.")] = None,
    config: ConfigOpt = None,
) -> None:
    """Show a host's leaf certificate and SPKI pin (TLS handshake only; no request, no token).

    The chain is not trusted for this, so compare the fingerprint with the browser's view
    (padlock, certificate, SHA-256 fingerprint) before putting the pin into http.pins.
    """
    settings = _settings(config)
    info = describe(fetch_leaf(host, port, settings.http.timeout_s, connect), host, port)
    configured = settings.http.pins.get(host.lower(), [])
    typer.echo(f"host:        {info.host}:{info.port}")
    typer.echo(f"subject:     {info.subject}")
    typer.echo(f"issuer:      {info.issuer}")
    typer.echo(f"valid:       {info.not_before.isoformat()} .. {info.not_after.isoformat()}")
    typer.echo(
        f"dns names:   {', '.join(info.dns_names) or '-'} (matches host: {info.hostname_ok})"
    )
    typer.echo(f"pin:         {info.pin}")
    typer.echo(f"sha256 cert: {info.fingerprint}")
    if configured:
        state = "matches" if info.pin in configured else "DOES NOT MATCH"
        typer.echo(f"config:      {state} http.pins[{host}]")
    else:
        typer.echo("config:      host not pinned")
