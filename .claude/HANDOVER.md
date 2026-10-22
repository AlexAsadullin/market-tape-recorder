# HANDOVER: market-tape-recorder (Tradeq I1)

Last updated 2026-10-22 by the orchestrating session, from the transcript of the agent that stopped at the
session rate limit.

## Goal

The project is a self-hosted recorder for market data from crypto exchanges (Binance, Bybit, OKX), the Hyperliquid
on-chain perp DEX and MOEX. It writes everything in one normalised Parquet tape schema (contract 6.2) and issues a
quality certificate for each instrument-day. The spec is in `../prompts/02-market-tape-recorder.md`. Platform rules
are in `../prompts/00-platform-conventions.md`, and the README and report rules are in
`../prompts/11-readme-and-research-report-standards.md`.
Other projects use the tape: I2, S3, I6, I5 and S1. The default port is 8602.

Still to do: write `docs/report.md`, which needs a working `make report`, and a lean README. The README is currently 1 line.

## Stack

Python 3.13, a uv-managed `.venv`, and pins from `../constraints.txt`. The project uses websockets 17 (direct venue
clients), httpx (MOEX ISS REST), pyarrow, pydantic and pydantic-settings, typer (the `tape` CLI), FastAPI and uvicorn
(the status service), structlog, orjson and sortedcontainers. The `bench` dependency group adds cryptofeed 3.0.1,
which is used only as an analog in the report experiments. Settings come from `config/settings.yaml` and can be
overridden with `TAPE_*` env variables. The data root is `$TRADEQ_DATA_DIR/market-tape-recorder/`.

## Architecture and data flow

Raw frames arrive from the transport (`transport.py`) inside a resilient session (`session.py`: reconnect with backoff,
overlapping rotation, resync). From there they go through the venue adapter (`adapters/*.py`, which parse frames into
records and apply sequencing rules), dedup (`dedup.py`, earliest arrival across redundant connections) and the writer
(`writer.py`, partitioned Parquet with atomic finalisation). `pipeline.py` holds that shared path, which is used by
both live recording and `replay.py`. `quality/certify.py` reads the tape back and issues a certificate per
instrument-day (gaps, sequencing, coverage and clock skew). `merge.py` merges two regional captures, and `archive.py`
builds monthly archives.

## Files

| Path | Purpose |
|---|---|
| src/tape_recorder/adapters/{binance,bybit,okx,hyperliquid,moex}.py | Venue wire-format parsers behind the `VenueAdapter` protocol (base.py) |
| src/tape_recorder/session.py | Live sessions: backoff, overlapping or naive rotation, resubscription |
| src/tape_recorder/sequencing.py | Per-venue order-book delta continuity rules (pure functions) |
| src/tape_recorder/book.py | L2 book reconstructed from snapshots and deltas |
| src/tape_recorder/schema.py | Contract 6.2 Arrow schema |
| src/tape_recorder/writer.py, reader in quality/reader.py | Parquet tape write and read |
| src/tape_recorder/quality/certify.py, certificate.py, intervals.py | Quality certificate (contract 6.2) |
| src/tape_recorder/faults.py | Deterministic fault injection (gaps, duplicates, latency) for tests and experiments |
| src/tape_recorder/merge.py, archive.py, replay.py, rawlog.py | Region merge, monthly archive, offline replay, raw frame log |
| src/tape_recorder/recorder.py, service.py, monitor.py | `tape record`, status endpoint, per-stream health |
| src/tape_recorder/demo.py | `make reproduce`: offline demo with two simulated regions |
| experiments/synthetic.py | UNTRACKED. Synthetic L2 streams in real venue wire formats |
| experiments/stats.py | UNTRACKED. Interval estimates and two-sample tests for the report tables |
| experiments/config.py + config/report.yaml | UNTRACKED. Typed experiment parameters (seed 20261022, 10000 bootstrap resamples) |
| experiments/detection.py | UNTRACKED. Experiment 1: does each method (certifier, gap monitor, cryptofeed) detect one injected fault per stream |
| experiments/cryptofeed_probe.py | UNTRACKED. Runs cryptofeed's own Binance book handler offline on the same frames |
| experiments/rotation.py | UNTRACKED. Experiment 2: the blind interval of a planned reconnect, overlapping vs naive rotation, on a live session against a fake server |
| tests/unit, tests/integration, tests/live | 79 offline tests; 6 live tests deselected by default (`-m live`) |

## State at handover

Updated 2026-10-23 by the subagent that finished the repo.

1. Green: 83 tests passed and 6 live tests deselected, coverage is 94%, and ruff and mypy are clean on src, tests and experiments.
2. `make report` works. It runs `python -m experiments` (experiments/__main__.py) with the bench group and executes detection (3600 runs), rotation (60 live-session runs) and regions (100 two-region merges). It takes about 7 minutes and writes tables.md, results.json, detection.csv, rotation.csv and regions.csv to `$TRADEQ_DATA_DIR/market-tape-recorder/reports/`. `make report-smoke` does a 20-second check.
3. New files: experiments/regions.py (experiment 3) and tests/unit/test_report_stats.py. The README is written, with a key-results table.
4. The report text is drafted. The orchestrator writes it to docs/report.md from the subagent's final message (a scratchpad copy is mtr_report.md). If docs/report.md is missing, that step is still open.
5. Headline numbers: the certificate caught 600 of 600 faults per fault type and raised 0 false alarms on 600 clean runs. cryptofeed missed 200 of 200 duplicates. Overlapping rotation lost 0.0 trades per reconnect, against 75.0 for naive rotation. Merging two regions raised the certified days from 0% to 70%.

## Mistakes and lessons

1. The Makefile `report` target called a module `tape_recorder.experiments` that never existed. The fix keeps `experiments/` at the top level and runs `python -m experiments` from the repo root, where make -C sets the cwd. The same command fails when it runs from another directory ("No module named experiments").
2. cryptofeed needs network access to load its symbol table. The workaround is to pre-seed it with `Symbols.set(BINANCE, {'BTC-USDT': 'BTCUSDT'}, {})` and stub the snapshot fetch, which lets it run offline. Its `cross_check` flag is its strictest mode.
3. Use `git -C`, `make -C` and single commands, because compound shell commands are denied.
4. Commit early. The first session lost its uncommitted experiments to a rate limit, and this session committed them first, as found.
5. Subagents may not write report files. Return the report text, and the orchestrator writes `docs/report.md`.
6. mypy on `experiments/` needs `follow_imports = "skip"` for cryptofeed. Otherwise its results depend on whether the bench group is installed, because the installed package is partly typed.
7. A pooled rate can hide a venue split. The gap monitor showed 0.333 for every fault, including clean streams, because it alarms on every OKX stream (exponential arrivals) and only on re-subscriptions elsewhere. Table 3 now reports it per venue.
8. The rotation experiment runs on wall-clock time, so do not run it while another CPU-heavy job runs. Its numbers vary slightly between runs, and the detection and regions numbers are deterministic.
9. Certify synthetic streams with `quality.coverage_window: observed`, because a full-day window marks a stream of less than a minute as uncovered. The runner sets this in report_settings().
10. A merge cannot recover a message that both regions lost. 30 of 100 merged days stayed degraded, because a drop in one region fell inside the outage of the other.
