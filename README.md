# Market Tape Recorder (I1)

A self-hosted recorder for crypto exchange market data (Binance, Bybit, OKX), the Hyperliquid on-chain perp
exchange and MOEX. It writes one normalised Parquet tape (contract 6.2) and issues a quality certificate per
instrument-day, which records sequence gaps, out-of-order and duplicate messages, crossed books, blind intervals
at re-subscription, clock skew and coverage. On synthetic streams with one injected fault each, the certificate
caught 100% of the faults (95% CI 99.4% to 100%, 600 runs per fault type) with no false alarm on 600 clean
streams. Overlapping reconnects lost 0.0 trades per planned reconnect, where naive reconnects lost 75.0 (95% CI 68.5 to 81.4, 20 runs each). Details are in [docs/report.md](docs/report.md).

Specification: `quant_platform_plan_full.md`, project I1, and `../prompts/02-market-tape-recorder.md`.

## Quickstart

```bash
cp .env.example .env     # set TRADEQ_DATA_DIR=/Users/.../Tradeq/data
make setup               # uv venv --python 3.13 .venv, uv sync --frozen
make lint typecheck test # ruff, mypy --strict, pytest with coverage (offline, about 10 s)
make reproduce           # offline demo: two simulated regions, certify, merge, archive
make report              # experiments behind docs/report.md, about 8 minutes (installs the bench group)
make record              # live recording; status service on 127.0.0.1:8602
make test-live           # 6 tests against the real venues (network)
```

CLI: `tape record | certify | merge-regions | archive | replay`.

## Configuration

Defaults live in `config/settings.yaml`. Environment variables override them with the prefix `TAPE_` and the
nested delimiter `__`, for example `TAPE_SESSION__REDUNDANT_CONNECTIONS=3`. `TRADEQ_DATA_DIR` is the data root,
`TAPE_CONFIG_FILE` selects another settings file, and `TAPE_REPORT_CONFIG_FILE` another experiment file
(default `config/report.yaml`).

| Setting | Default | Meaning |
|---|---|---|
| `region` | eu-local | capture region written into every row |
| `session.redundant_connections` | 2 | always-on connections per venue, deduplicated by earliest arrival |
| `session.overlap_rotation` | true | open the replacement connection and wait for its data before closing the old one |
| `session.rotate_after_s` | 72000 | planned reconnect interval (venues drop connections at about 24 h) |
| `dedup.window_s` | 120 | window in which copies from redundant connections are recognised |
| `quality.checks` | see file | threshold, direction and severity of each certificate check |
| `quality.coverage_window` | day | `day` (venue session) or `observed` (first to last message) |
| `venues.<v>.symbols` | see file | native symbol to normalised `<BASE>-<QUOTE>-<KIND>` |

## Architecture

```
transport (websockets, httpx) --> session (backoff, overlapping rotation, resync)
  --> adapters/<venue> (parse, sequencing rules) --> dedup (earliest arrival) --> writer (Parquet, atomic)
tape/ --> quality/certify --> certificates/        two capture trees --> merge (earliest arrival) --> tape/
tape/ + certificates/ --> archive (monthly bundle with QUALITY.md)
```

`pipeline.py` is the shared path for live recording and `replay.py`, so a replayed raw log produces the same
tape. `sequencing.py` holds one continuity rule that covers Binance spot and USD-M, Bybit and OKX update ids.
`faults.py` injects deterministic faults for tests, the demo and the report experiments (`experiments/`).

## Contracts

Produced: contract 6.2 (conventions section 6.2), the Parquet tape under
`$TRADEQ_DATA_DIR/market-tape-recorder/tape/venue=<v>/channel=<c>/symbol=<s>/date=<d>/part-<n>.parquet` and the
quality certificates under `certificates/venue=<v>/symbol=<s>/date=<d>.json`. Consumed: none.

Optional extensions (no required field was added or changed):

| Extension | Detail |
|---|---|
| Certificate extra fields | `generated_at`, `window_start_ns`, `window_end_ns`, `capture_regions`, `rows`, `connections`, `resubscription`, `book`, `clock_skew`, `clock_skew_by_region` |
| `Check` extra fields | `direction`, `severity` |
| `Gap` extra fields | `channel`, `detail` |
| `latency/` | per-message arrival times of both regions and their latency distributions, written by `merge-regions` |
| `GET /health`, `GET /v1/streams`, `GET /v1/venues` | status service during `tape record` |

## Key results

Synthetic streams in the real venue wire formats (`make report`, seed 20261022); see
[docs/report.md](docs/report.md) for the method and all tables.

| Result | Value (95% CI) | Runs |
|---|---|---|
| Injected faults caught by the certificate (drop, reorder, cross, re-subscription, duplicate) | 100% (99.4% to 100%) per fault type | 600 per type |
| False alarms of the certificate on clean streams | 0.0% (0.0% to 0.6%) | 600 |
| Duplicates caught on Binance, certificate vs cryptofeed 3.0.1 | 100% (98.1% to 100%) vs 0.0% (0.0% to 1.9%) | 200 |
| Trades lost per planned reconnect, naive vs overlapping rotation | 75.0 (68.5 to 81.4) vs 0.0 (0.0 to 0.0) | 20 per mode |
| Days certified after two regions with independent losses are merged, vs one region | 70.0% (60.4% to 78.1%) vs 0.0% (0.0% to 3.7%) | 100 |

## Limitations

The evidence is synthetic: the experiments measure software behaviour on generated streams and a fake venue,
not market data quality. The certificate audits sequence ids, so it cannot see losses on channels without them
(Hyperliquid books, MOEX polling); there it falls back to coverage and silence checks. A merge cannot recover a
message that both regions lost, which left 30% of merged days degraded in the regions experiment. Checksum
validation (OKX `checksum`) is not implemented.

## Roadmap

1. OKX CRC32 checksum validation as a further certificate check.
2. A long-running live soak test across a daily venue disconnect, to measure the real blind interval.
3. A vn.py CTP gateway adapter for Chinese futures, as the specification lists as optional.
