# Market Tape Recorder (I1)

A self-hosted recorder for Moscow Exchange market data. It captures order books, trades, candles, trading
status and the AlgoPack Super Candles from two sources, the T-Bank Invest API stream and MOEX AlgoPack, writes
one normalised Parquet tape (contract 6.2, revision 2), and issues a quality certificate per instrument-day. The
certificate records crossed books, duplicates, blind intervals at re-subscription, silence, clock skew and
coverage of the MOEX session, and checks sequence gaps where a source has sequence numbers. In the platform it
is the first link of the real-time RL loop, and collection has the highest computing priority on the host.

Status (2026-10-29): MOEX adaptation done and tested offline on fixtures that follow the documented payloads.
No live token has been used yet, so nothing has been recorded from the real market. The crypto venues of v0.1
(Binance, Bybit, OKX, Hyperliquid) were removed and remain in git history.

Specification: `../quant_platform_plan_full.md`, project I1 and section 5.4; brief
`../prompts/02-market-tape-recorder.md` (revision 2); source facts `../docs/moex-sources.md`.

## Quickstart

```bash
cp .env.example .env     # set TRADEQ_DATA_DIR, and TBANK_TOKEN / ALGOPACK_TOKEN when you have them
make setup               # uv venv --python 3.13 .venv, uv sync --frozen
make lint typecheck test # ruff, mypy --strict, pytest with coverage (offline, about 5 s)
make reproduce           # offline demo: two capture hosts, injected faults, certify, merge, archive
make record              # live recording of every venue whose token is set; status on 127.0.0.1:8602
make backfill VENUE=moex-tbank FROM=2026-10-01 TO=2026-10-28   # history into the same tape
make test-live           # live checks per venue, skipped when its token is missing
```

CLI: `tape record | backfill | certify | merge-regions | archive | replay`.

## Sources

| Venue | Source | Transport | Channels | Token |
|---|---|---|---|---|
| `moex-tbank` | T-Bank Invest API | WebSocket `MarketDataStream` (JSON proxy) + REST | `book_snapshot` (20 levels, full snapshot per message), `trades`, `candles` (1 min), `trading_status` | `TBANK_TOKEN` (free) |
| `moex-algopack` | MOEX AlgoPack (`apim.moex.com`) | REST polling every 2 s | `book_snapshot` (20 levels, `SEQNUM`), `trades` (`TRADENO` cursor), `candles`, `trading_status`, `tradestats`, `orderstats`, `obstats` | `ALGOPACK_TOKEN` (paid) |
| `moex-iss` | public ISS | REST polling | `trades`, `candles`, `trading_status` (15-minute delay) | none; disabled by default |

A venue whose token is missing is skipped by `tape record`. Lot sizes and price ticks come from config and
are checked at start (T-Bank `ShareBy`, ISS `securities` table); a mismatch is logged. Sizes in the tape are
shares, not lots.

## Configuration

Defaults live in `config/settings.yaml`. Environment variables override them with the prefix `TAPE_` and the
nested delimiter `__`, for example `TAPE_SESSION__REDUNDANT_CONNECTIONS=3`. Tokens are read only from the
variable named by `venues.<v>.token_env`.

| Setting | Default | Meaning |
|---|---|---|
| `region` | eu-local | label of the capture host, written into every row |
| `session.redundant_connections` | 2 | always-on stream connections, deduplicated by earliest arrival |
| `session.overlap_rotation` | true | open the replacement connection and wait for its data before closing the old one |
| `venues.<v>.symbols` | SBER, GAZP, LKOH | native ticker to `<SECID>-RUB-<BOARD>`, lot size, tick, board |
| `venues.moex-algopack.poll_every` | see file | slow resources (candles, Super Candles) run every N rounds |
| `quality.venue_overrides` | see file | per-source thresholds (T-Bank books are sparse; ISS stamps to the second) |
| `priority.*` | nice 0, lag limit 200 ms | collector priority and the `/v1/load` pressure limits |
| `backfill.min_request_interval_s` | 0.2 | pacing of history requests inside the documented API limits |

## Architecture

```
transport (websockets with token and subprotocol, httpx GET/POST) --> session (backoff, overlapping rotation)
  --> adapters/{tbank,moex} (parse, lots->shares, Moscow time->UTC) --> dedup --> writer (Parquet, atomic)
tape/ --> quality/certify --> certificates/        two capture trees --> merge (earliest arrival) --> tape/
history.py (tape backfill) --> same adapters and writer        recorder --> /v1/load (collector pressure)
```

`pipeline.py` is the shared path for live recording, backfill and `replay.py`, so a replayed raw log produces the
same tape. T-Bank stream messages carry only `figi` and `instrumentUid`, so the recorder resolves every symbol
through `ShareBy` before streaming; the answer passes through the pipeline and is kept in the raw log.

Collection first: the recorder applies `priority.nice`, samples event-loop lag, and serves `GET /v1/load`
(`pressure: ok|high`). Other jobs on the host (the I2 follower, training, reports) poll it and pause while
pressure is high.

## Contracts

Produced: contract 6.2 revision 2 (conventions section 6.2): the Parquet tape under
`$TRADEQ_DATA_DIR/market-tape-recorder/tape/venue=<v>/channel=<c>/symbol=<s>/date=<d>/part-<n>.parquet` and
certificates under `certificates/venue=<v>/symbol=<s>/date=<d>.json`. Consumed: none.

| Extension | Detail |
|---|---|
| Channels `candles`, `trading_status`, `tradestats`, `orderstats`, `obstats` | revision 2 of contract 6.2 |
| `trades.trading_session` | ISS `TRADINGSESSION` (0 morning, 1 main, 2 evening) |
| Super Candles `ts_published` | AlgoPack `SYSTIME`, the moment the bar became knowable |
| Certificate extra fields | `generated_at`, `window_*`, `capture_regions`, `rows`, `connections`, `resubscription`, `book`, `clock_skew*` |
| `GET /health`, `/v1/streams`, `/v1/venues`, `/v1/load` | status service during `tape record` |

## Results

The research report [docs/report.md](docs/report.md) describes the crypto version of v0.1 and is outdated
after the MOEX pivot; its experiments were removed with the crypto adapters and are reproducible from commit
`e845dbc`. The MOEX demo (`make reproduce`) shows each injected fault caught or recovered: a blind
re-subscription and a crossed book are flagged on the host that suffered them, a lost AlgoPack poll is recovered
by the `TRADENO` cursor, and the merged tape of the two hosts is certified.

## Limitations

1. No live run yet: the T-Bank stream path and the response shapes are coded from the documentation and the
   official protobuf contracts, and the first token run is the real test (`make test-live`).
2. T-Bank trades have no trade id or sequence number, so a lost trade on the stream cannot be detected from the
   stream alone; the certificate checks staleness, crossed books and coverage there.
3. MOEX `TRADENO` is exchange-wide, so jumps between one security's trades are not gaps, and AlgoPack book
   snapshots are polled, so changes between two polls are not seen.
4. The cross-source check (T-Bank book against AlgoPack book for the same moment) is not implemented yet.

## Roadmap

1. First live run with `TBANK_TOKEN`, then with `ALGOPACK_TOKEN`; fix any payload difference with a fixture.
2. Cross-source certificate check between `moex-tbank` and `moex-algopack`.
3. ISS+ STOMP WebSocket for AlgoPack order books (`MXSE.orderbooks`, snapshot and `updates`), which would give
   real deltas and sequence checks.
4. Rerun a MOEX version of the report experiments once real days exist.
