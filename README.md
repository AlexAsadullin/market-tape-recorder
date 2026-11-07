# Market Tape Recorder (I1)

A self-hosted recorder for Moscow Exchange market data. It captures order books, trades, candles, trading
status and the AlgoPack Super Candles from two sources, the T-Bank Invest API stream and MOEX AlgoPack, writes
one normalised Parquet tape (contract 6.2, revision 2), and issues a quality certificate per instrument-day. The
certificate records crossed books, duplicates, blind intervals at re-subscription, silence, clock skew and
coverage of the MOEX session, and checks sequence gaps where a source has sequence numbers. In the platform it
is the first link of the real-time RL loop, and collection has the highest computing priority on the host.

Status (2026-11-05): first live runs done for T-Bank (pinned TLS, stream and REST) and public ISS; both
captures are certified on the observed window. The crypto venues of v0.1 were removed and remain in git
history.

Specification: `../quant_platform_plan_full.md`, project I1 and section 5.4; brief
`../prompts/02-market-tape-recorder.md` (revision 2); source facts `../docs/moex-sources.md`.

## Quickstart

```bash
cp .env.example .env     # set TRADEQ_DATA_DIR, TBANK_TOKEN / ALGOPACK_TOKEN
make setup               # uv venv --python 3.13 .venv, uv sync --frozen
make lint typecheck test # ruff, mypy --strict, pytest with coverage (offline; pytest about 7 s, mypy longer on a cold cache)
make reproduce           # offline demo: two capture hosts, injected faults, certify, merge, archive
make record              # live recording of every venue whose token is set; status on 127.0.0.1:8602
make backfill VENUE=moex-tbank FROM=2026-10-01 TO=2026-10-28   # history into the same tape
make test-live           # live checks per venue (30 s capture each), skipped when its token is missing
uv run tape instruments-ref   # regenerate reference/tbank_zero_fee_funds.json (T-Bank's own funds)
uv run tape universe          # reference/universe_moex.json: TQBR shares + MOEX-listed T-Capital funds
uv run tape download --interval day      # full daily history for the universe (resumable)
uv run tape download --interval minute   # last 12 months of 1-minute candles (resumable)
../scripts/download-all.sh --smoke       # whole chain for SBER, GAZP, TMOS, then I2 ingest
```

CLI: `tape record | backfill | certify | merge-regions | archive | replay | instruments-ref | universe |
download | pin-show`. Run it from
the repository directory: `.env` is looked up from the working directory.

## Token safety

The T-Bank token is used for market data and instrument reference data only; trading happens in the
platform's own sandbox, never through T-Bank. The adapter builds REST calls only for the read-only methods
in `ALLOWED_REST_METHODS` (`ShareBy`, `Etfs`, `GetAssetBy`, `GetCandles`, `GetLastTrades`, `GetOrderBook`),
refuses any other stream than `MarketDataStream` and any sandbox host, and `tests/unit/test_tbank_safety.py`
fails if an order, stop-order, sandbox, operations or account method becomes reachable. Request headers are
never serialised into raw logs or fixtures. TLS verification is never switched off.

## TLS pinning (T-Bank)

T-Bank's hosts chain to the Russian Trusted Root CA, which is not trusted anywhere in this project. Instead
`http.pins` pins the leaf public key per host (`sha256/<base64>` of the SPKI): for a pinned host the
connection is accepted only when the leaf key matches a pin and its subjectAltName matches the host; the
chain is not checked for that host only. A mismatch fails closed with `pinned key changed for <host>: got
sha256/..., expected ...`. Every other host (ISS, AlgoPack) keeps certifi validation. REST and the
`MarketDataStream` WebSocket both use `invest-public-api.tbank.ru`.

Re-pin on rotation: run `uv run tape pin-show invest-public-api.tbank.ru` (TLS handshake only, no request,
no token), compare its `sha256 cert` fingerprint with the browser (padlock, certificate, SHA-256
fingerprint), add the new `pin` next to the old one in `config/settings.yaml`, update the dated comment,
and remove the old pin after the switch.

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
| `http.pins` | T-Bank leaf pin | per-host public-key pins (see TLS pinning) |
| `reference.zero_fee_*` | `asset.brand.company`/`name`, T-Capital pattern | the rule of `tape instruments-ref` (verified 2026-11-05: `asset.brand.company = Т-Капитал`) |

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

## First live run (2026-11-05)

| Source | What ran | Result |
|---|---|---|
| `moex-iss` (public) | `pytest -m live -k iss` (30 s) and `tape record --venue moex-iss --duration-s 120 --raw-log` for SBER, GAZP, LKOH | passed; 293,099 rows (trades from the morning session on, 927 1-minute candles per name, trading status); no parse errors, no crossed books, no gaps |
| `moex-tbank` | `pytest -m live -k tbank` (30 s), `tape record --venue moex-tbank --duration-s 120 --raw-log`, `tape instruments-ref` | passed; 24,049 rows (books, trades, 1-minute candles, status), 0 parse errors, 859 duplicates removed across 2 connections; SBER and GAZP `certified`, LKOH `degraded` (coverage 0.785, a quiet name) on the observed window; requests answered in about 0.3 s, no throttling |

Findings: without `from`, ISS `candles.json` answers with the oldest candles of the security (2011 for SBER),
so the first live poll wrote 2011 bars into today's partition; the first poll now starts at 00:00 Moscow time
of the current day (that capture was moved to `quarantine/` in the data root). The first trades poll replays
the whole day from `TRADENO` 0, so the clock-skew median of that capture is hours, not the 15-minute delay
(the minimum skew, 900.3 s, confirms the delay); certificates of such a short capture are `failed` on the
day window (coverage about 0.1%) and `degraded` on the observed window.

T-Bank findings: the payloads match the documented shapes; real messages also carry `ticker` and
`classCode`, and candles carry `volumeBuy`, `volumeSell` and `candleSourceType`. Lot sizes and ticks matched
config. Subscriptions without a type default to `ORDERBOOK_TYPE_ALL` and all trade sources, which mix in
T-Bank's own dealer market; the adapter now asks for exchange books, trades and candles only and drops
dealer messages. A real excerpt is in `tests/fixtures/real/moex-tbank-2026-11-05.json`. The zero-fee list
(`reference/tbank_zero_fee_funds.json`) holds every listing of each T-Capital fund, because the waiver belongs
to the fund, not to one listing: 167 listings (28 on MOEX `TQTF`, 20 on `TQBR`, 12 on `SPBRU`, the rest on
OTC and internal boards) out of 2,275 in `INSTRUMENT_STATUS_BASE` + `INSTRUMENT_STATUS_ALL`, merged by uid,
selected by `asset.brand.company` (`Т-Капитал`, or the former `Тинькофф Капитал` for 2). Each entry has
`api_tradable` (in `BASE` with `api_trade_available_flag`; 23 are); most funds are API-tradable only on
`SPBRU` (`TMOS@`), while their MOEX `TQTF` listings (`TMOS`) are not. Top-level `base_tickers` (66) strips
everything from `@` and a trailing `_OTC`. One run lost assets to HTTP 429 and silently dropped funds, so
`instruments-ref` now waits for `x-ratelimit-reset`, retries, and refuses to write a partial list. No geo-block
was seen from the Netherlands; the median clock skew of the stream was about 180 ms.

## Bulk history download

Progress: `download/progress-<1d|1m>.json` under the project data root is rewritten on every tick (contract 6.2).

`tape universe` lists `Shares` (status `download.shares_status`, default BASE) and `Etfs` (ALL) and keeps
TQBR shares plus the T-Capital funds of the zero-fee list that trade on TQTF: 282 instruments on
2026-11-06 (254 shares, 28 funds). It also writes `reference/moex_instruments_i2.jsonl` for I2's
`moex.instruments`. The full `Shares` ALL listing is about 12 MB and T-Bank closes slow transfers after
about 30 s, so listings go through the retrying fetcher.

`tape download` writes day and 1-minute candles through the T-Bank adapter into the normal tape (contract
6.2), so `pit follow --once` ingests them unchanged. It is paced at 80% of `x-ratelimit-limit`, retries
429/5xx/transport errors, keeps a checkpoint per venue, symbol and interval under
`download/checkpoints/` (a rerun fetches only what is missing; the last two bars are always refetched),
seeds deduplication with the bars already in the tape, runs at `nice` +10 and pauses while the recorder
reports `pressure: high`. Public ISS fills what T-Bank lacks. Each run writes a quality report
(rows per instrument, missing trading days) to `download/reports/`. `scripts/download-all.sh` in the
umbrella runs the whole chain: zero-fee list (if older than 7 days), universe, daily, 1-minute, certify,
I2 ingest.

Smoke run 2026-11-06 (SBER, GAZP, TMOS): 828 day rows for one year (3 requests) and 6,350 one-minute
rows for 3 days (9 requests), 0 failures; a rerun added 0 rows; I2 ingested 7,181 rows. Full-run estimate
for 282 instruments: about 1,500 daily requests and about 85,000 one-minute requests (one per instrument
and trading day); at the observed 160-480 requests per minute that is 3-9 hours, roughly 40 million rows
and 1-2 GB of Parquet.

## Limitations

1. The T-Bank pin must be renewed when T-Bank rotates its key (the leaf is valid until 2027-08-24); a
   rotation fails closed until the new pin is added.
2. T-Bank trades have no trade id or sequence number, so a lost trade on the stream cannot be detected from the
   stream alone; the certificate checks staleness, crossed books and coverage there.
3. MOEX `TRADENO` is exchange-wide, so jumps between one security's trades are not gaps, and AlgoPack book
   snapshots are polled, so changes between two polls are not seen.
4. The cross-source check (T-Bank book against AlgoPack book for the same moment) is not implemented yet.
5. The universe leaves out TQBR names that are not tradable through the API (the ALL listing is cut off).
6. The download calendar counts weekend sessions, in which shares trade but funds do not, so funds show
   weekend "gaps" (use `missing_weekdays`). T-Bank's TMOS day candles stop at 2026-06-19 (not explained
   yet; possibly a new listing).

## Roadmap

1. A full T-Bank day capture and its certificate; then the `ALGOPACK_TOKEN` run.
2. Cross-source certificate check between `moex-tbank` and `moex-algopack`.
3. ISS+ STOMP WebSocket for AlgoPack order books (`MXSE.orderbooks`, snapshot and `updates`), which would give
   real deltas and sequence checks.
4. Rerun a MOEX version of the report experiments once real days exist.
