# HANDOVER: market-tape-recorder (Tradeq I1)

Last updated 2026-11-05 by the session of the first live run. Nothing of this session is committed.

## Goal

Self-hosted recorder for Moscow Exchange market data (MOEX only since 2026-10-29). Sources: T-Bank Invest API
(`moex-tbank`, WebSocket stream + REST), MOEX AlgoPack (`moex-algopack`, paid REST) and public ISS
(`moex-iss`, 15-minute delay, disabled by default). One normalised Parquet tape (contract 6.2 revision 2)
and a quality certificate per instrument-day. Spec: `../prompts/02-market-tape-recorder.md` (revision 2);
platform rules: `../prompts/00-platform-conventions.md`; vendor facts: `../docs/moex-sources.md`; T-Invest
protobuf contracts: `../docs/tinvest-contracts/`. Consumers: I2, S3, I6, I5, S1. Status port 8602.

User rule: the T-Bank token is ONLY for market-data download. Trading happens in the platform's local
sandbox, never through T-Bank. Never print, log or commit the token.

## Stack

Python 3.13, uv-managed `.venv`, pins from `../constraints.txt`. websockets 17, httpx, pyarrow, pydantic +
pydantic-settings, typer (`tape` CLI), FastAPI/uvicorn (status), structlog, orjson, sortedcontainers.
Settings: `config/settings.yaml`, overridden by `TAPE_*` env; tokens only from the env variable named by
`venues.<v>.token_env`. Data root `$TRADEQ_DATA_DIR/market-tape-recorder/`.

## Files

| Path | Purpose |
|---|---|
| src/tape_recorder/adapters/tbank.py | T-Bank stream + REST; `ALLOWED_REST_METHODS` read-only allowlist, `ForbiddenEndpointError` |
| src/tape_recorder/adapters/moex.py | AlgoPack and ISS polling (TRADENO, SEQNUM, candle cursor from today 00:00 MSK) |
| src/tape_recorder/pinning.py | leaf SPKI pinning (`http.pins`), `tape pin-show` helpers |
| src/tape_recorder/reference.py | `tape instruments-ref`: zero-fee fund list (Etfs + GetAssetBy, brand rule) |
| src/tape_recorder/transport.py | websockets/httpx; `TlsPolicy` (certifi, or the pin for pinned hosts), `http_client` |
| src/tape_recorder/session.py, recorder.py, service.py, monitor.py | live sessions, `tape record`, `/v1/load` |
| src/tape_recorder/history.py | `tape backfill` (T-Bank GetCandles, AlgoPack candles and Super Candles) |
| src/tape_recorder/universe.py, download.py | `tape universe`, `tape download` (bulk history: pacing, retries, checkpoints, quality report) |
| ../scripts/download-all.sh (umbrella) | the whole download chain, then `pit follow --once` |
| src/tape_recorder/schema.py, writer.py, quality/ | contract 6.2 schema (keeps v0.1 crypto channels on purpose), Parquet writer, certificate |
| src/tape_recorder/merge.py, archive.py, replay.py, rawlog.py, demo.py | region merge, archive, replay, raw log, `make reproduce` |
| tests/unit/test_tbank_safety.py | fails if an order/sandbox/operations/account call becomes reachable |
| tests/fixtures/venues/*.json, tests/fixtures/reference/tbank_etfs.json | documented-shape fixtures (demo and unit tests) |
| tests/fixtures/real/moex-tbank-2026-11-05.json | real T-Bank excerpt of the first live run (no headers) |
| tests/unit/test_pinning.py | pin match, key change, wrong host, unpinned hosts, pin-show sends nothing |

`experiments/` in the working tree holds only an ignored `__pycache__`; the v0.1 experiments are in git
history (`e845dbc`), and `docs/report.md` carries an "outdated" banner.

## State

1. `make lint typecheck test`: ruff and mypy clean, 138 passed, 3 live deselected, coverage 83% (the
   download run loop is covered by the live smoke run, not by unit tests).
   `make test-live`: ISS and T-Bank passed (re-run after the exchange-only change), AlgoPack skipped (no token).
2. Live run 2026-11-05: ISS live test passed; a 120 s `tape record --venue moex-iss --raw-log` (SBER, GAZP,
   LKOH) wrote 293,099 rows under `tape/venue=moex-iss/`, raw frames under `raw/`, certificates
   `failed` on the day window (coverage ~0.1%) and `degraded` on the observed window; no gaps, no crossed
   books, no parse errors. The first, buggy capture (2011 candles) is in `quarantine/2026-11-05-iss-candles-from-2011/`.
3. T-Bank works through SPKI pinning (no Russian CA is trusted anywhere): pin
   `sha256/xU/NS71QVL2qP8naQf6PUKFhVehLfvYnI4CFs5qo6iQ=` for invest-public-api.tbank.ru (REST and stream),
   leaf `*.tbank.ru` valid to 2027-08-24, cert SHA-256 `22:4F:05:8D:...:2D:06:23:99` (full value in
   config/settings.yaml). Re-pin: `tape pin-show <host>`, compare with the browser, add the new pin next to
   the old one. No geo-block or throttling from the Netherlands on 2026-11-05 (requests about 0.3 s).
4. T-Bank live run 2026-11-05 ~23:07 MSK: live test passed; 120 s record wrote 24,049 rows; certify on the
   observed window: SBER and GAZP certified, LKOH degraded (0.785). Fixed: subscriptions now ask for exchange
   books/trades/candles only (the defaults mix in T-Bank's dealer market) and dealer messages are dropped.
   Real excerpt: tests/fixtures/real/moex-tbank-2026-11-05.json.
5. `reference/tbank_zero_fee_funds.json`: every listing of each T-Capital fund (BASE + ALL Etfs merged by
   uid, 845 assets): 167 listings, 28 on TQTF, 20 TQBR, 12 SPBRU; `api_tradable` true for 23; rule
   `asset.brand.company` matches `Т-Капитал` (165) or `Тинькофф Капитал` (2); `base_tickers` (66) strips `@...`
   and `_OTC`. Consumers (I9 commission.py, I5 costs.py, S8 fees.py) read `funds[].ticker`/`instrument_uid`,
   so TMOS/TQTF, TGLD/TQTF and TPAY/TQBR resolve to 0% without changes. 429s are retried after
   `x-ratelimit-reset`; a list with unresolved assets is not written. A run takes about 5 minutes.
6. `staging/` holds 9 `.inprogress` files from the ISS run that was killed when the user paused work;
   they are orphans (the writer warns `staging_orphans`) and can be removed by the user.

7. Bulk download (2026-11-06): `tape universe` -> 282 instruments (254 TQBR shares, 28 TQTF funds);
   `scripts/download-all.sh --smoke` passed (828 day rows, 6,350 minute rows, I2 ingested 7,181). The
   full run on 2026-11-06 took 3.5 hours (17:22 to 20:52 MSK): day candles 765,090 rows from T-Bank plus
   63,826 from ISS before T-Bank's first candle; 1-minute 28,603,914 rows (85,124 requests); 0 failed chunks;
   certified; I2 ingested 29.4M rows (I2 parquet 1.1 GB, tape 1 GB). Day report: 253 of 282 instruments show
   calendar gaps, mostly weekend sessions that funds and thin names do not trade (see missing_weekdays).

Open items, in order: a full T-Bank day; AlgoPack token run; cross-source certificate check; ISS+
STOMP order books; a MOEX version of the report.

## Mistakes and lessons

1. pydantic-settings env names cannot address dict keys with a dash (`moex-tbank`): tokens use `token_env`,
   and enabling `moex-iss` needs a config file copy (`--config`), not `TAPE_VENUES__...`.
2. python-dotenv's default search starts at the calling module's directory, so `load_settings` found the
   repository `.env` (with the real token) even inside tests that chdir away. It now uses
   `find_dotenv(usecwd=True)`; run the CLI from the repository directory.
3. ISS `candles.json` without `from` returns the security's oldest candles; always pass `from`.
4. The first ISS trades poll replays the whole day from TRADENO 0, so the skew median of a short capture
   is hours; judge the delay by the minimum skew (900 s).
5. ISS candle `volume` is already in shares; order-book and trade quantities are lots.
6. Use `git -C`, `make -C` and single commands; compound shell commands may be denied. Subagents return
   report text instead of writing report files.
7. T-Bank docs and API use the Russian CA, which is not trusted here: pin the API host instead; the
   built-in browser and WebFetch refuse the docs site. The GitHub repo
   RussianInvestments/investAPI has the contracts and the AsyncAPI spec in plain files.
8. `instruments-ref` once dropped funds silently when GetAssetBy hit 429; a reference list must fail
   rather than be partial.
9. Live tests read the token from the process env or the repo .env before the test fixture clears it;
   after the dotenv fix they would otherwise skip T-Bank.
10. T-Bank does not compress replies and closes transfers after about 30 s, so a 3-12 MB listing over a
    slow link fails part-way; use the retrying fetcher and the BASE shares listing.
