"""``tape universe``: the instrument universe of the bulk history download.

The universe is every share on the configured share boards (``TQBR``) from
``InstrumentsService/Shares`` with ``download.shares_status`` (default ``INSTRUMENT_STATUS_BASE``:
the ``ALL`` listing is ~12 MB across every exchange and T-Bank closes the connection
part-way through it, so names not tradable through the API are left out), plus the T-Capital
funds of the zero-fee list (``reference/tbank_zero_fee_funds.json``) listed on the fund boards
(``TQTF``),
with their trading data from ``InstrumentsService/Etfs`` (``ALL``). Each entry carries the fields
the download needs (uid, lot, tick, the first 1-minute and 1-day candle dates) and the tape
symbol ``<SECID>-RUB-<BOARD>``.

Two files are written to ``reference/``: ``universe_moex.json`` (the full document) and
``moex_instruments_i2.jsonl``, the shape of I2's file-fed dataset ``moex.instruments``
(``symbol, secid, board, lot_size, tick, currency, effective_from, published_at``). An entry whose
lot, tick, board and currency did not change keeps its earlier ``effective_from`` and
``published_at``, so re-ingesting the file adds no revision; a change is stamped with the time it
was observed, which is the honest knowledge time.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from pathlib import Path
from typing import Any

import orjson

from tape_recorder.adapters.tbank import TBankAdapter, quotation, snake_keys
from tape_recorder.settings import DownloadSettings
from tape_recorder.timeutil import Clock, ns_to_iso
from tape_recorder.transport import HttpFetcher

UNIVERSE_FIELDS = (
    "ticker",
    "figi",
    "instrument_uid",
    "class_code",
    "lot",
    "min_price_increment",
    "currency",
    "first_1min_candle_date",
    "first_1day_candle_date",
    "trading_status",
    "name",
    "sector",
)
I2_COLUMNS = (
    "symbol",
    "secid",
    "board",
    "lot_size",
    "tick",
    "currency",
    "effective_from",
    "published_at",
)
_RULE_COLUMNS = ("board", "lot_size", "tick", "currency")


def tape_symbol(ticker: str, board: str, currency: str | None = "rub") -> str:
    """``SBER``/``TQBR`` -> ``SBER-RUB-TQBR`` (contract 6.2)."""
    return f"{ticker}-{(currency or 'rub').upper()}-{board}"


def instrument_entry(doc: dict[str, Any], kind: str) -> dict[str, Any]:
    """A snake_case ``Share`` or ``Etf`` record -> one universe entry."""
    ticker, board = str(doc["ticker"]), str(doc["class_code"])
    return {
        "symbol": tape_symbol(ticker, board, doc.get("currency")),
        "kind": kind,
        "ticker": ticker,
        "figi": doc.get("figi"),
        "instrument_uid": doc.get("uid"),
        "class_code": board,
        "isin": doc.get("isin"),
        "lot": int(doc["lot"]) if doc.get("lot") is not None else None,
        "min_price_increment": quotation(doc.get("min_price_increment")),
        "currency": doc.get("currency"),
        # snake_keys turns first1minCandleDate into first1min_candle_date (no break at the digit)
        "first_1min_candle_date": doc.get("first1min_candle_date")
        or doc.get("first_1min_candle_date"),
        "first_1day_candle_date": doc.get("first1day_candle_date")
        or doc.get("first_1day_candle_date"),
        "trading_status": doc.get("trading_status"),
        "api_trade_available": doc.get("api_trade_available_flag"),
        "name": doc.get("name"),
        "sector": doc.get("sector") or None,
    }


def select_universe(
    shares: Iterable[dict[str, Any]],
    etfs: Iterable[dict[str, Any]],
    zero_fee: dict[str, Any],
    settings: DownloadSettings,
) -> list[dict[str, Any]]:
    """Shares on the share boards plus the zero-fee funds on the fund boards (snake_case input)."""
    out: dict[str, dict[str, Any]] = {}
    boards = set(settings.share_boards)
    for share in shares:
        if share.get("class_code") in boards and share.get("ticker") and share.get("uid"):
            entry = instrument_entry(share, "share")
            out[entry["symbol"]] = entry
    fund_boards = set(settings.fund_boards)
    wanted = {
        str(f["instrument_uid"])
        for f in zero_fee.get("funds", [])
        if f.get("class_code") in fund_boards and f.get("instrument_uid")
    }
    found: set[str] = set()
    for etf in etfs:
        uid = str(etf.get("uid"))
        if uid in wanted and etf.get("class_code") in fund_boards:
            entry = instrument_entry(etf, "fund")
            out[entry["symbol"]] = entry
            found.add(uid)
    missing = wanted - found
    if missing:
        raise ValueError(f"{len(missing)} zero-fee funds are not in Etfs: {sorted(missing)[:5]}")
    return sorted(out.values(), key=lambda e: (e["kind"] != "share", e["symbol"]))


def universe_document(entries: list[dict[str, Any]], now_ns: int) -> dict[str, Any]:
    return {
        "source": "T-Bank Invest API: InstrumentsService/Shares (download.shares_status) and "
        "InstrumentsService/Etfs (INSTRUMENT_STATUS_ALL); "
        "funds from reference/tbank_zero_fee_funds.json",
        "retrieved_at": ns_to_iso(now_ns),
        "count": len(entries),
        "count_by_kind": dict(sorted(Counter(e["kind"] for e in entries).items())),
        "count_by_class_code": dict(sorted(Counter(e["class_code"] for e in entries).items())),
        "count_by_trading_status": dict(
            sorted(Counter(str(e["trading_status"]) for e in entries).items())
        ),
        "instruments": entries,
    }


def i2_rows(
    entries: list[dict[str, Any]], now_ns: int, previous: dict[str, dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Rows of I2's ``moex.instruments``; unchanged rules keep their earlier times."""
    previous = previous or {}
    stamp = ns_to_iso(now_ns)
    rows = []
    for e in entries:
        row: dict[str, Any] = {
            "symbol": e["symbol"],
            "secid": e["ticker"],
            "board": e["class_code"],
            "lot_size": float(e["lot"]) if e["lot"] is not None else None,
            "tick": e["min_price_increment"],
            "currency": e["currency"],
            "effective_from": stamp,
            "published_at": stamp,
        }
        old = previous.get(e["symbol"])
        if old is not None and all(old.get(c) == row[c] for c in _RULE_COLUMNS):
            row["effective_from"] = old["effective_from"]
            row["published_at"] = old["published_at"]
        rows.append(row)
    return rows


def read_i2_rows(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows = (orjson.loads(line) for line in path.read_bytes().splitlines() if line.strip())
    return {str(r["symbol"]): r for r in rows}


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(b"".join(orjson.dumps(r) + b"\n" for r in rows))
    tmp.replace(path)


async def build_universe(
    adapter: TBankAdapter,
    fetcher: HttpFetcher,
    zero_fee: dict[str, Any],
    settings: DownloadSettings,
    clock: Clock,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    """Fetch the share and fund listings (two listing calls, paced) and select the universe."""
    shares = snake_keys(orjson.loads(await fetcher(adapter.shares_request(settings.shares_status))))
    await sleep(settings.listing_interval_s)
    etfs = snake_keys(orjson.loads(await fetcher(adapter.etfs_request("INSTRUMENT_STATUS_ALL"))))
    entries = select_universe(
        shares.get("instruments", []), etfs.get("instruments", []), zero_fee, settings
    )
    return universe_document(entries, clock())
