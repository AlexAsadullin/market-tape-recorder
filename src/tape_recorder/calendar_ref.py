"""``tape calendar``: dividends and report dates of the T-Capital funds and of their holdings.

Holdings are the IMOEX constituents (MOEX ISS ``statistics/.../analytics/IMOEX``, one snapshot per
month since ``--since``) plus the funds themselves (TPAY pays monthly distributions). For each of
them the read-only T-Invest methods ``GetDividends`` and ``GetAssetReports`` are called, and the
answers become ``macro.ru_vintages`` rows for I2 (``pit fetch-macro --calendar <file>``):

* ``cal.div.<TICKER>.<last_buy_date>`` (yield %, from ``yield_value`` or amount / close) and
  ``cal.divamt.<TICKER>.<last_buy_date>`` (RUB per share): valid time = declared date, knowledge
  time = declared date + 1 day 00:00 MSK;
* ``cal.report.<TICKER>.<report_date>``: valid time = the record's creation day, knowledge time =
  its ``created_at``, value = 1 quarter, 2 half-year, 3 annual.

The event date is in the series id and the valid time is the announcement, so an as-known-at
query at T returns exactly the events announced by T. ``Cancelled`` dividends are skipped.
Research basis: ``robust-mm-rl/docs/research-funds-features.md``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import orjson
import structlog

from tape_recorder.adapters.tbank import TBankAdapter, quotation, snake_keys
from tape_recorder.transport import TRANSPORT_ERRORS, HttpFetcher

log = structlog.get_logger(__name__)
MSK = ZoneInfo("Europe/Moscow")
PERIODS = {"PERIOD_TYPE_QUARTER": 1.0, "PERIOD_TYPE_SEMIANNUAL": 2.0, "PERIOD_TYPE_ANNUAL": 3.0}


def _ns(moment: datetime) -> int:
    return int(moment.timestamp() * 1e9)


def _ts(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _msk_midnight(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=MSK)


def dividend_rows(ticker: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for d in snake_keys(payload).get("dividends") or []:
        if str(d.get("dividend_type", "")).lower() == "cancelled":
            continue
        last_buy = _ts(d.get("last_buy_date"))
        declared = _ts(d.get("declared_date")) or _ts(d.get("created_at"))
        if last_buy is None or declared is None:
            continue
        amount = quotation(d.get("dividend_net"))
        if amount is None or amount <= 0:
            continue
        yld = quotation(d.get("yield_value"))
        close = quotation(d.get("close_price"))
        if not yld and close:
            yld = amount / close * 100.0
        known = _ns(_msk_midnight(declared.astimezone(MSK).date() + timedelta(days=1)))
        valid = _ns(_msk_midnight(declared.astimezone(MSK).date()))
        event = last_buy.astimezone(MSK).date().isoformat()
        row = {"date": valid, "announced_at": known}
        out.append({"series_id": f"cal.divamt.{ticker}.{event}", "value": float(amount), **row})
        if yld:
            out.append({"series_id": f"cal.div.{ticker}.{event}", "value": float(yld), **row})
    return out


def report_rows(ticker: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for e in snake_keys(payload).get("events") or []:
        report = _ts(e.get("report_date"))
        created = _ts(e.get("created_at"))
        if report is None or created is None:
            continue
        out.append(
            {
                "series_id": f"cal.report.{ticker}.{report.astimezone(MSK).date().isoformat()}",
                "date": _ns(_msk_midnight(created.astimezone(MSK).date())),
                "value": PERIODS.get(str(e.get("period_type")), 0.0),
                "announced_at": _ns(created),
            }
        )
    return out


async def imoex_constituents(
    client: httpx.AsyncClient, iss_url: str, start: date, today: date
) -> set[str]:
    """Every ticker that was in IMOEX in any monthly snapshot from ``start`` to ``today``."""
    tickers: set[str] = set()
    month = start.replace(day=1)
    while month <= today:
        r = await client.get(
            iss_url.rstrip("/")
            + "/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json",
            params={"date": month.isoformat(), "limit": "100", "iss.meta": "off"},
        )
        r.raise_for_status()
        block = r.json().get("analytics") or {}
        cols = block.get("columns") or []
        if "ticker" in cols:
            i = cols.index("ticker")
            tickers |= {row[i] for row in block.get("data") or []}
        month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
    return tickers


async def build_calendar(
    adapter: TBankAdapter,
    fetcher: HttpFetcher,
    instruments: list[dict[str, Any]],
    start: date,
    horizon_days: int,
    interval_s: float,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Dividends and report dates for universe entries (``ticker``, ``instrument_uid``)."""
    lo = _ns(datetime(start.year, start.month, start.day, tzinfo=UTC))
    hi = _ns(datetime.now(UTC) + timedelta(days=horizon_days))
    rows: list[dict[str, Any]] = []
    stats = {"instruments": 0, "dividends": 0, "reports": 0, "failed": 0}
    for inst in instruments:
        ticker, uid = str(inst["ticker"]), str(inst["instrument_uid"])
        stats["instruments"] += 1
        for build, parse, key in (
            (adapter.dividends_request, dividend_rows, "dividends"),
            (adapter.asset_reports_request, report_rows, "reports"),
        ):
            body = None
            for attempt in range(4):
                await sleep(interval_s)
                try:
                    body = await fetcher(build(uid, lo, hi))
                    break
                except TRANSPORT_ERRORS as exc:
                    if (
                        isinstance(exc, httpx.HTTPStatusError)
                        and exc.response.status_code not in (429,)
                        and (exc.response.status_code < 500)
                    ):
                        log.info(
                            "calendar_none",
                            ticker=ticker,
                            kind=key,
                            status=exc.response.status_code,
                        )
                        break
                    log.warning(
                        "calendar_retry",
                        ticker=ticker,
                        kind=key,
                        attempt=attempt + 1,
                        error=repr(exc),
                    )
                    await sleep(2.0 * (attempt + 1))
            if body is None:
                stats["failed"] += 1
                continue
            found = parse(ticker, orjson.loads(body))
            stats[key] += len(found)
            rows += found
    return rows, stats
