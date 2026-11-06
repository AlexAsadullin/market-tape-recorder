"""``tape instruments-ref``: reference files from the T-Bank instruments service.

The zero-fee fund list holds the funds managed by T-Bank's own asset manager (T-Capital,
formerly Tinkoff Capital), which the broker trades without commission. They are identified from
API data only: ``InstrumentsService/Etfs`` lists the funds, ``GetAssetBy`` returns each fund's
asset with its ``brand`` (``name``, ``company``), and a fund is selected when one of
``reference.zero_fee_fields`` matches ``reference.zero_fee_pattern``. The file records the rule,
and every fund records the field and value that matched, so the selection can be audited.

The fee waiver belongs to the fund, not to one listing, so the list holds every listing of a
matched fund: both ``INSTRUMENT_STATUS_BASE`` (tradable through the API) and
``INSTRUMENT_STATUS_ALL`` are fetched, merged by instrument uid, and each entry carries
``api_tradable`` (listed in ``BASE`` and ``api_trade_available_flag`` true). On 2026-11-05 most
T-Capital funds were API-tradable only on ``SPBRU`` with an ``@`` ticker (``TMOS@``), while their
MOEX ``TQTF`` listings (``TMOS``) appeared only in ``ALL``; consumers that match MOEX symbols by
ticker or uid need those entries. ``base_tickers`` is the unique set of tickers with everything
from the first ``@`` and a trailing ``_OTC`` removed (``TMOS@`` -> ``TMOS``, ``TKVM_OTC`` ->
``TKVM``), the form a MOEX ``SECID`` takes. Matching is on ``asset.brand.company``: 165 listings
carry the current T-Capital name (Cyrillic) and 2 the former Tinkoff Capital name (2026-11-05).
Only read-only instrument methods are called (see the allowlist in ``adapters/tbank.py``).
"""

from __future__ import annotations

import asyncio
import re
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import orjson
import structlog

from tape_recorder.adapters.tbank import TBankAdapter, snake_keys
from tape_recorder.settings import ReferenceSettings
from tape_recorder.timeutil import Clock, ns_to_iso
from tape_recorder.transport import TRANSPORT_ERRORS, HttpFetcher

log = structlog.get_logger(__name__)

ZERO_FEE_FILE = "tbank_zero_fee_funds.json"
FUND_FIELDS = ("ticker", "figi", "instrument_uid", "class_code", "name", "lot", "currency")


ASSET_ATTEMPTS = 4
LISTING_INTERVAL_S = 5.0  # Etfs is a listing method: 15 calls per minute


class IncompleteReferenceError(RuntimeError):
    """Some fund assets stayed unresolved, so the reference file is not written."""


def retry_after_s(exc: BaseException) -> float | None:
    """Seconds to wait before retrying a rate-limited (429) or server-side (5xx) answer."""
    if not isinstance(exc, httpx.HTTPStatusError):
        return 2.0  # connection-level failure: one short pause
    status = exc.response.status_code
    if status == 429:
        try:
            return min(float(exc.response.headers.get("x-ratelimit-reset", "60")), 60.0) + 1.0
        except ValueError:
            return 61.0
    return 2.0 if status >= 500 else None


def dig(doc: dict[str, Any], path: str) -> Any:
    """``a.b.c`` lookup in nested dicts; ``None`` when any step is missing."""
    value: Any = doc
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def fund_record(etf: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": etf.get("ticker"),
        "figi": etf.get("figi"),
        "instrument_uid": etf.get("uid"),
        "class_code": etf.get("class_code"),
        "name": etf.get("name"),
        "lot": int(etf["lot"]) if etf.get("lot") is not None else None,
        "currency": etf.get("currency"),
        "isin": etf.get("isin"),
        "exchange": etf.get("exchange"),
        "real_exchange": etf.get("real_exchange"),
        "api_trade_available": etf.get("api_trade_available_flag"),
        "buy_available": etf.get("buy_available_flag"),
        "asset_uid": etf.get("asset_uid"),
    }


OTC_SUFFIX = "_OTC"


def base_ticker(ticker: str) -> str:
    """``TMOS@`` (SPB) -> ``TMOS``, ``TKVM_OTC`` (OTC) -> ``TKVM``; other tickers unchanged."""
    return ticker.split("@", 1)[0].removesuffix(OTC_SUFFIX)


@dataclass(slots=True)
class FundSelection:
    funds: list[dict[str, Any]] = field(default_factory=list)
    etfs: int = 0
    assets: int = 0
    brands: Counter[str] = field(default_factory=Counter)


def select_zero_fee(
    etfs: list[dict[str, Any]],
    assets: dict[str, dict[str, Any]],
    rule: ReferenceSettings,
    tradable_uids: frozenset[str] | None = None,
) -> FundSelection:
    """Listings whose fund asset matches the rule; ``etfs``/``assets`` are snake_case documents.

    ``tradable_uids`` are the instrument uids of ``INSTRUMENT_STATUS_BASE``; ``None`` treats every
    listing with ``api_trade_available_flag`` as tradable.
    """
    pattern = re.compile(rule.zero_fee_pattern, re.IGNORECASE)
    out = FundSelection(etfs=len(etfs), assets=len(assets))
    for etf in etfs:
        doc = {"etf": etf, "asset": assets.get(str(etf.get("asset_uid")), {})}
        out.brands[str(dig(doc, "asset.brand.company") or dig(doc, "asset.brand.name"))] += 1
        for path in rule.zero_fee_fields:
            value = dig(doc, path)
            if isinstance(value, str) and pattern.search(value):
                listed = tradable_uids is None or str(etf.get("uid")) in tradable_uids
                tradable = listed and etf.get("api_trade_available_flag") is True
                out.funds.append(
                    {
                        **fund_record(etf),
                        "api_tradable": tradable,
                        "matched_field": path,
                        "matched_value": value,
                    }
                )
                break
    out.funds.sort(key=lambda f: (str(f["ticker"]), str(f["class_code"])))
    return out


def reference_document(
    selection: FundSelection, rule: ReferenceSettings, now_ns: int
) -> dict[str, Any]:
    return {
        "source": "T-Bank Invest API: InstrumentsService/Etfs (instrumentStatus "
        "INSTRUMENT_STATUS_BASE and INSTRUMENT_STATUS_ALL, merged by uid) and "
        "InstrumentsService/GetAssetBy per fund asset_uid",
        "retrieved_at": ns_to_iso(now_ns),
        "rule": {
            "description": "a fund is selected when one of `fields` of its asset or ETF record "
            "matches `pattern` (case-insensitive regex); funds of T-Bank's own asset manager "
            "(T-Capital, formerly Tinkoff) trade without commission on the platform",
            "fields": list(rule.zero_fee_fields),
            "pattern": rule.zero_fee_pattern,
            "listings": "every listing (any board, BASE or ALL) of a matched fund is included; "
            "api_tradable = listed in INSTRUMENT_STATUS_BASE with api_trade_available_flag",
            "base_tickers": "unique tickers with everything from the first '@' and a trailing "
            "'_OTC' removed (TMOS@ -> TMOS, TKVM_OTC -> TKVM); ISIN-style tickers are kept",
        },
        "etfs_listed": selection.etfs,
        "assets_resolved": selection.assets,
        "count": len(selection.funds),
        "count_by_class_code": dict(
            sorted(Counter(str(f["class_code"]) for f in selection.funds).items())
        ),
        "base_tickers": sorted({base_ticker(str(f["ticker"])) for f in selection.funds}),
        "funds": selection.funds,
    }


async def build_zero_fee_reference(
    adapter: TBankAdapter,
    fetcher: HttpFetcher,
    rule: ReferenceSettings,
    clock: Clock,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> tuple[dict[str, Any], FundSelection]:
    """Fetch both fund lists and the assets (paced to the instruments limit) and select."""
    base = await _etfs(adapter, fetcher, "INSTRUMENT_STATUS_BASE")
    await sleep(LISTING_INTERVAL_S)
    every = await _etfs(adapter, fetcher, "INSTRUMENT_STATUS_ALL")
    by_uid = {str(e.get("uid")): e for e in every}
    by_uid.update({str(e.get("uid")): e for e in base})  # BASE wins: current trading flags
    etfs = list(by_uid.values())
    tradable = frozenset(str(e.get("uid")) for e in base)
    assets: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for uid in sorted({str(e["asset_uid"]) for e in etfs if e.get("asset_uid")}):
        body = None
        for attempt in range(ASSET_ATTEMPTS):
            await sleep(rule.min_request_interval_s)
            try:
                body = await fetcher(adapter.asset_request(uid))
                break
            except TRANSPORT_ERRORS as exc:
                wait = retry_after_s(exc)
                log.warning("asset_retry", asset_uid=uid, attempt=attempt + 1, error=repr(exc))
                if wait is None:
                    break
                await sleep(wait)
        if body is None:
            missing.append(uid)
            continue
        assets[uid] = snake_keys(orjson.loads(body)).get("asset") or {}
    if missing:
        # A fund without its asset cannot be classified; a partial list would silently drop funds.
        raise IncompleteReferenceError(f"{len(missing)} fund assets could not be fetched")
    selection = select_zero_fee(etfs, assets, rule, tradable)
    return reference_document(selection, rule, clock()), selection


async def _etfs(adapter: TBankAdapter, fetcher: HttpFetcher, status: str) -> list[dict[str, Any]]:
    doc = snake_keys(orjson.loads(await fetcher(adapter.etfs_request(status))))
    return list(doc.get("instruments", []))


def write_reference(path: Path, document: dict[str, Any]) -> None:
    """Write atomically, so a reader never sees half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_bytes(orjson.dumps(document, option=orjson.OPT_INDENT_2) + b"\n")
    tmp.replace(path)
