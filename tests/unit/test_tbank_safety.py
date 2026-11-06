"""The T-Bank token is for market data only: no order, sandbox, operations or account call."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pytest

from tape_recorder.adapters.tbank import ALLOWED_REST_METHODS, ForbiddenEndpointError, TBankAdapter
from tape_recorder.frames import RestRequest
from tape_recorder.history import backfill
from tape_recorder.settings import Settings
from tape_recorder.timeutil import NS_PER_S

SRC = Path(__file__).resolve().parents[2] / "src" / "tape_recorder"
FORBIDDEN = re.compile(
    r"(Orders|StopOrders|Sandbox|Operations|Users|Signal)Service|Post\w*Order|Cancel\w*Order"
)


def method_of(request: RestRequest) -> str:
    return request.url.rsplit(".", 1)[-1]


def adapter(settings: Settings) -> TBankAdapter:
    return TBankAdapter("moex-tbank", settings.venues["moex-tbank"])


def test_every_built_request_is_read_only(settings: Settings) -> None:
    a = adapter(settings)
    requests = [
        *a.resolve_requests(),
        a.candles_request("SBER", 0, NS_PER_S),
        a.last_trades_request("SBER", 0, NS_PER_S),
        a.orderbook_request("SBER"),
        a.etfs_request(),
        a.shares_request(),
        a.candles_request("SBER", 0, NS_PER_S, "CANDLE_INTERVAL_DAY"),
        a.asset_request("uid"),
    ]
    for r in requests:
        assert method_of(r) in ALLOWED_REST_METHODS
        assert not FORBIDDEN.search(r.url)
    assert not any(FORBIDDEN.search(m) for m in ALLOWED_REST_METHODS)
    assert a.ws_url().endswith("MarketDataStreamService/MarketDataStream")


@pytest.mark.parametrize(
    "method",
    [
        "OrdersService/PostOrder",
        "OrdersService/CancelOrder",
        "StopOrdersService/PostStopOrder",
        "SandboxService/PostSandboxOrder",
        "SandboxService/OpenSandboxAccount",
        "SandboxService/SandboxPayIn",
        "OperationsService/GetPortfolio",
        "UsersService/GetAccounts",
    ],
)
def test_order_and_sandbox_methods_are_refused(settings: Settings, method: str) -> None:
    with pytest.raises(ForbiddenEndpointError):
        adapter(settings)._rest(method, "x", "SBER", {})


@pytest.mark.parametrize("field", ["rest_url", "ws_url"])
def test_sandbox_hosts_and_other_streams_are_refused(settings: Settings, field: str) -> None:
    cfg = settings.venues["moex-tbank"]
    url = str(getattr(cfg, field)).replace("invest-public-api", "sandbox-invest-public-api")
    with pytest.raises(ForbiddenEndpointError):
        TBankAdapter("moex-tbank", cfg.model_copy(update={field: url}))
    other = str(cfg.ws_url).replace("MarketDataStream", "OrdersStream")
    with pytest.raises(ForbiddenEndpointError):
        TBankAdapter("moex-tbank", cfg.model_copy(update={"ws_url": other}))


def test_source_names_no_order_or_sandbox_service() -> None:
    hits = [
        str(p.relative_to(SRC))
        for p in SRC.rglob("*.py")
        if FORBIDDEN.search(p.read_text(encoding="utf-8"))
    ]
    assert hits == []


async def test_backfill_calls_only_allowed_methods(
    make_settings: Callable[..., Settings], tmp_path: Path
) -> None:
    s = make_settings(TBANK_TOKEN="test-token", TAPE_BACKFILL__MIN_REQUEST_INTERVAL_S="0")
    seen: list[str] = []

    async def fetcher(request: RestRequest) -> str:
        seen.append(method_of(request))
        return "{}"

    await backfill(
        s, "moex-tbank", date(2026, 11, 2), date(2026, 11, 3), tmp_path, fetcher, lambda: 1
    )
    assert seen and set(seen) <= ALLOWED_REST_METHODS
