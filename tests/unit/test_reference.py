from __future__ import annotations

from pathlib import Path

import httpx
import orjson
import pytest

from tape_recorder.adapters.tbank import ALLOWED_REST_METHODS, TBankAdapter
from tape_recorder.frames import RestRequest
from tape_recorder.reference import (
    IncompleteReferenceError,
    base_ticker,
    build_zero_fee_reference,
    dig,
    write_reference,
)
from tape_recorder.settings import Settings

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "reference" / "tbank_etfs.json"


async def test_zero_fee_funds_from_fixture(settings: Settings, tmp_path: Path) -> None:
    data = orjson.loads(FIXTURE.read_bytes())
    calls: list[RestRequest] = []
    pauses: list[float] = []

    async def fetcher(request: RestRequest) -> str:
        calls.append(request)
        if request.tag == "etfs":
            status = orjson.loads(request.body or "{}")["instrumentStatus"]
            return orjson.dumps(data["etfs" if status.endswith("BASE") else "etfs_all"]).decode()
        uid = orjson.loads(request.body or "{}")["id"]
        return orjson.dumps(data["assets"][uid]).decode()

    async def sleep(seconds: float) -> None:
        pauses.append(seconds)

    adapter = TBankAdapter("moex-tbank", settings.venues["moex-tbank"])
    doc, selection = await build_zero_fee_reference(
        adapter, fetcher, settings.reference, lambda: 1_793_880_000_000_000_000, sleep
    )
    assert {r.url.rsplit(".", 1)[-1] for r in calls} <= ALLOWED_REST_METHODS
    assert len(calls) == 5
    assert pauses == [5.0] + [settings.reference.min_request_interval_s] * 3
    funds = {(f["ticker"], f["class_code"]): f for f in doc["funds"]}
    # Every listing of a matched fund, including the MOEX listing that is not API-tradable.
    assert set(funds) == {("TFND1", "TQTF"), ("TFND1@", "SPBRU"), ("TFND2", "TQTF")}
    moex = funds[("TFND1", "TQTF")]
    assert moex["api_tradable"] is False and moex["instrument_uid"] == "uid-5"
    spb = funds[("TFND1@", "SPBRU")]
    assert spb["api_tradable"] is True and spb["figi"] == "FIGI00000001"
    assert spb["lot"] == 1 and spb["currency"] == "rub" and spb["name"] == "Fund one"
    assert funds[("TFND2", "TQTF")]["api_tradable"] is True  # BASE flags win over ALL
    assert spb["matched_field"] == "asset.brand.company"
    assert funds[("TFND2", "TQTF")]["matched_field"] == "asset.brand.name"
    assert doc["base_tickers"] == ["TFND1", "TFND2"]
    assert doc["count_by_class_code"] == {"SPBRU": 1, "TQTF": 2}
    assert doc["rule"]["pattern"] == settings.reference.zero_fee_pattern
    assert doc["retrieved_at"].startswith("2026-11-05") and doc["count"] == 3
    assert selection.etfs == 5 and selection.assets == 3
    out = tmp_path / "ref" / "funds.json"
    write_reference(out, doc)
    assert orjson.loads(out.read_bytes())["count"] == 3


def test_base_ticker() -> None:
    assert [base_ticker(t) for t in ("TMOS@", "TKVM_OTC", "TPAY", "RU000A1071F0")] == [
        "TMOS",
        "TKVM",
        "TPAY",
        "RU000A1071F0",
    ]


def test_dig() -> None:
    assert dig({"a": {"b": "x"}}, "a.b") == "x"
    assert dig({"a": "x"}, "a.b") is None


async def test_rate_limit_is_retried_and_partial_lists_are_refused(settings: Settings) -> None:
    data = orjson.loads(FIXTURE.read_bytes())
    adapter = TBankAdapter("moex-tbank", settings.venues["moex-tbank"])
    attempts: dict[str, int] = {}
    pauses: list[float] = []

    def error(status: int, request: RestRequest) -> httpx.HTTPStatusError:
        req = httpx.Request("POST", request.url)
        resp = httpx.Response(status, headers={"x-ratelimit-reset": "7"}, request=req)
        return httpx.HTTPStatusError("x", request=req, response=resp)

    async def fetcher(request: RestRequest) -> str:
        if request.tag == "etfs":
            status = orjson.loads(request.body or "{}")["instrumentStatus"]
            return orjson.dumps(data["etfs" if status.endswith("BASE") else "etfs_all"]).decode()
        uid = orjson.loads(request.body or "{}")["id"]
        attempts[uid] = attempts.get(uid, 0) + 1
        if uid == "asset-1" and attempts[uid] == 1:
            raise error(429, request)
        if uid == "asset-3" and fail_asset_3:
            raise error(400, request)
        return orjson.dumps(data["assets"][uid]).decode()

    async def sleep(seconds: float) -> None:
        pauses.append(seconds)

    fail_asset_3 = False
    doc, _ = await build_zero_fee_reference(adapter, fetcher, settings.reference, lambda: 1, sleep)
    assert doc["count"] == 3 and attempts["asset-1"] == 2 and 8.0 in pauses
    fail_asset_3 = True
    with pytest.raises(IncompleteReferenceError):
        await build_zero_fee_reference(adapter, fetcher, settings.reference, lambda: 1, sleep)
