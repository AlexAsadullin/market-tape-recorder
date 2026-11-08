"""Calendar rows from documented T-Invest answer shapes (camelCase JSON from the REST gateway)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from tape_recorder.adapters.tbank import ALLOWED_REST_METHODS
from tape_recorder.calendar_ref import dividend_rows, report_rows

MSK = ZoneInfo("Europe/Moscow")


def ns(*a: int) -> int:
    return int(datetime(*a, tzinfo=MSK).timestamp() * 1e9)


def test_dividends_are_known_the_day_after_declaration_and_cancelled_are_skipped() -> None:
    payload = {
        "dividends": [
            {
                "dividendNet": {"units": "34", "nano": 840000000, "currency": "rub"},
                "declaredDate": "2026-04-29T00:00:00Z",
                "lastBuyDate": "2026-07-17T00:00:00Z",
                "dividendType": "Regular Cash",
                "yieldValue": {"units": "11", "nano": 500000000},
                "createdAt": "2026-04-30T02:00:00Z",
            },
            {
                "dividendNet": {"units": "1"},
                "declaredDate": "2026-01-01T00:00:00Z",
                "lastBuyDate": "2026-02-01T00:00:00Z",
                "dividendType": "Cancelled",
            },
        ]
    }
    amt, row = dividend_rows("SBER", payload)
    assert amt["series_id"] == "cal.divamt.SBER.2026-07-17"
    assert abs(amt["value"] - 34.84) < 1e-9
    assert row["series_id"] == "cal.div.SBER.2026-07-17"
    assert row["value"] == 11.5
    assert row["announced_at"] == ns(2026, 4, 30)
    assert row["date"] == ns(2026, 4, 29)


def test_reports_known_at_record_creation() -> None:
    payload = {
        "events": [
            {
                "instrumentId": "u",
                "reportDate": "2026-10-30T00:00:00Z",
                "periodYear": 2026,
                "periodNum": 3,
                "periodType": "PERIOD_TYPE_QUARTER",
                "createdAt": "2026-08-01T10:00:00Z",
            }
        ]
    }
    (row,) = report_rows("SBER", payload)
    assert row["series_id"] == "cal.report.SBER.2026-10-30"
    assert row["value"] == 1.0
    assert row["announced_at"] == int(
        datetime(2026, 8, 1, 10, tzinfo=ZoneInfo("UTC")).timestamp() * 1e9
    )


def test_calendar_methods_are_read_only_instrument_calls() -> None:
    assert {
        "InstrumentsService/GetDividends",
        "InstrumentsService/GetAssetReports",
        "InstrumentsService/GetInstrumentBy",
    } <= ALLOWED_REST_METHODS
