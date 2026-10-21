"""Time helpers. Storage is int64 nanoseconds since the Unix epoch (UTC)."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

NS_PER_US = 1_000
NS_PER_MS = 1_000_000
NS_PER_S = 1_000_000_000
NS_PER_DAY = 86_400 * NS_PER_S

Clock = Callable[[], int]
"""A source of 'now' in ns; injected so tests and replays are deterministic."""


def wall_clock_ns() -> int:
    return time.time_ns()


def ms_to_ns(ms: int | float | str) -> int:
    return int(ms) * NS_PER_MS if isinstance(ms, (int, str)) else round(ms * NS_PER_MS)


def ns_to_datetime(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / NS_PER_S, tz=UTC)


def ns_to_iso(ns: int | None) -> str | None:
    if ns is None:
        return None
    seconds, rem = divmod(ns, NS_PER_S)
    base = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{rem:09d}Z"


def ns_to_date(ns: int) -> date:
    return ns_to_datetime(ns).date()


def date_bounds_ns(day: date) -> tuple[int, int]:
    """[start, end) of a UTC calendar day in ns."""
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return datetime_to_ns(start), datetime_to_ns(start + timedelta(days=1))


def datetime_to_ns(value: datetime) -> int:
    if value.tzinfo is None:
        raise ValueError("naive datetime; timezone-aware values are required")
    delta = value.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * NS_PER_S + delta.microseconds * NS_PER_US


def local_to_ns(text: str, fmt: str, tz: str) -> int:
    """Parse a venue-local wall-clock string (e.g. MOEX ``2026-10-21 10:00:01``) to UTC ns."""
    return datetime_to_ns(datetime.strptime(text, fmt).replace(tzinfo=ZoneInfo(tz)))


def window_bounds_ns(day: date, start_hhmm: str, end_hhmm: str, tz: str) -> tuple[int, int]:
    """A trading window on ``day`` expressed in venue-local ``HH:MM`` times, as UTC ns."""
    zone = ZoneInfo(tz)
    sh, sm = (int(x) for x in start_hhmm.split(":"))
    eh, em = (int(x) for x in end_hhmm.split(":"))
    start = datetime(day.year, day.month, day.day, sh, sm, tzinfo=zone)
    end = datetime(day.year, day.month, day.day, eh, em, tzinfo=zone)
    return datetime_to_ns(start), datetime_to_ns(end)


def local_date(ns: int, tz: str) -> date:
    return ns_to_datetime(ns).astimezone(ZoneInfo(tz)).date()
