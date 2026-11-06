"""``tape download``: bulk candle history for the whole universe into the normal tape.

For every instrument of ``reference/universe_moex.json`` the downloader fetches day candles for
the full available history or 1-minute candles for the last ``download.minute_days`` days from
T-Bank ``GetCandles`` and writes them through the T-Bank adapter and the pipeline, so the rows
are identical in shape to live and ``tape backfill`` rows (contract 6.2; ``ts_local`` is the
fetch time, so they land in the partition of the download date and I2's follower ingests them).

* Range splitting: one request covers at most the documented maximum per interval (1 day for
  1-minute candles, aligned to Moscow midnight; 6 years for day candles). 1-minute requests are
  made only for the days on which the instrument has a day candle in the tape (plus today), so
  run ``--interval day`` first; without day candles every calendar day is requested.
* Pacing: one limiter for all workers, at ``rate_fraction`` of the limit reported in
  ``x-ratelimit-limit``; when ``x-ratelimit-remaining`` runs low it waits for
  ``x-ratelimit-reset``. HTTP 429 or a gRPC ``RESOURCE_EXHAUSTED`` answer waits for the reset;
  5xx and transport failures back off exponentially; other 4xx answers are final for that chunk.
* Resume: a checkpoint per venue, symbol and interval records the ranges done (and those that
  came back empty). A range is recorded only after its parts were finalised in the tape, and
  bars that may still change (the current and the previous bar) are never recorded, so a rerun
  fetches exactly what is missing.
* Idempotence: the deduplicator of each instrument is seeded with the bars already in the tape
  (same content key as the adapter's), so a refetched range adds no duplicate rows even after a
  crash between finalising a part and saving its checkpoint. A bar whose content changed is a
  new revision and is kept.
* Priority: the CLI adds ``download.nice`` to the process niceness, and no new request starts
  while the recorder's ``/v1/load`` reports ``pressure: high``.
* Fallback: public ISS (venue ``moex-iss``) only for what T-Bank lacks: day candles before
  T-Bank's first day candle, and 1-minute days before T-Bank's first minute candle or for which
  T-Bank returned nothing although the instrument traded that day.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import orjson
import pyarrow.parquet as pq
import structlog

from tape_recorder.adapters.moex import MoexIssAdapter
from tape_recorder.adapters.tbank import TBankAdapter
from tape_recorder.dedup import Deduplicator
from tape_recorder.frames import Frame, RestRequest
from tape_recorder.pipeline import Pipeline
from tape_recorder.schema import Channel
from tape_recorder.settings import DownloadSettings, Settings, SymbolSettings
from tape_recorder.timeutil import NS_PER_DAY, NS_PER_S, Clock, datetime_to_ns, ns_to_iso
from tape_recorder.transport import TRANSPORT_ERRORS
from tape_recorder.writer import TapeWriter, atomic_write_bytes

log = structlog.get_logger(__name__)

TZ = "Europe/Moscow"
TBANK = "moex-tbank"
ISS = "moex-iss"
INTERVALS: dict[str, tuple[str, str, int]] = {
    # tape label -> (T-Bank interval, ISS interval, bar length in ns)
    "1d": ("CANDLE_INTERVAL_DAY", "24", NS_PER_DAY),
    "1m": ("CANDLE_INTERVAL_1_MIN", "1", 60 * NS_PER_S),
}
Interval = tuple[int, int]  # inclusive [lo, hi] in ns


# -- ranges ------------------------------------------------------------------------------------


def merge(intervals: Iterable[Interval]) -> list[Interval]:
    """Union of inclusive intervals; adjacent ones (hi + 1 == lo) are joined."""
    out: list[Interval] = []
    for lo, hi in sorted(intervals):
        if out and lo <= out[-1][1] + 1:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def subtract(lo: int, hi: int, done: Iterable[Interval]) -> list[Interval]:
    """Parts of ``[lo, hi]`` not covered by ``done``."""
    out: list[Interval] = []
    cursor = lo
    for d_lo, d_hi in merge(done):
        if d_hi < cursor:
            continue
        if d_lo > hi:
            break
        if d_lo > cursor:
            out.append((cursor, d_lo - 1))
        cursor = max(cursor, d_hi + 1)
        if cursor > hi:
            return out
    if cursor <= hi:
        out.append((cursor, hi))
    return out


def split_span(lo: int, hi: int, span_ns: int) -> list[Interval]:
    """``[lo, hi]`` in consecutive pieces of at most ``span_ns``."""
    out = []
    while lo <= hi:
        end = min(lo + span_ns - 1, hi)
        out.append((lo, end))
        lo = end + 1
    return out


def day_window(day: date, tz: str = TZ) -> Interval:
    start = datetime(day.year, day.month, day.day, tzinfo=ZoneInfo(tz))
    lo = datetime_to_ns(start)
    return lo, datetime_to_ns(start + timedelta(days=1)) - 1


def local_day(ns: int, tz: str = TZ) -> date:
    return datetime.fromtimestamp(ns / NS_PER_S, ZoneInfo(tz)).date()


def minute_chunks(missing: list[Interval], days: Iterable[date], tz: str = TZ) -> list[Interval]:
    """One request per local day (the 1-minute maximum), only on ``days``, only what is missing."""
    out: list[Interval] = []
    for day in sorted(set(days)):
        d_lo, d_hi = day_window(day, tz)
        for m_lo, m_hi in missing:
            lo, hi = max(d_lo, m_lo), min(d_hi, m_hi)
            if lo <= hi:
                out.append((lo, hi))
    return out


def daily_chunks(missing: list[Interval], span_days: int) -> list[Interval]:
    return [c for lo, hi in missing for c in split_span(lo, hi, span_days * NS_PER_DAY)]


def stable_until(now_ns: int, bar_ns: int) -> int:
    """Last ns whose bars are final: the current and the previous bar may still change."""
    return (now_ns // bar_ns) * bar_ns - bar_ns - 1


# -- checkpoint --------------------------------------------------------------------------------


@dataclass(slots=True)
class Checkpoint:
    """Ranges done (fetched and committed to the tape) and ranges that came back empty."""

    path: Path
    done: list[Interval] = field(default_factory=list)
    empty: list[Interval] = field(default_factory=list)
    rows: int = 0

    @classmethod
    def load(cls, path: Path) -> Checkpoint:
        if not path.exists():
            return cls(path)
        doc = orjson.loads(path.read_bytes())
        return cls(
            path,
            [(int(a), int(b)) for a, b in doc.get("done", [])],
            [(int(a), int(b)) for a, b in doc.get("empty", [])],
            int(doc.get("rows", 0)),
        )

    def add(self, lo: int, hi: int, parsed: int, rows: int) -> None:
        if lo > hi:
            return
        self.done = merge([*self.done, (lo, hi)])
        kept = [p for e_lo, e_hi in self.empty for p in _minus(e_lo, e_hi, lo, hi)]
        self.empty = merge([*kept, (lo, hi)] if parsed == 0 else kept)
        self.rows += rows

    def missing(self, lo: int, hi: int) -> list[Interval]:
        return subtract(lo, hi, self.done)

    def save(self) -> None:
        doc = {
            "done": [list(i) for i in self.done],
            "empty": [list(i) for i in self.empty],
            "rows": self.rows,
            "done_iso": [[ns_to_iso(a), ns_to_iso(b)] for a, b in self.done],
        }
        atomic_write_bytes(self.path, orjson.dumps(doc, option=orjson.OPT_INDENT_2))


def _minus(lo: int, hi: int, c_lo: int, c_hi: int) -> list[Interval]:
    return subtract(lo, hi, [(c_lo, c_hi)])


def checkpoint_path(root: Path, venue: str, symbol: str, interval: str) -> Path:
    return root / f"venue={venue}" / f"symbol={symbol}" / f"interval={interval}.json"


# -- HTTP: pacing, backoff, recorder pressure --------------------------------------------------


@dataclass(frozen=True, slots=True)
class HttpReply:
    status: int
    headers: Mapping[str, str]
    text: str


Send = Callable[[RestRequest], Awaitable[HttpReply]]
Sleep = Callable[[float], Awaitable[None]]


def httpx_send(client: httpx.AsyncClient) -> Send:
    async def send(request: RestRequest) -> HttpReply:
        headers = dict(request.headers)
        if request.method == "POST":
            headers.setdefault("Content-Type", "application/json")
            resp = await client.post(
                request.url,
                params=list(request.params),
                headers=headers,
                content=(request.body or "{}").encode(),
            )
        else:
            resp = await client.get(request.url, params=list(request.params), headers=headers)
        headers_lc = {k.lower(): v for k, v in resp.headers.items()}
        return HttpReply(resp.status_code, headers_lc, resp.text)

    return send


def header_int(headers: Mapping[str, str], name: str) -> int | None:
    """``x-ratelimit-limit: 600, 600;w=60`` -> 600; missing or unreadable -> None."""
    value = headers.get(name)
    if value is None:
        return None
    try:
        return int(value.split(",", 1)[0].split(";", 1)[0].strip())
    except ValueError:
        return None


class RateLimiter:
    """Shared request slots at ``fraction`` of the reported per-minute limit (see module doc)."""

    def __init__(
        self,
        fraction: float,
        default_limit_per_min: int,
        sleep: Sleep,
        monotonic: Callable[[], float] = time.monotonic,
        window_s: float = 60.0,
    ) -> None:
        self.fraction = fraction
        self.limit = default_limit_per_min
        self.window_s = window_s
        self.remaining: int | None = None
        self.reset_s: float = window_s
        self._sleep = sleep
        self._mono = monotonic
        self._next = 0.0
        self.waits = 0

    @property
    def interval_s(self) -> float:
        return self.window_s / (self.limit * self.fraction)

    @property
    def reserve(self) -> int:
        return max(2, math.ceil(self.limit * 0.05))

    async def acquire(self) -> None:
        now = self._mono()
        if self.remaining is not None and self.remaining <= self.reserve:
            # The window is almost used up (by us or by the recorder on the same token).
            self._next = max(self._next, now + self.reset_s + 1.0)
            self.remaining = None
            self.waits += 1
        slot = max(now, self._next)
        self._next = slot + self.interval_s
        if slot > now:
            await self._sleep(slot - now)

    def observe(self, headers: Mapping[str, str]) -> None:
        limit = header_int(headers, "x-ratelimit-limit")
        if limit:
            self.limit = limit
        self.remaining = header_int(headers, "x-ratelimit-remaining")
        reset = header_int(headers, "x-ratelimit-reset")
        if reset is not None:
            self.reset_s = float(reset)

    def throttled(self) -> float:
        """Wait after a 429: until the window resets, and no slot is handed out before."""
        wait = self.reset_s + 1.0
        self._next = max(self._next, self._mono() + wait)
        return wait


class FetchError(RuntimeError):
    def __init__(self, status: int | None, detail: str) -> None:
        super().__init__(f"HTTP {status}: {detail}" if status else detail)
        self.status = status


class StoppedError(RuntimeError):
    """The run was asked to stop (Ctrl-C) while a request waited."""


def is_throttled(reply: HttpReply) -> bool:
    if reply.status == 429:
        return True
    head = reply.text[:400]
    return "RESOURCE_EXHAUSTED" in head or ('"code":8' in head.replace(" ", ""))


def backoff_s(attempt: int, initial: float, cap: float) -> float:
    return float(min(cap, initial * (2.0**attempt)))


class PressureGate:
    """Blocks new requests while the recorder reports ``pressure: high`` on ``/v1/load``."""

    def __init__(
        self,
        probe: Callable[[], Awaitable[bool]] | None,
        probe_interval_s: float,
        yield_sleep_s: float,
        sleep: Sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._probe = probe
        self._interval = probe_interval_s
        self._yield = yield_sleep_s
        self._sleep = sleep
        self._mono = monotonic
        self._checked = -math.inf
        self._high = False
        self._lock = asyncio.Lock()
        self.yields = 0

    async def wait(self, stopped: Callable[[], bool] = lambda: False) -> None:
        if self._probe is None:
            return
        while not stopped():
            async with self._lock:
                if self._mono() - self._checked >= self._interval:
                    self._high = await self._probe()
                    self._checked = self._mono()
            if not self._high:
                return
            self.yields += 1
            log.info("download_yielding", reason="recorder pressure high", sleep_s=self._yield)
            await self._sleep(self._yield)
            self._checked = -math.inf


def http_load_probe(client: httpx.AsyncClient, url: str) -> Callable[[], Awaitable[bool]]:
    async def probe() -> bool:
        try:
            resp = await client.get(url, timeout=2.0)
            return bool(resp.status_code == 200 and resp.json().get("pressure") == "high")
        except (httpx.HTTPError, ValueError):
            return False  # no recorder running: nothing to yield to

    return probe


@dataclass(slots=True)
class FetchStats:
    requests: int = 0
    retries: int = 0
    throttled: int = 0
    failures: int = 0


class LimitedFetcher:
    """``HttpFetcher`` with the shared limiter, retries and the pressure gate."""

    def __init__(
        self,
        send: Send,
        limiter: RateLimiter,
        settings: DownloadSettings,
        sleep: Sleep,
        gate: PressureGate | None = None,
        stopped: Callable[[], bool] = lambda: False,
    ) -> None:
        self._send = send
        self.limiter = limiter
        self._settings = settings
        self._sleep = sleep
        self._gate = gate
        self._stopped = stopped
        self.stats = FetchStats()

    async def __call__(self, request: RestRequest) -> str:
        s = self._settings
        last = "no attempt"
        for attempt in range(s.max_attempts):
            if self._stopped():
                raise StoppedError("stopped")
            if self._gate is not None:
                await self._gate.wait(self._stopped)
            await self.limiter.acquire()
            if self._stopped():
                raise StoppedError("stopped")
            try:
                reply = await self._send(request)
            except TRANSPORT_ERRORS as exc:
                last = repr(exc)
                delay = backoff_s(attempt, s.backoff_initial_s, s.backoff_max_s)
            else:
                self.limiter.observe(reply.headers)
                if 200 <= reply.status < 300:
                    self.stats.requests += 1
                    return reply.text
                last = f"HTTP {reply.status}: {reply.text[:200]}"
                if is_throttled(reply):
                    self.stats.throttled += 1
                    delay = self.limiter.throttled()
                elif reply.status >= 500:
                    delay = backoff_s(attempt, s.backoff_initial_s, s.backoff_max_s)
                else:
                    self.stats.failures += 1
                    raise FetchError(reply.status, reply.text[:300])
            self.stats.retries += 1
            log.warning(
                "download_retry", tag=request.tag, attempt=attempt + 1, delay_s=delay, error=last
            )
            await self._sleep(delay)
        self.stats.failures += 1
        raise FetchError(None, f"gave up after {s.max_attempts} attempts: {last}")


# -- tape index --------------------------------------------------------------------------------


def candle_files(tape_root: Path, venue: str, symbol: str) -> list[Path]:
    base = tape_root / f"venue={venue}" / f"channel={Channel.CANDLES}" / f"symbol={symbol}"
    return sorted(base.glob("date=*/part-*.parquet"))


def tape_bars(
    tape_root: Path, venue: str, symbol: str, interval: str
) -> list[tuple[int, float, float]]:
    """``(ts_open, close, volume)`` of every candle row of one interval in the tape."""
    out: list[tuple[int, float, float]] = []
    for path in candle_files(tape_root, venue, symbol):
        table = pq.read_table(
            path, columns=["ts_open", "close", "volume"], filters=[("interval", "==", interval)]
        )
        cols = [table.column(n).to_pylist() for n in ("ts_open", "close", "volume")]
        out.extend(zip(*cols, strict=True))
    return out


def bar_days(tape_root: Path, symbol: str, interval: str, venues: Iterable[str]) -> set[date]:
    return {local_day(b[0]) for v in venues for b in tape_bars(tape_root, v, symbol, interval)}


# -- planning ----------------------------------------------------------------------------------


@dataclass(slots=True)
class Task:
    entry: dict[str, Any]
    venue: str
    interval: str
    chunks: list[Interval]
    checkpoint: Checkpoint

    @property
    def symbol(self) -> str:
        return str(self.entry["symbol"])


@dataclass(frozen=True, slots=True)
class Window:
    lo: int
    hi: int


def parse_ts(text: str | None) -> int | None:
    if not text:
        return None
    return datetime_to_ns(datetime.fromisoformat(text.replace("Z", "+00:00")))


def floor_day(ns: int, tz: str = TZ) -> int:
    return day_window(local_day(ns, tz), tz)[0]


def requested_window(
    interval: str,
    settings: DownloadSettings,
    now_ns: int,
    since: date | None,
    until: date | None,
) -> Window:
    if interval == "1d":
        start = since or date.fromisoformat(settings.history_start)
    else:
        start = since or (local_day(now_ns) - timedelta(days=settings.minute_days))
    hi = min(now_ns, day_window(until)[1]) if until else now_ns
    return Window(day_window(start)[0], hi)


def tbank_first(entry: Mapping[str, Any], interval: str) -> int | None:
    key = "first_1day_candle_date" if interval == "1d" else "first_1min_candle_date"
    first = parse_ts(entry.get(key))
    return None if first is None else floor_day(first)


def plan_tbank(
    entry: dict[str, Any],
    interval: str,
    window: Window,
    checkpoint: Checkpoint,
    settings: DownloadSettings,
    trading_days: set[date] | None,
    now_ns: int,
) -> list[Interval]:
    first = tbank_first(entry, interval)
    if first is None:
        return []
    lo = max(window.lo, first)
    if lo > window.hi:
        return []
    missing = checkpoint.missing(lo, window.hi)
    if interval == "1d":
        return daily_chunks(missing, settings.daily_span_days)
    first_day, last_day = local_day(lo), local_day(window.hi)
    if trading_days:
        days = {d for d in trading_days if first_day <= d <= last_day}
        today = local_day(now_ns)
        if first_day <= today <= last_day:
            days.add(today)
    else:
        days = {first_day + timedelta(days=i) for i in range((last_day - first_day).days + 1)}
    return minute_chunks(missing, days)


def plan_iss(
    entry: dict[str, Any],
    interval: str,
    window: Window,
    tbank_cp: Checkpoint,
    iss_cp: Checkpoint,
    settings: DownloadSettings,
    trading_days: set[date],
    now_ns: int,
) -> list[Interval]:
    """What T-Bank lacks: before its first candle, and ranges it answered empty."""
    first = tbank_first(entry, interval)
    lacking: list[Interval] = []
    if first is None:
        lacking.append((window.lo, window.hi))
    elif first > window.lo:
        lacking.append((window.lo, min(first - 1, window.hi)))
    lacking.extend(tbank_cp.empty)
    lacking = merge((max(a, window.lo), min(b, window.hi)) for a, b in lacking)
    lacking = [(a, b) for a, b in lacking if a <= b]
    missing = [m for a, b in lacking for m in iss_cp.missing(a, b)]
    if interval == "1d":
        return daily_chunks(missing, settings.daily_span_days)
    stable = local_day(stable_until(now_ns, NS_PER_DAY))
    return minute_chunks(missing, {d for d in trading_days if d <= stable})


# -- execution ---------------------------------------------------------------------------------


@dataclass(slots=True)
class Progress:
    total_chunks: int
    started: float
    monotonic: Callable[[], float] = time.monotonic
    done_chunks: int = 0
    rows: int = 0

    def line(self, requests: int) -> dict[str, Any]:
        elapsed = max(self.monotonic() - self.started, 1e-9)
        rate = self.done_chunks / elapsed
        left = self.total_chunks - self.done_chunks
        eta = left / rate if rate > 0 else None
        return {
            "chunks": f"{self.done_chunks}/{self.total_chunks}",
            "pct": round(100.0 * self.done_chunks / max(self.total_chunks, 1), 1),
            "rows": self.rows,
            "requests": requests,
            "req_per_min": round(60.0 * requests / elapsed, 1),
            "eta": None if eta is None else str(timedelta(seconds=round(eta))),
        }


@dataclass(slots=True)
class DownloadSummary:
    interval: str
    tasks: int = 0
    chunks_total: int = 0
    chunks_done: int = 0
    rows: int = 0
    failed_chunks: int = 0
    stopped: bool = False
    rows_by_symbol: Counter[str] = field(default_factory=Counter)
    rows_by_venue: Counter[str] = field(default_factory=Counter)
    fetch: dict[str, FetchStats] = field(default_factory=dict)
    yields: int = 0


def symbol_settings(entry: Mapping[str, Any], iss: bool) -> SymbolSettings:
    return SymbolSettings(
        symbol=str(entry["symbol"]),
        contract_size=float(entry.get("lot") or 1),
        tick=entry.get("min_price_increment") or None,
        board=str(entry["class_code"]),
        engine="stock" if iss else None,
        market="shares" if iss else None,
        instrument_id=None if iss else entry.get("instrument_uid"),
        instrument_uid=None if iss else entry.get("instrument_uid"),
    )


def build_adapters(
    settings: Settings, entries: list[dict[str, Any]]
) -> tuple[TBankAdapter, MoexIssAdapter]:
    tickers = Counter(str(e["ticker"]) for e in entries)
    clash = [t for t, n in tickers.items() if n > 1]
    if clash:
        raise ValueError(f"tickers on more than one board are not supported: {clash[:5]}")
    tb = settings.venues[TBANK].model_copy(
        update={"symbols": {e["ticker"]: symbol_settings(e, False) for e in entries}}
    )
    iss = settings.venues[ISS].model_copy(
        update={
            "symbols": {e["ticker"]: symbol_settings(e, True) for e in entries},
            "channels": [Channel.CANDLES],
        }
    )
    return TBankAdapter(TBANK, tb), MoexIssAdapter(ISS, iss)


class Downloader:
    """Runs planned tasks with ``workers`` concurrent requests (see module doc)."""

    def __init__(
        self,
        settings: Settings,
        entries: list[dict[str, Any]],
        out_root: Path,
        fetchers: Mapping[str, LimitedFetcher],
        clock: Clock,
        stop: asyncio.Event,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.cfg = settings.download
        self.entries = entries
        self.out_root = out_root
        self.fetchers = fetchers
        self.clock = clock
        self.stop = stop
        self.mono = monotonic
        self.tbank, self.iss = build_adapters(settings, entries)
        self.cp_root = settings.project_root / self.cfg.dir / "checkpoints"

    def checkpoint(self, venue: str, symbol: str, interval: str) -> Checkpoint:
        return Checkpoint.load(checkpoint_path(self.cp_root, venue, symbol, interval))

    def trading_days(self, symbol: str) -> set[date]:
        return bar_days(self.out_root, symbol, "1d", (TBANK, ISS))

    def plan(
        self, interval: str, since: date | None, until: date | None, venue: str = TBANK
    ) -> list[Task]:
        now = self.clock()
        window = requested_window(interval, self.cfg, now, since, until)
        tasks = []
        for e in self.entries:
            sym = str(e["symbol"])
            days = self.trading_days(sym) if interval == "1m" else set()
            if venue == TBANK:
                cp = self.checkpoint(TBANK, sym, interval)
                chunks = plan_tbank(e, interval, window, cp, self.cfg, days or None, now)
            else:
                cp = self.checkpoint(ISS, sym, interval)
                tb = self.checkpoint(TBANK, sym, interval)
                chunks = plan_iss(e, interval, window, tb, cp, self.cfg, days, now)
            if chunks:
                tasks.append(Task(e, venue, interval, chunks, cp))
        return tasks

    def requests_for(self, task: Task, lo: int, hi: int, start: int = 0) -> RestRequest:
        native = str(task.entry["ticker"])
        tb_interval, iss_interval, _ = INTERVALS[task.interval]
        if task.venue == TBANK:
            return self.tbank.candles_request(native, lo, hi, tb_interval)
        fmt = "%Y-%m-%d %H:%M:%S"
        tz = ZoneInfo(TZ)
        return self.iss.history_request(
            "candles",
            native,
            local_day(lo),
            interval=iss_interval,
            **{
                "from": datetime.fromtimestamp(lo / NS_PER_S, tz).strftime(fmt),
                "till": datetime.fromtimestamp(hi // NS_PER_S, tz).strftime(fmt),
                "start": str(start),
            },
        )

    def _seeded_dedup(self, task: Task) -> Deduplicator:
        dedup = Deduplicator(window_s=1e12, max_keys=10**9)
        for ts_open, close, volume in tape_bars(
            self.out_root, task.venue, task.symbol, task.interval
        ):
            dedup.first_arrival(
                (task.venue, ("candle", task.symbol, task.interval, ts_open, close, volume)), 0
            )
        return dedup

    async def _run_task(self, task: Task, progress: Progress, summary: DownloadSummary) -> None:
        adapter = self.tbank if task.venue == TBANK else self.iss
        fetcher = self.fetchers[task.venue]
        s = self.settings
        writer = TapeWriter(
            self.out_root,
            s.data_path(s.paths.staging),
            s.writer,
            self.clock,
            index_gap=self.cfg.part_index_gap,
        )
        pipeline = Pipeline({task.venue: adapter}, s.region, writer, self._seeded_dedup(task))
        bar_ns = INTERVALS[task.interval][2]
        page_rows = s.backfill.iss_page_rows
        pending: list[tuple[int, int, int, int]] = []
        last_commit = self.mono()

        def commit() -> None:
            writer.close()
            for lo, hi, parsed, rows in pending:
                task.checkpoint.add(lo, hi, parsed, rows)
            if pending:
                task.checkpoint.save()
            pending.clear()

        try:
            for lo, hi in task.chunks:
                if self.stop.is_set():
                    break
                parsed = rows = 0
                offset = 0
                try:
                    while True:
                        request = self.requests_for(task, lo, hi, offset)
                        body = await fetcher(request)
                        events = pipeline.process(
                            Frame(
                                task.venue,
                                self.clock(),
                                f"download-{task.venue}",
                                "rest",
                                body,
                                request,
                            )
                        )
                        n = pipeline.last_parsed
                        parsed += n
                        rows += sum(len(e.records) for e in events)
                        if task.venue == TBANK or n < page_rows:
                            break
                        offset += n
                except StoppedError:
                    break
                except FetchError as exc:
                    summary.failed_chunks += 1
                    log.warning(
                        "download_chunk_failed",
                        venue=task.venue,
                        symbol=task.symbol,
                        interval=task.interval,
                        start=ns_to_iso(lo),
                        error=str(exc),
                    )
                    progress.done_chunks += 1
                    continue
                stable = stable_until(self.clock(), bar_ns)
                pending.append((lo, min(hi, stable), parsed, rows))
                summary.rows += rows
                summary.rows_by_symbol[task.symbol] += rows
                summary.rows_by_venue[task.venue] += rows
                summary.chunks_done += 1
                progress.done_chunks += 1
                progress.rows += rows
                if self.mono() - last_commit >= self.cfg.commit_interval_s:
                    commit()
                    last_commit = self.mono()
        finally:
            commit()

    async def run(self, tasks: list[Task], interval: str) -> DownloadSummary:
        summary = DownloadSummary(interval, tasks=len(tasks))
        summary.chunks_total = sum(len(t.chunks) for t in tasks)
        progress = Progress(summary.chunks_total, self.mono(), self.mono)
        queue: asyncio.Queue[Task] = asyncio.Queue()
        for t in tasks:
            queue.put_nowait(t)

        async def worker() -> None:
            while not self.stop.is_set():
                try:
                    task = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                await self._run_task(task, progress, summary)

        def requests() -> int:
            return sum(f.stats.requests for f in self.fetchers.values())

        async def reporter() -> None:
            while True:
                await asyncio.sleep(self.cfg.progress_interval_s)
                log.info("download_progress", interval=interval, **progress.line(requests()))

        report = asyncio.create_task(reporter())
        try:
            n = max(1, min(self.cfg.workers, len(tasks)))
            if tasks and tasks[0].venue == ISS:
                n = 1  # public ISS: one request at a time
            await asyncio.gather(*(worker() for _ in range(n)))
        finally:
            report.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await report
        log.info("download_done", interval=interval, **progress.line(requests()))
        summary.stopped = self.stop.is_set()
        summary.fetch = {v: f.stats for v, f in self.fetchers.items()}
        return summary


# -- quality report ----------------------------------------------------------------------------


def trading_calendar(daily: Mapping[str, set[date]], threshold: float) -> set[date]:
    """Days on which at least ``threshold`` of the names listed then (first..last bar) traded."""
    spans = {s: (min(d), max(d)) for s, d in daily.items() if d}
    counts: Counter[date] = Counter(d for days in daily.values() for d in days)
    out = set()
    for day, n in counts.items():
        alive = sum(1 for lo, hi in spans.values() if lo <= day <= hi)
        if alive and n >= threshold * alive:
            out.add(day)
    return out


def quality_report(
    tape_root: Path,
    entries: list[dict[str, Any]],
    interval: str,
    window: Window,
    threshold: float,
    now_ns: int,
) -> dict[str, Any]:
    """Rows per instrument and missing trading days against the calendar of the day candles."""
    daily = {
        str(e["symbol"]): bar_days(tape_root, str(e["symbol"]), "1d", (TBANK, ISS)) for e in entries
    }
    calendar = trading_calendar(daily, threshold)
    last_stable = local_day(stable_until(now_ns, NS_PER_DAY))
    w_lo, w_hi = local_day(window.lo), min(local_day(window.hi), last_stable)
    rows_out = []
    for e in entries:
        sym = str(e["symbol"])
        by_venue = {}
        bars: set[int] = set()
        for venue in (TBANK, ISS):
            got = tape_bars(tape_root, venue, sym, interval)
            by_venue[venue] = len(got)
            bars.update(b[0] for b in got)
        days = {local_day(b) for b in bars}
        if interval == "1d":
            lo = max(w_lo, min(days)) if days else w_lo
            hi = min(w_hi, max(days)) if days else w_hi
        else:
            first = tbank_first(e, "1m")
            lo = max(w_lo, local_day(first)) if first is not None else w_lo
            listed = daily.get(sym) or set()
            hi = min(w_hi, max(listed)) if listed else w_hi
        expected = {d for d in calendar if lo <= d <= hi}
        missing = sorted(expected - days)
        rows_out.append(
            {
                "symbol": sym,
                "rows": sum(by_venue.values()),
                "rows_by_venue": by_venue,
                "bars": len(bars),
                "first": min(days).isoformat() if days else None,
                "last": max(days).isoformat() if days else None,
                "expected_days": len(expected),
                "missing_days": len(missing),
                "missing_weekdays": sum(1 for d in missing if d.weekday() < 5),
                "missing_sample": [d.isoformat() for d in missing[:10]],
            }
        )
    return {
        "interval": interval,
        "generated_at": ns_to_iso(now_ns),
        "window": [w_lo.isoformat(), w_hi.isoformat()],
        "calendar": {
            "rule": f"a day is a trading day when >= {threshold:.0%} of the names listed then "
            "(between their first and last day candle) have a day candle",
            "days_in_window": sum(1 for d in calendar if w_lo <= d <= w_hi),
        },
        "instruments": len(rows_out),
        "rows": sum(r["rows"] for r in rows_out),
        "instruments_with_gaps": sum(1 for r in rows_out if r["missing_days"]),
        "instruments_without_rows": [r["symbol"] for r in rows_out if not r["rows"]],
        "per_instrument": rows_out,
    }
