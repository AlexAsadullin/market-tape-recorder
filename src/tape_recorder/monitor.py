"""Live per-stream health: last seen, lag and connection state, read by the status service."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from pydantic import BaseModel

from tape_recorder.schema import SCHEMA_VERSION, Record
from tape_recorder.timeutil import NS_PER_MS, NS_PER_S, Clock, ns_to_iso


@dataclass(slots=True)
class _StreamState:
    messages: int = 0
    last_ts_local: int = 0
    last_ts_exchange: int | None = None
    last_connection: str = ""


@dataclass(slots=True)
class _VenueState:
    active_connections: set[str] = field(default_factory=set)
    connects: int = 0
    disconnects: int = 0
    resyncs: int = 0


class StreamStatus(BaseModel):
    schema_version: str = SCHEMA_VERSION
    venue: str
    symbol: str
    channel: str
    messages: int
    last_seen: str | None
    age_s: float
    lag_ms: float | None
    last_connection: str
    stale: bool


class VenueStatus(BaseModel):
    schema_version: str = SCHEMA_VERSION
    venue: str
    active_connections: list[str]
    connects: int
    disconnects: int
    resyncs: int


class StreamMonitor:
    def __init__(self, clock: Clock, stale_after_s: float) -> None:
        self._clock = clock
        self._stale_ns = round(stale_after_s * NS_PER_S)
        self._streams: dict[tuple[str, str, str], _StreamState] = {}
        self._venues: dict[str, _VenueState] = {}

    def observe(self, records: Sequence[Record]) -> None:
        for r in records:
            st = self._streams.get((r.venue, r.symbol, r.channel))
            if st is None:
                st = self._streams[(r.venue, r.symbol, r.channel)] = _StreamState()
            st.messages += 1
            st.last_ts_local = max(st.last_ts_local, r.ts_local)
            if r.ts_exchange is not None:
                st.last_ts_exchange = r.ts_exchange
            st.last_connection = r.connection_id

    def _venue(self, venue: str) -> _VenueState:
        return self._venues.setdefault(venue, _VenueState())

    def connection_opened(self, venue: str, connection_id: str) -> None:
        v = self._venue(venue)
        v.active_connections.add(connection_id)
        v.connects += 1

    def connection_closed(self, venue: str, connection_id: str) -> None:
        v = self._venue(venue)
        v.active_connections.discard(connection_id)
        v.disconnects += 1

    def resync(self, venue: str) -> None:
        self._venue(venue).resyncs += 1

    def streams(self) -> list[StreamStatus]:
        now = self._clock()
        out = []
        for (venue, symbol, channel), st in sorted(self._streams.items()):
            lag = (
                (st.last_ts_local - st.last_ts_exchange) / NS_PER_MS
                if st.last_ts_exchange is not None
                else None
            )
            age_ns = now - st.last_ts_local
            out.append(
                StreamStatus(
                    venue=venue,
                    symbol=symbol,
                    channel=channel,
                    messages=st.messages,
                    last_seen=ns_to_iso(st.last_ts_local),
                    age_s=age_ns / NS_PER_S,
                    lag_ms=lag,
                    last_connection=st.last_connection,
                    stale=age_ns > self._stale_ns,
                )
            )
        return out

    def venues(self) -> list[VenueStatus]:
        return [
            VenueStatus(
                venue=name,
                active_connections=sorted(v.active_connections),
                connects=v.connects,
                disconnects=v.disconnects,
                resyncs=v.resyncs,
            )
            for name, v in sorted(self._venues.items())
        ]


class LoadStatus(BaseModel):
    """Recorder load, read by other jobs on the host so they back off before capture suffers.

    ``pressure`` is ``high`` when the event loop is late by more than the configured limit (the
    recorder cannot read sockets on time) or the writer's backlog exceeds its limit.
    """

    schema_version: str = SCHEMA_VERSION
    loop_lag_ms_max: float
    loop_lag_ms_last: float
    buffered_rows: int
    pressure: str
    limits: dict[str, float]


class LoadMonitor:
    """Event-loop lag over a sliding window plus the writer backlog; see :class:`LoadStatus`."""

    def __init__(
        self,
        buffered_rows: Callable[[], int],
        lag_limit_ms: float,
        buffered_limit_rows: int,
        window: int,
    ) -> None:
        self._buffered = buffered_rows
        self._lag_limit = lag_limit_ms
        self._rows_limit = buffered_limit_rows
        self._lags: deque[float] = deque(maxlen=window)

    def observe_lag(self, lag_ns: int) -> None:
        self._lags.append(max(0, lag_ns) / NS_PER_MS)

    def status(self) -> LoadStatus:
        lag_max = max(self._lags, default=0.0)
        rows = self._buffered()
        high = lag_max > self._lag_limit or rows > self._rows_limit
        return LoadStatus(
            loop_lag_ms_max=lag_max,
            loop_lag_ms_last=self._lags[-1] if self._lags else 0.0,
            buffered_rows=rows,
            pressure="high" if high else "ok",
            limits={"loop_lag_ms": self._lag_limit, "buffered_rows": float(self._rows_limit)},
        )
