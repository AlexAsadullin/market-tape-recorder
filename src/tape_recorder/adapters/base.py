"""The ``VenueAdapter`` protocol and helpers shared by every venue.

An adapter is pure translation: it builds subscription messages and REST requests from settings,
and turns raw payloads into normalised records. It never touches the network itself; sessions do.
That keeps every adapter testable by replaying recorded frames.
"""

from __future__ import annotations

from collections.abc import Hashable, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from tape_recorder.frames import RestRequest
from tape_recorder.schema import (
    STATS_COLUMNS,
    BookDelta,
    BookSide,
    BookSnapshotLevel,
    Candle,
    Channel,
    Record,
    StatsBar,
    Trade,
    TradingStatus,
)
from tape_recorder.settings import SymbolSettings, VenueSettings

TransportKind = Literal["ws", "rest_poll"]
Level = tuple[float, float]


@dataclass(frozen=True, slots=True)
class ParsedEvent:
    """Records produced by one venue event, with a key identifying the event for dedup.

    Two connections that deliver the same venue event produce equal keys; ``None`` disables dedup.
    """

    key: Hashable | None
    records: tuple[Record, ...]


class RecordFactory:
    """Stamps records with capture metadata (venue, region, connection, arrival time)."""

    __slots__ = ("capture_region", "connection_id", "ts_local", "venue")

    def __init__(self, venue: str, capture_region: str, connection_id: str, ts_local: int) -> None:
        self.venue = venue
        self.capture_region = capture_region
        self.connection_id = connection_id
        self.ts_local = ts_local

    def trade(
        self,
        symbol: str,
        ts_exchange: int | None,
        seq: int | None,
        trade_id: str,
        price: float,
        size: float,
        side: str,
        trading_session: str | None = None,
    ) -> Trade:
        return Trade(
            self.venue,
            symbol,
            ts_exchange,
            self.ts_local,
            self.capture_region,
            self.connection_id,
            seq,
            trade_id,
            price,
            size,
            side,
            None,
            None,
            trading_session,
        )

    def deltas(
        self,
        symbol: str,
        ts_exchange: int | None,
        seq: int,
        prev_seq: int | None,
        first_seq: int | None,
        bids: Sequence[Level],
        asks: Sequence[Level],
    ) -> tuple[BookDelta, ...]:
        out: list[BookDelta] = []
        for side, levels in ((BookSide.BID, bids), (BookSide.ASK, asks)):
            out.extend(
                BookDelta(
                    self.venue,
                    symbol,
                    ts_exchange,
                    self.ts_local,
                    self.capture_region,
                    self.connection_id,
                    seq,
                    side,
                    price,
                    size,
                    prev_seq,
                    first_seq,
                )
                for price, size in levels
            )
        return tuple(out)

    def snapshot(
        self,
        symbol: str,
        ts_exchange: int | None,
        seq: int | None,
        snapshot_id: str,
        bids: Sequence[Level],
        asks: Sequence[Level],
    ) -> tuple[BookSnapshotLevel, ...]:
        """Snapshot rows; levels are re-sorted best-first so ``level`` 0 is the touch."""
        out: list[BookSnapshotLevel] = []
        ordered = (
            (BookSide.BID, sorted(bids, key=lambda lv: -lv[0])),
            (BookSide.ASK, sorted(asks, key=lambda lv: lv[0])),
        )
        for side, levels in ordered:
            out.extend(
                BookSnapshotLevel(
                    self.venue,
                    symbol,
                    ts_exchange,
                    self.ts_local,
                    self.capture_region,
                    self.connection_id,
                    seq,
                    side,
                    price,
                    size,
                    i,
                    snapshot_id,
                )
                for i, (price, size) in enumerate(levels)
            )
        return tuple(out)

    def candle(
        self,
        symbol: str,
        interval: str,
        ts_open: int,
        ts_close: int,
        ohlc: tuple[float, float, float, float],
        volume: float,
        value: float | None,
    ) -> Candle:
        o, h, lo, c = ohlc
        return Candle(
            self.venue,
            symbol,
            ts_open,
            self.ts_local,
            self.capture_region,
            self.connection_id,
            None,
            interval,
            ts_open,
            ts_close,
            o,
            h,
            lo,
            c,
            volume,
            value,
        )

    def trading_status(
        self,
        symbol: str,
        ts_exchange: int | None,
        status: str,
        limit_order_allowed: bool | None = None,
        market_order_allowed: bool | None = None,
    ) -> TradingStatus:
        return TradingStatus(
            self.venue,
            symbol,
            ts_exchange,
            self.ts_local,
            self.capture_region,
            self.connection_id,
            None,
            status,
            limit_order_allowed,
            market_order_allowed,
        )

    def stats_bar(
        self,
        channel: Channel,
        symbol: str,
        ts_exchange: int,
        ts_published: int | None,
        values: dict[str, float | None],
    ) -> StatsBar:
        return StatsBar(
            self.venue,
            symbol,
            ts_exchange,
            self.ts_local,
            self.capture_region,
            self.connection_id,
            None,
            channel,
            ts_published,
            tuple((name, values.get(name)) for name in STATS_COLUMNS[channel]),
        )


class VenueAdapter(Protocol):
    """Translation layer for one venue; see the module docstring."""

    venue: str
    transport: TransportKind
    settings: VenueSettings

    def ws_url(self) -> str:
        """WebSocket endpoint (``ws`` transports only)."""
        ...

    def ws_headers(self) -> tuple[tuple[str, str], ...]:
        """Extra handshake headers (for example a bearer token); empty for public feeds."""
        ...

    def ws_subprotocols(self) -> tuple[str, ...]:
        """WebSocket subprotocols to request in the handshake."""
        ...

    def subscribe_messages(self) -> list[str]:
        """Text frames to send right after a connection opens."""
        ...

    def snapshot_requests(self) -> list[RestRequest]:
        """REST book snapshots needed to (re)synchronise WebSocket deltas; empty if in-band."""
        ...

    def poll_requests(self) -> list[RestRequest]:
        """REST requests for one polling round (``rest_poll`` transports only)."""
        ...

    def backlogged(self) -> bool:
        """True when the last poll filled a page and should be repeated without waiting."""
        ...

    def parse_ws(self, payload: str, factory: RecordFactory) -> list[ParsedEvent]:
        """Normalise one WebSocket text frame."""
        ...

    def parse_rest(
        self, request: RestRequest, payload: str, factory: RecordFactory
    ) -> list[ParsedEvent]:
        """Normalise one REST response body for ``request``."""
        ...


class SymbolMap:
    """Native venue symbol <-> contract symbol (``<SECID>-RUB-<BOARD>``), from settings only."""

    def __init__(self, symbols: dict[str, SymbolSettings]) -> None:
        self._by_native = symbols
        self._native_by_symbol = {s.symbol: native for native, s in symbols.items()}
        self._by_upper = {native.upper(): native for native in symbols}

    def natives(self) -> list[str]:
        return list(self._by_native)

    def resolve(self, native: str) -> SymbolSettings | None:
        """Settings for a native symbol (case-insensitive), or None if not configured."""
        key = self._by_upper.get(native.upper())
        return self._by_native[key] if key is not None else None

    def native_for(self, symbol: str) -> str:
        return self._native_by_symbol[symbol]


def levels(raw: Sequence[Sequence[str | float]], contract_size: float) -> list[Level]:
    """Parse ``[[price, size, ...], ...]`` level arrays, converting sizes to base units."""
    return [(float(lv[0]), float(lv[1]) * contract_size) for lv in raw]


def require_channels(settings: VenueSettings, *channels: Channel) -> bool:
    return any(c in settings.channels for c in channels)


def no_rest_requests() -> list[RestRequest]:
    return []
