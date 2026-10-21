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
    BookDelta,
    BookSide,
    BookSnapshotLevel,
    Channel,
    Funding,
    Liquidation,
    OpenInterest,
    Record,
    Trade,
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
        buyer: str | None = None,
        seller: str | None = None,
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
            buyer,
            seller,
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

    def funding(
        self,
        symbol: str,
        ts_exchange: int | None,
        rate: float,
        mark_price: float,
        index_price: float | None,
        next_funding_ts: int | None,
    ) -> Funding:
        return Funding(
            self.venue,
            symbol,
            ts_exchange,
            self.ts_local,
            self.capture_region,
            self.connection_id,
            None,
            rate,
            mark_price,
            index_price,
            next_funding_ts,
        )

    def liquidation(
        self,
        symbol: str,
        ts_exchange: int | None,
        side: str,
        price: float,
        size: float,
        trader: str | None = None,
    ) -> Liquidation:
        return Liquidation(
            self.venue,
            symbol,
            ts_exchange,
            self.ts_local,
            self.capture_region,
            self.connection_id,
            None,
            side,
            price,
            size,
            trader,
        )

    def open_interest(
        self, symbol: str, ts_exchange: int | None, value: float, notional: float | None
    ) -> OpenInterest:
        return OpenInterest(
            self.venue,
            symbol,
            ts_exchange,
            self.ts_local,
            self.capture_region,
            self.connection_id,
            None,
            value,
            notional,
        )


class VenueAdapter(Protocol):
    """Translation layer for one venue; see the module docstring."""

    venue: str
    transport: TransportKind
    settings: VenueSettings

    def ws_url(self) -> str:
        """WebSocket endpoint (``ws`` transports only)."""
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
    """Native venue symbol <-> contract symbol (``BASE-QUOTE-KIND``), from settings only."""

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
