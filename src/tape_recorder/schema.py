"""Contract 6.2: the normalised tape schema shared by every venue.

Each channel has a frozen record dataclass (what adapters emit) and an Arrow schema (what lands in
Parquet). Prices and sizes are float64: the tape is an analytics artefact, not an accounting ledger.

Optional extensions to contract 6.2 (nullable, documented in the README):
``book_delta.first_seq`` (venue's first update id in the message, Binance ``U``/Bybit ``u``) and
``trades.buyer``/``trades.seller`` (on-chain wallet addresses, Hyperliquid).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

import pyarrow as pa

SCHEMA_VERSION = "1"


class Channel(StrEnum):
    TRADES = "trades"
    BOOK_DELTA = "book_delta"
    BOOK_SNAPSHOT = "book_snapshot"
    FUNDING = "funding"
    LIQUIDATIONS = "liquidations"
    OPEN_INTEREST = "open_interest"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class BookSide(StrEnum):
    BID = "bid"
    ASK = "ask"


@dataclass(frozen=True, slots=True)
class _Common:
    venue: str
    symbol: str
    ts_exchange: int | None
    ts_local: int
    capture_region: str
    connection_id: str
    seq: int | None


@dataclass(frozen=True, slots=True)
class Trade(_Common):
    channel: ClassVar[Channel] = Channel.TRADES
    trade_id: str
    price: float
    size: float
    side: str
    buyer: str | None = None
    seller: str | None = None


@dataclass(frozen=True, slots=True)
class BookDelta(_Common):
    channel: ClassVar[Channel] = Channel.BOOK_DELTA
    side: str
    price: float
    size: float
    prev_seq: int | None
    first_seq: int | None = None


@dataclass(frozen=True, slots=True)
class BookSnapshotLevel(_Common):
    channel: ClassVar[Channel] = Channel.BOOK_SNAPSHOT
    side: str
    price: float
    size: float
    level: int
    snapshot_id: str


@dataclass(frozen=True, slots=True)
class Funding(_Common):
    channel: ClassVar[Channel] = Channel.FUNDING
    rate: float
    mark_price: float
    index_price: float | None
    next_funding_ts: int | None


@dataclass(frozen=True, slots=True)
class Liquidation(_Common):
    channel: ClassVar[Channel] = Channel.LIQUIDATIONS
    side: str
    price: float
    size: float
    trader: str | None = None


@dataclass(frozen=True, slots=True)
class OpenInterest(_Common):
    channel: ClassVar[Channel] = Channel.OPEN_INTEREST
    open_interest: float
    open_interest_value: float | None


Record = Trade | BookDelta | BookSnapshotLevel | Funding | Liquidation | OpenInterest

RECORD_TYPES: dict[Channel, type[Record]] = {
    Channel.TRADES: Trade,
    Channel.BOOK_DELTA: BookDelta,
    Channel.BOOK_SNAPSHOT: BookSnapshotLevel,
    Channel.FUNDING: Funding,
    Channel.LIQUIDATIONS: Liquidation,
    Channel.OPEN_INTEREST: OpenInterest,
}

_COMMON_FIELDS: list[pa.Field] = [
    pa.field("venue", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("ts_exchange", pa.int64(), nullable=True),
    pa.field("ts_local", pa.int64(), nullable=False),
    pa.field("capture_region", pa.string(), nullable=False),
    pa.field("connection_id", pa.string(), nullable=False),
    pa.field("seq", pa.int64(), nullable=True),
]

_CHANNEL_FIELDS: dict[Channel, list[pa.Field]] = {
    Channel.TRADES: [
        pa.field("trade_id", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("size", pa.float64(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("buyer", pa.string(), nullable=True),
        pa.field("seller", pa.string(), nullable=True),
    ],
    Channel.BOOK_DELTA: [
        pa.field("side", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("size", pa.float64(), nullable=False),
        pa.field("prev_seq", pa.int64(), nullable=True),
        pa.field("first_seq", pa.int64(), nullable=True),
    ],
    Channel.BOOK_SNAPSHOT: [
        pa.field("side", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("size", pa.float64(), nullable=False),
        pa.field("level", pa.int32(), nullable=False),
        pa.field("snapshot_id", pa.string(), nullable=False),
    ],
    Channel.FUNDING: [
        pa.field("rate", pa.float64(), nullable=False),
        pa.field("mark_price", pa.float64(), nullable=False),
        pa.field("index_price", pa.float64(), nullable=True),
        pa.field("next_funding_ts", pa.int64(), nullable=True),
    ],
    Channel.LIQUIDATIONS: [
        pa.field("side", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("size", pa.float64(), nullable=False),
        pa.field("trader", pa.string(), nullable=True),
    ],
    Channel.OPEN_INTEREST: [
        pa.field("open_interest", pa.float64(), nullable=False),
        pa.field("open_interest_value", pa.float64(), nullable=True),
    ],
}

SCHEMAS: dict[Channel, pa.Schema] = {
    channel: pa.schema(
        _COMMON_FIELDS + fields, metadata={"schema_version": SCHEMA_VERSION, "channel": channel}
    )
    for channel, fields in _CHANNEL_FIELDS.items()
}


def records_to_table(channel: Channel, records: Sequence[Record]) -> pa.Table:
    """Columnar conversion of same-channel records into an Arrow table with the contract schema."""
    schema = SCHEMAS[channel]
    columns = {name: [getattr(r, name) for r in records] for name in schema.names}
    return pa.Table.from_pydict(columns, schema=schema)


def table_to_records(channel: Channel, table: pa.Table) -> list[Record]:
    """Inverse of :func:`records_to_table` (used by tests and small-scale tooling)."""
    cls = RECORD_TYPES[channel]
    return [cls(**row) for row in table.to_pylist()]
