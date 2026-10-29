"""Contract 6.2: the normalised tape schema shared by every venue.

Each channel has a frozen record dataclass (what adapters emit) and an Arrow schema (what lands in
Parquet). Prices and sizes are float64: the tape is an analytics artefact, not an accounting ledger.

Contract 6.2 revision 2 (MOEX) adds the channels ``candles``, ``trading_status`` and the AlgoPack
Super Candles ``tradestats``, ``orderstats`` and ``obstats``, and the nullable trade column
``trading_session``. The crypto channels (``funding``, ``liquidations``, ``open_interest``) and the
v0.1 extensions ``book_delta.first_seq`` and ``trades.buyer``/``seller`` stay readable, so tapes
written before the MOEX pivot still load.
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
    CANDLES = "candles"
    TRADING_STATUS = "trading_status"
    TRADESTATS = "tradestats"
    ORDERSTATS = "orderstats"
    OBSTATS = "obstats"


STATS_CHANNELS: tuple[Channel, ...] = (Channel.TRADESTATS, Channel.ORDERSTATS, Channel.OBSTATS)

# AlgoPack Super Candles columns per channel, as documented (moexalgo.github.io/docs/api).
STATS_COLUMNS: dict[Channel, tuple[str, ...]] = {
    Channel.TRADESTATS: (
        "pr_open", "pr_high", "pr_low", "pr_close", "pr_std", "vol", "val", "trades", "pr_vwap",
        "pr_change", "trades_b", "trades_s", "val_b", "val_s", "vol_b", "vol_s", "disb",
        "pr_vwap_b", "pr_vwap_s",
    ),
    Channel.ORDERSTATS: (
        "put_orders_b", "put_orders_s", "put_val_b", "put_val_s", "put_vol_b", "put_vol_s",
        "put_vwap_b", "put_vwap_s", "put_vol", "put_val", "put_orders", "cancel_orders_b",
        "cancel_orders_s", "cancel_val_b", "cancel_val_s", "cancel_vol_b", "cancel_vol_s",
        "cancel_vwap_b", "cancel_vwap_s", "cancel_vol", "cancel_val", "cancel_orders",
    ),
    Channel.OBSTATS: (
        "spread_bbo", "spread_lv10", "spread_1mio", "levels_b", "levels_s", "vol_b", "vol_s",
        "val_b", "val_s", "imbalance_vol_bbo", "imbalance_val_bbo", "imbalance_vol",
        "imbalance_val", "vwap_b", "vwap_s", "vwap_b_1mio", "vwap_s_1mio",
    ),
}  # fmt: skip


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
    trading_session: str | None = None


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


@dataclass(frozen=True, slots=True)
class Candle(_Common):
    """An OHLCV bar; ``ts_exchange`` is the bar open, ``ts_close`` its last nanosecond."""

    channel: ClassVar[Channel] = Channel.CANDLES
    interval: str
    ts_open: int
    ts_close: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    value: float | None = None


@dataclass(frozen=True, slots=True)
class TradingStatus(_Common):
    channel: ClassVar[Channel] = Channel.TRADING_STATUS
    status: str
    limit_order_allowed: bool | None = None
    market_order_allowed: bool | None = None


@dataclass(frozen=True, slots=True)
class StatsBar(_Common):
    """One AlgoPack Super Candle (5-minute bar); the metric names are in :data:`STATS_COLUMNS`.

    ``ts_exchange`` is the bar time stamped by the exchange (``tradedate`` + ``tradetime``) and
    ``ts_published`` the publication time (``SYSTIME``), which is when the bar became knowable.
    Unlike the other records the channel is a field, because three channels share this shape.
    """

    stats_channel: Channel
    ts_published: int | None
    values: tuple[tuple[str, float | None], ...]

    @property
    def channel(self) -> Channel:
        return self.stats_channel

    def value(self, name: str) -> float | None:
        return dict(self.values).get(name)


Record = (
    Trade
    | BookDelta
    | BookSnapshotLevel
    | Funding
    | Liquidation
    | OpenInterest
    | Candle
    | TradingStatus
    | StatsBar
)

RECORD_TYPES: dict[Channel, type[Record]] = {
    Channel.TRADES: Trade,
    Channel.BOOK_DELTA: BookDelta,
    Channel.BOOK_SNAPSHOT: BookSnapshotLevel,
    Channel.FUNDING: Funding,
    Channel.LIQUIDATIONS: Liquidation,
    Channel.OPEN_INTEREST: OpenInterest,
    Channel.CANDLES: Candle,
    Channel.TRADING_STATUS: TradingStatus,
    **dict.fromkeys(STATS_CHANNELS, StatsBar),
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
        pa.field("trading_session", pa.string(), nullable=True),
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
    Channel.CANDLES: [
        pa.field("interval", pa.string(), nullable=False),
        pa.field("ts_open", pa.int64(), nullable=False),
        pa.field("ts_close", pa.int64(), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.float64(), nullable=False),
        pa.field("value", pa.float64(), nullable=True),
    ],
    Channel.TRADING_STATUS: [
        pa.field("status", pa.string(), nullable=False),
        pa.field("limit_order_allowed", pa.bool_(), nullable=True),
        pa.field("market_order_allowed", pa.bool_(), nullable=True),
    ],
    **{
        channel: [
            pa.field("ts_published", pa.int64(), nullable=True),
            *(pa.field(name, pa.float64(), nullable=True) for name in names),
        ]
        for channel, names in STATS_COLUMNS.items()
    },
}

SCHEMAS: dict[Channel, pa.Schema] = {
    channel: pa.schema(
        _COMMON_FIELDS + fields, metadata={"schema_version": SCHEMA_VERSION, "channel": channel}
    )
    for channel, fields in _CHANNEL_FIELDS.items()
}


def _column_value(record: Record, name: str) -> object:
    if isinstance(record, StatsBar) and not hasattr(record, name):
        return record.value(name)
    return getattr(record, name)


def records_to_table(channel: Channel, records: Sequence[Record]) -> pa.Table:
    """Columnar conversion of same-channel records into an Arrow table with the contract schema."""
    schema = SCHEMAS[channel]
    columns = {name: [_column_value(r, name) for r in records] for name in schema.names}
    return pa.Table.from_pydict(columns, schema=schema)


def table_to_records(channel: Channel, table: pa.Table) -> list[Record]:
    """Inverse of :func:`records_to_table` (used by tests and small-scale tooling)."""
    if channel in STATS_CHANNELS:
        common = [f.name for f in _COMMON_FIELDS]
        names = STATS_COLUMNS[channel]
        return [
            StatsBar(
                **{k: row[k] for k in common},
                stats_channel=channel,
                ts_published=row["ts_published"],
                values=tuple((n, row[n]) for n in names),
            )
            for row in table.to_pylist()
        ]
    cls = RECORD_TYPES[channel]
    return [cls(**row) for row in table.to_pylist()]
