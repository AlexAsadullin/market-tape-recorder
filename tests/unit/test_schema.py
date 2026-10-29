from __future__ import annotations

import pyarrow as pa
import pytest

from tape_recorder.adapters.base import RecordFactory
from tape_recorder.schema import (
    SCHEMA_VERSION,
    SCHEMAS,
    STATS_COLUMNS,
    Channel,
    Funding,
    Liquidation,
    OpenInterest,
    Record,
    records_to_table,
    table_to_records,
)

COMMON = ["venue", "symbol", "ts_exchange", "ts_local", "capture_region", "connection_id", "seq"]


def sample_records() -> dict[Channel, list[Record]]:
    f = RecordFactory("moex-algopack", "msk", "c1", 1_000)
    sym = "SBER-RUB-TQBR"
    # Legacy crypto channels stay readable, so tapes written before the MOEX pivot still load.
    common = ("binance", "BTC-USDT-PERP", 900, 1_000, "eu", "c1", None)
    out: dict[Channel, list[Record]] = {
        Channel.TRADES: [f.trade(sym, 900, 5, "5", 285.4, 10.0, "buy", "1")],
        Channel.BOOK_DELTA: list(f.deltas(sym, 900, 7, 6, None, [(285.0, 2.0)], [])),
        Channel.BOOK_SNAPSHOT: list(
            f.snapshot(sym, None, 7, "s7", [(1.0, 1.0), (2.0, 1.0)], [(3.0, 1.0)])
        ),
        Channel.FUNDING: [Funding(*common, 1e-4, 100.0, None, None)],
        Channel.LIQUIDATIONS: [Liquidation(*common, "sell", 99.0, 1.0)],
        Channel.OPEN_INTEREST: [OpenInterest(*common, 10.0, None)],
        Channel.CANDLES: [f.candle(sym, "1m", 0, 59, (1.0, 2.0, 0.5, 1.5), 100.0, None)],
        Channel.TRADING_STATUS: [f.trading_status(sym, 900, "T", True, None)],
    }
    for channel, names in STATS_COLUMNS.items():
        values = {name: float(i) for i, name in enumerate(names)}
        out[channel] = [f.stats_bar(channel, sym, 900, 950, values)]
    return out


@pytest.mark.parametrize("channel", list(Channel))
def test_round_trip_and_contract_columns(channel: Channel) -> None:
    records = sample_records()[channel]
    table = records_to_table(channel, records)
    assert table.schema.names[:7] == COMMON
    assert table.schema.metadata[b"schema_version"] == SCHEMA_VERSION.encode()
    assert table.schema.field("ts_local").type == pa.int64()
    assert table_to_records(channel, table) == records


def test_snapshot_levels_are_best_first() -> None:
    snap = sample_records()[Channel.BOOK_SNAPSHOT]
    bids = [r for r in snap if r.side == "bid"]  # type: ignore[union-attr]
    assert [(r.price, r.level) for r in bids] == [(2.0, 0), (1.0, 1)]  # type: ignore[union-attr]


def test_contract_6_2_channel_fields() -> None:
    assert {"trade_id", "price", "size", "side"} <= set(SCHEMAS[Channel.TRADES].names)
    assert {"side", "price", "size", "prev_seq"} <= set(SCHEMAS[Channel.BOOK_DELTA].names)
    assert {"level", "snapshot_id"} <= set(SCHEMAS[Channel.BOOK_SNAPSHOT].names)
    assert {"rate", "mark_price", "index_price", "next_funding_ts"} <= set(
        SCHEMAS[Channel.FUNDING].names
    )
    assert {"trader"} <= set(SCHEMAS[Channel.LIQUIDATIONS].names)
    assert {"open_interest", "open_interest_value"} <= set(SCHEMAS[Channel.OPEN_INTEREST].names)


def test_contract_6_2_revision_2_fields() -> None:
    assert "trading_session" in SCHEMAS[Channel.TRADES].names
    assert {"interval", "ts_open", "ts_close", "close", "volume", "value"} <= set(
        SCHEMAS[Channel.CANDLES].names
    )
    assert {"status", "limit_order_allowed"} <= set(SCHEMAS[Channel.TRADING_STATUS].names)
    for channel, names in STATS_COLUMNS.items():
        assert {"ts_published", *names} <= set(SCHEMAS[channel].names)
    assert "spread_bbo" in STATS_COLUMNS[Channel.OBSTATS]
