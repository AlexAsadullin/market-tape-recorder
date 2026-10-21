from __future__ import annotations

import pyarrow as pa
import pytest

from tape_recorder.adapters.base import RecordFactory
from tape_recorder.schema import (
    SCHEMA_VERSION,
    SCHEMAS,
    Channel,
    Record,
    records_to_table,
    table_to_records,
)

COMMON = ["venue", "symbol", "ts_exchange", "ts_local", "capture_region", "connection_id", "seq"]


def sample_records() -> dict[Channel, list[Record]]:
    f = RecordFactory("okx", "eu", "c1", 1_000)
    return {
        Channel.TRADES: [f.trade("BTC-USDT-PERP", 900, None, "t1", 1.5, 2.0, "buy", "0xa", None)],
        Channel.BOOK_DELTA: list(f.deltas("BTC-USDT-PERP", 900, 7, 6, None, [(1.0, 2.0)], [])),
        Channel.BOOK_SNAPSHOT: list(
            f.snapshot("BTC-USDT-PERP", None, 7, "s7", [(1.0, 1.0), (2.0, 1.0)], [(3.0, 1.0)])
        ),
        Channel.FUNDING: [f.funding("BTC-USDT-PERP", 900, 1e-4, 100.0, None, None)],
        Channel.LIQUIDATIONS: [f.liquidation("BTC-USDT-PERP", 900, "sell", 99.0, 1.0)],
        Channel.OPEN_INTEREST: [f.open_interest("BTC-USDT-PERP", 900, 10.0, None)],
    }


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
