from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tape_recorder.adapters.base import RecordFactory
from tape_recorder.quality.reader import list_partitions, read_partition
from tape_recorder.schema import SCHEMAS, Channel, Record
from tape_recorder.settings import WriterSettings
from tape_recorder.timeutil import NS_PER_DAY, NS_PER_S
from tape_recorder.writer import TapeWriter, atomic_write_bytes, next_part_index

DAY0 = 1_792_454_400 * NS_PER_S  # 2026-10-20T00:00:00Z


class Clock:
    def __init__(self) -> None:
        self.now = DAY0

    def __call__(self) -> int:
        return self.now


def trades(n: int, ts: int, start: int = 0) -> list[Record]:
    f = RecordFactory("binance", "eu", "c1", ts)
    return [
        f.trade("BTC-USDT-SPOT", ts, i, str(i), 1.0, 1.0, "buy") for i in range(start, start + n)
    ]


def make(tmp_path: Path, **kw: float) -> tuple[TapeWriter, Clock]:
    cfg = WriterSettings(
        flush_rows=int(kw.get("flush_rows", 10)),
        flush_interval_s=kw.get("flush_interval_s", 5.0),
        rotate_rows=int(kw.get("rotate_rows", 25)),
        rotate_interval_s=kw.get("rotate_interval_s", 60.0),
        compression="zstd",
    )
    clock = Clock()
    return TapeWriter(tmp_path / "tape", tmp_path / "staging", cfg, clock), clock


def visible_parts(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "tape").rglob("*.parquet"))


def test_no_partial_file_is_visible_until_finalised(tmp_path: Path) -> None:
    w, _ = make(tmp_path)
    w.write(trades(15, DAY0))  # one row group flushed into staging
    assert visible_parts(tmp_path) == []
    assert list((tmp_path / "staging").glob("*.inprogress"))
    w.close()
    (part,) = visible_parts(tmp_path)
    assert part.name == "part-0.parquet"
    assert "venue=binance/channel=trades/symbol=BTC-USDT-SPOT/date=2026-10-20" in str(part)
    assert pq.read_table(part).num_rows == 15
    assert not list((tmp_path / "staging").glob("*.inprogress"))


def test_rotation_by_rows_and_time(tmp_path: Path) -> None:
    w, clock = make(tmp_path)
    w.write(trades(30, DAY0))  # 3 flushes of 10 -> rotates after 30 >= 25
    assert [p.name for p in visible_parts(tmp_path)] == ["part-0.parquet"]
    w.write(trades(10, DAY0, start=30))
    clock.now += 61 * NS_PER_S
    w.tick()
    assert [p.name for p in visible_parts(tmp_path)] == ["part-0.parquet", "part-1.parquet"]
    w.write(trades(3, DAY0, start=40))
    clock.now += 6 * NS_PER_S
    w.tick()  # time-based flush into a new staging part
    w.close()
    key = list_partitions(tmp_path / "tape")[0]
    table = read_partition(tmp_path / "tape", key)
    assert table.column("trade_id").to_pylist() == [str(i) for i in range(43)]


def test_partitions_split_by_utc_date_and_channel(tmp_path: Path) -> None:
    w, _ = make(tmp_path)
    f = RecordFactory("okx", "eu", "c1", DAY0 + NS_PER_DAY + 5)
    w.write(trades(2, DAY0))
    w.write([f.funding("BTC-USDT-PERP", None, 1e-4, 1.0, None, None)])
    w.write(trades(1, DAY0 + NS_PER_DAY))
    w.close()
    keys = list_partitions(tmp_path / "tape")
    assert {(k.venue, k.channel, k.date) for k in keys} == {
        ("binance", Channel.TRADES, "2026-10-20"),
        ("binance", Channel.TRADES, "2026-10-21"),
        ("okx", Channel.FUNDING, "2026-10-21"),
    }


def test_part_numbers_continue_after_restart(tmp_path: Path) -> None:
    for _ in range(2):
        w, _ = make(tmp_path)
        w.write(trades(3, DAY0))
        w.close()
    assert [p.name for p in visible_parts(tmp_path)] == ["part-0.parquet", "part-1.parquet"]
    assert next_part_index(tmp_path / "nowhere", {4}) == 5


def test_orphaned_staging_files_are_never_published(tmp_path: Path) -> None:
    (tmp_path / "staging").mkdir(parents=True)
    orphan = tmp_path / "staging" / "dead-part-0.parquet.inprogress"
    orphan.write_bytes(b"PAR1 no footer")
    w, _ = make(tmp_path)
    w.close()
    assert orphan.exists() and visible_parts(tmp_path) == []


def test_read_missing_partition_is_empty(tmp_path: Path) -> None:
    from tape_recorder.writer import PartitionKey

    t = read_partition(tmp_path, PartitionKey("x", Channel.TRADES, "y", "2026-01-01"))
    assert t.num_rows == 0 and t.schema == SCHEMAS[Channel.TRADES]


def test_atomic_write_bytes(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b.json"
    atomic_write_bytes(target, b"{}")
    atomic_write_bytes(target, b"[1]")
    assert target.read_bytes() == b"[1]"
    assert [p.name for p in target.parent.iterdir()] == ["b.json"]


@pytest.mark.parametrize("bad", [0, -1])
def test_writer_settings_validate(bad: int) -> None:
    with pytest.raises(ValueError):
        WriterSettings(
            flush_rows=bad,
            flush_interval_s=1,
            rotate_rows=1,
            rotate_interval_s=1,
            compression="zstd",
        )
