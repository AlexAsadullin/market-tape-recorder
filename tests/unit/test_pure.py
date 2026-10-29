"""Sequencing rules, book, interval maths and dedup."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from tape_recorder.book import OrderBook, levels_match
from tape_recorder.dedup import Deduplicator
from tape_recorder.quality.intervals import clip, holes, runs, subtract, total, union
from tape_recorder.sequencing import Continuity, DeltaIds, SequenceTracker, classify

SEED_SETTINGS = settings(derandomize=True, max_examples=200, deadline=None)


def test_first_id_alignment_after_snapshot() -> None:
    # Snapshot lastUpdateId = 100: first event must satisfy U <= 101 <= u.
    assert classify(100, DeltaIds(seq=100, prev_seq=None, first_seq=95)) is Continuity.STALE
    assert classify(100, DeltaIds(seq=104, prev_seq=None, first_seq=98)) is Continuity.CONTIGUOUS
    assert classify(100, DeltaIds(seq=104, prev_seq=None, first_seq=102)) is Continuity.GAP


def test_prev_seq_chain() -> None:
    assert classify(50, DeltaIds(seq=57, prev_seq=50, first_seq=None)) is Continuity.CONTIGUOUS
    assert classify(50, DeltaIds(seq=57, prev_seq=53, first_seq=51)) is Continuity.GAP


def test_consecutive_ids() -> None:
    assert classify(7, DeltaIds(seq=8, prev_seq=None, first_seq=8)) is Continuity.CONTIGUOUS
    assert classify(7, DeltaIds(seq=9, prev_seq=None, first_seq=9)) is Continuity.GAP


@SEED_SETTINGS
@given(st.lists(st.integers(min_value=1, max_value=50), min_size=2, max_size=60), st.data())
def test_chain_has_no_gap_until_a_message_is_dropped(steps: list[int], data: st.DataObject) -> None:
    msgs, last = [], 1000
    for step in steps:
        msgs.append(DeltaIds(seq=last + step, prev_seq=last, first_seq=None))
        last += step
    tracker = SequenceTracker()
    tracker.on_snapshot(1000)
    assert all(tracker.on_delta(m) is Continuity.CONTIGUOUS for m in msgs)
    drop = data.draw(st.integers(min_value=0, max_value=len(msgs) - 2))
    tracker = SequenceTracker()
    tracker.on_snapshot(1000)
    verdicts = [tracker.on_delta(m) for i, m in enumerate(msgs) if i != drop]
    assert Continuity.GAP in verdicts
    assert tracker.gaps == 1 and not tracker.synced


def test_tracker_unsynced_until_snapshot() -> None:
    t = SequenceTracker()
    assert t.on_delta(DeltaIds(5, 4, None)) is None
    t.on_snapshot(4)
    assert t.on_delta(DeltaIds(5, 4, None)) is Continuity.CONTIGUOUS
    t.on_snapshot(3)  # an older snapshot never moves the tracker back
    assert t.last == 5


def test_order_book_touch_and_top() -> None:
    b = OrderBook()
    b.load([(99.0, 1.0), (98.0, 2.0), (97.0, 0.0)], [(101.0, 1.0)])
    assert (b.best_bid(), b.best_ask(), len(b)) == (99.0, 101.0, 3)
    b.apply("ask", 100.0, 3.0)
    b.apply("bid", 99.0, 0.0)
    assert b.top(5) == ([(98.0, 2.0)], [(100.0, 3.0), (101.0, 1.0)])
    b.apply("bid", 100.0, 1.0)
    assert b.is_locked() and not b.is_crossed()
    b.apply("bid", 100.5, 1.0)
    assert b.is_crossed()
    b.clear()
    assert b.best_bid() is None and not b.is_crossed()


def test_levels_match_tolerances() -> None:
    assert levels_match([(1.0, 2.0)], [(1.0, 2.0 + 1e-12)], 1e-9, 1e-9)
    assert not levels_match([(1.0, 2.0)], [(1.0, 2.1)], 1e-9, 1e-9)
    assert not levels_match([(1.0, 2.0)], [(1.1, 2.0)], 1e-9, 1e-9)
    assert not levels_match([(1.0, 2.0)], [], 1e-9, 1e-9)


def test_interval_helpers() -> None:
    merged = union([(5, 7), (0, 2), (2, 3), (10, 12)])
    assert merged == [(0, 3), (5, 7), (10, 12)]
    assert holes(merged) == [(3, 5), (7, 10)]
    assert clip(merged, (1, 11)) == [(1, 3), (5, 7), (10, 11)]
    assert subtract([(0, 10)], [(2, 3), (5, 6)]) == [(0, 2), (3, 5), (6, 10)]
    assert runs([1, 2, 3, 10, 11], 2) == [(1, 3), (10, 11)]
    assert runs([], 2) == []


intervals = st.lists(
    st.tuples(st.integers(0, 1000), st.integers(0, 50)).map(lambda p: (p[0], p[0] + p[1])),
    max_size=20,
)


@SEED_SETTINGS
@given(intervals, intervals)
def test_subtract_is_measure_consistent(a: list[tuple[int, int]], b: list[tuple[int, int]]) -> None:
    ua, ub = union(a), union(b)
    rest = subtract(ua, ub)
    inter = subtract(ua, rest)
    assert total(rest) + total(inter) == total(ua)
    assert total(rest) >= total(ua) - total(ub)


def test_dedup_keeps_first_arrival_within_window() -> None:
    d = Deduplicator(window_s=1.0, max_keys=10)
    assert d.first_arrival("k", 0)
    assert not d.first_arrival("k", 500_000_000)
    assert d.first_arrival("k", 2_000_000_000)  # expired, so it is new again
    assert d.duplicates == 1


def test_dedup_is_bounded() -> None:
    d = Deduplicator(window_s=1000.0, max_keys=3)
    for i in range(10):
        d.first_arrival(i, i)
    assert len(d) <= 3
