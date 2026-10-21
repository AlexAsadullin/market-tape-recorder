"""Closed-interval arithmetic on int64 ns timestamps (pure functions)."""

from __future__ import annotations

import itertools
from collections.abc import Iterable

Interval = tuple[int, int]


def union(intervals: Iterable[Interval]) -> list[Interval]:
    """Sorted, non-overlapping union; touching intervals are merged."""
    out: list[Interval] = []
    for start, end in sorted(i for i in intervals if i[1] >= i[0]):
        if out and start <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], end))
        else:
            out.append((start, end))
    return out


def holes(merged: list[Interval]) -> list[Interval]:
    """Open spaces between consecutive intervals of a merged union."""
    return [(a[1], b[0]) for a, b in itertools.pairwise(merged) if b[0] > a[1]]


def clip(intervals: Iterable[Interval], window: Interval) -> list[Interval]:
    lo, hi = window
    return [(max(s, lo), min(e, hi)) for s, e in intervals if e > lo and s < hi]


def subtract(base: list[Interval], cut: list[Interval]) -> list[Interval]:
    """``base`` minus ``cut``; both must be merged unions."""
    out: list[Interval] = []
    j = 0
    for start, end in base:
        cur = start
        while j < len(cut) and cut[j][1] <= cur:
            j += 1
        k = j
        while k < len(cut) and cut[k][0] < end:
            if cut[k][0] > cur:
                out.append((cur, cut[k][0]))
            cur = max(cur, cut[k][1])
            k += 1
        if cur < end:
            out.append((cur, end))
    return out


def total(intervals: Iterable[Interval]) -> int:
    return sum(e - s for s, e in intervals)


def runs(sorted_ts: list[int], max_gap: int) -> list[Interval]:
    """Group sorted timestamps into runs whose consecutive spacing is at most ``max_gap``."""
    if not sorted_ts:
        return []
    out: list[Interval] = []
    start = prev = sorted_ts[0]
    for t in sorted_ts[1:]:
        if t - prev > max_gap:
            out.append((start, prev))
            start = t
        prev = t
    out.append((start, prev))
    return out
