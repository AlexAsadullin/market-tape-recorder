"""Earliest-arrival deduplication across redundant connections.

Events are offered in arrival order, so the first offer of a key is the earliest arrival and wins;
later copies (from an overlapping or redundant connection) are dropped. Keys expire after a time
window measured on ``ts_local`` and the table is bounded, so memory stays flat on long sessions.
"""

from __future__ import annotations

from collections.abc import Hashable

from tape_recorder.timeutil import NS_PER_S


class Deduplicator:
    def __init__(self, window_s: float, max_keys: int) -> None:
        self._window_ns = round(window_s * NS_PER_S)
        self._max_keys = max_keys
        self._seen: dict[Hashable, int] = {}
        self.duplicates = 0

    def __len__(self) -> int:
        return len(self._seen)

    def first_arrival(self, key: Hashable, ts_local: int) -> bool:
        """Record ``key``; True if it was not seen within the window (keep), False if duplicate."""
        horizon = ts_local - self._window_ns
        seen = self._seen
        while seen:
            oldest_key = next(iter(seen))
            if seen[oldest_key] >= horizon and len(seen) < self._max_keys:
                break
            del seen[oldest_key]
        if key in seen:
            self.duplicates += 1
            return False
        seen[key] = ts_local
        return True
