"""Per-venue sequence rules for order-book deltas (pure functions).

Every sequenced delta message is described by ``seq`` (its final update id) and either
``prev_seq`` (the final id of the message before it, OKX ``prevSeqId`` / Binance USD-M ``pu``) or
``first_seq`` (its first update id, Binance spot ``U`` / Bybit ``u``). From those we derive the
first update id the message needs to have been preceded by::

    start = prev_seq + 1   if prev_seq is known
          = first_seq      otherwise

Given the last applied id ``L`` (from a delta or a snapshot), a message is

- ``stale``      if ``seq <= L`` (already reflected; a duplicate or an old copy),
- ``contiguous`` if ``start <= L + 1`` (L2 levels carry absolute sizes, so overlap is harmless),
- ``gap``        otherwise: updates ``L + 1 .. start - 1`` were never seen.

This one rule covers Binance spot (``U <= L+1 <= u``, the documented snapshot alignment),
Binance USD-M (``pu == L``), OKX (``prevSeqId == L``) and Bybit (``u == L+1``).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Continuity(StrEnum):
    STALE = "stale"
    CONTIGUOUS = "contiguous"
    GAP = "gap"


@dataclass(frozen=True, slots=True)
class DeltaIds:
    seq: int
    prev_seq: int | None
    first_seq: int | None

    @property
    def start(self) -> int:
        if self.prev_seq is not None:
            return self.prev_seq + 1
        if self.first_seq is not None:
            return self.first_seq
        return self.seq


def classify(last_applied: int, ids: DeltaIds) -> Continuity:
    if ids.seq <= last_applied:
        return Continuity.STALE
    if ids.start <= last_applied + 1:
        return Continuity.CONTIGUOUS
    return Continuity.GAP


class SequenceTracker:
    """Live continuity tracker for one book stream, used by sessions to trigger resyncs."""

    def __init__(self) -> None:
        self.last: int | None = None
        self.gaps = 0

    @property
    def synced(self) -> bool:
        return self.last is not None

    def on_snapshot(self, seq: int) -> None:
        if self.last is None or seq > self.last:
            self.last = seq

    def on_delta(self, ids: DeltaIds) -> Continuity | None:
        """Classify a delta; returns None while unsynced (waiting for a snapshot)."""
        if self.last is None:
            return None
        result = classify(self.last, ids)
        if result is Continuity.CONTIGUOUS:
            self.last = ids.seq
        elif result is Continuity.GAP:
            self.gaps += 1
            self.last = None
        return result
