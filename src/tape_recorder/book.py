"""L2 order book reconstructed from snapshots and absolute-size deltas."""

from __future__ import annotations

from collections.abc import Iterable

from sortedcontainers import SortedDict

from tape_recorder.schema import BookSide

Level = tuple[float, float]


class OrderBook:
    """Price -> size per side; size 0 deletes a level. Best levels are O(log n)."""

    def __init__(self) -> None:
        self._bids: SortedDict = SortedDict()
        self._asks: SortedDict = SortedDict()

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()

    def load(self, bids: Iterable[Level], asks: Iterable[Level]) -> None:
        self.clear()
        for price, size in bids:
            if size > 0:
                self._bids[price] = size
        for price, size in asks:
            if size > 0:
                self._asks[price] = size

    def apply(self, side: str, price: float, size: float) -> None:
        book = self._bids if side == BookSide.BID else self._asks
        if size == 0:
            book.pop(price, None)
        else:
            book[price] = size

    def best_bid(self) -> float | None:
        return float(self._bids.peekitem(-1)[0]) if self._bids else None

    def best_ask(self) -> float | None:
        return float(self._asks.peekitem(0)[0]) if self._asks else None

    def is_crossed(self) -> bool:
        bid, ask = self.best_bid(), self.best_ask()
        return bid is not None and ask is not None and bid > ask

    def is_locked(self) -> bool:
        bid, ask = self.best_bid(), self.best_ask()
        return bid is not None and ask is not None and bid == ask

    def top(self, depth: int) -> tuple[list[Level], list[Level]]:
        """Best ``depth`` levels per side, best first."""
        n_b = len(self._bids)
        bids = [
            (float(self._bids.peekitem(n_b - 1 - i)[0]), float(self._bids.peekitem(n_b - 1 - i)[1]))
            for i in range(min(depth, n_b))
        ]
        asks = [
            (float(self._asks.peekitem(i)[0]), float(self._asks.peekitem(i)[1]))
            for i in range(min(depth, len(self._asks)))
        ]
        return bids, asks

    def __len__(self) -> int:
        return len(self._bids) + len(self._asks)


def levels_match(
    a: list[Level], b: list[Level], price_tolerance: float, size_rel_tolerance: float
) -> bool:
    if len(a) != len(b):
        return False
    for (pa, sa), (pb, sb) in zip(a, b, strict=True):
        if abs(pa - pb) > price_tolerance:
            return False
        if abs(sa - sb) > size_rel_tolerance * max(abs(sa), abs(sb), 1e-300):
            return False
    return True
