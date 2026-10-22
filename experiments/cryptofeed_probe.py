"""Run cryptofeed's own Binance book handler offline on the same frames as the certifier.

cryptofeed 3.0.1 checks ``U``/``u`` continuity in ``Binance._check_update_id``; on a gap it drops
the book, logs a warning and fetches a new REST snapshot. With ``cross_check=True`` its
``Feed.book_callback`` raises ``BidAskOverlapping`` after three consecutive crossed updates. We
count both signals. The REST snapshot call is answered with the latest snapshot frame of the
stream, and the symbol table is set by hand, so no network is used.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from cryptofeed.defines import BINANCE, L2_BOOK
from cryptofeed.exceptions import BidAskOverlapping
from cryptofeed.exchanges import Binance
from cryptofeed.symbols import Symbols

from tape_recorder.frames import Frame
from tape_recorder.timeutil import NS_PER_S

CRYPTOFEED_SYMBOL = "BTC-USDT"
NATIVE_SYMBOL = "BTCUSDT"


@dataclass(frozen=True, slots=True)
class CryptofeedResult:
    gap_resets: int
    crossed_raised: bool

    @property
    def detected(self) -> bool:
        return self.gap_resets > 0 or self.crossed_raised


class _ProbeBinance(Binance):  # type: ignore[misc]
    """Counts the book resets that ``_check_update_id`` performs on a sequence gap."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.gap_resets = 0

    def _drop_book(self, std_pair: str) -> None:
        self.gap_resets += 1
        super()._drop_book(std_pair)


async def _noop(*_: Any) -> None:
    return None


async def _run(frames: list[Frame]) -> CryptofeedResult:
    feed = _ProbeBinance(
        symbols=[CRYPTOFEED_SYMBOL],
        channels=[L2_BOOK],
        callbacks={L2_BOOK: _noop},
        cross_check=True,
    )
    latest_snapshot: list[str] = []

    async def read(*_: Any, **__: Any) -> str:
        return latest_snapshot[-1]

    feed.http_conn.read = read
    crossed = False
    for f in sorted(frames, key=lambda fr: fr.ts_local):
        if f.source == "rest":
            latest_snapshot.append(f.payload)
            continue
        try:
            await feed.message_handler(f.payload, None, f.ts_local / NS_PER_S)
        except BidAskOverlapping:
            crossed = True
            break
    return CryptofeedResult(feed.gap_resets, crossed)


def run_cryptofeed(frames: list[Frame]) -> CryptofeedResult:
    """Feed one Binance spot stream through cryptofeed and report what it noticed."""
    if not Symbols.populated(BINANCE):
        Symbols.set(BINANCE, {CRYPTOFEED_SYMBOL: NATIVE_SYMBOL}, {})
    logging.getLogger("feedhandler").setLevel(logging.CRITICAL)
    return asyncio.run(_run(frames))
