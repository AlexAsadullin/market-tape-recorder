"""Venue adapters behind one :class:`~tape_recorder.adapters.base.VenueAdapter` protocol."""

from __future__ import annotations

from tape_recorder.adapters.base import ParsedEvent, RecordFactory, VenueAdapter
from tape_recorder.adapters.binance import BinanceAdapter
from tape_recorder.adapters.bybit import BybitAdapter
from tape_recorder.adapters.hyperliquid import HyperliquidAdapter
from tape_recorder.adapters.moex import MoexIssAdapter
from tape_recorder.adapters.okx import OkxAdapter
from tape_recorder.settings import VenueSettings

__all__ = ["ParsedEvent", "RecordFactory", "VenueAdapter", "build_adapter", "build_adapters"]


def build_adapter(venue: str, settings: VenueSettings) -> VenueAdapter:
    match settings.adapter:
        case "binance_spot":
            return BinanceAdapter(venue, settings, "spot")
        case "binance_usdm":
            return BinanceAdapter(venue, settings, "usdm")
        case "bybit":
            return BybitAdapter(venue, settings)
        case "okx":
            return OkxAdapter(venue, settings)
        case "hyperliquid":
            return HyperliquidAdapter(venue, settings)
        case "moex_iss":
            return MoexIssAdapter(venue, settings)


def build_adapters(venues: dict[str, VenueSettings]) -> dict[str, VenueAdapter]:
    return {name: build_adapter(name, cfg) for name, cfg in venues.items()}
