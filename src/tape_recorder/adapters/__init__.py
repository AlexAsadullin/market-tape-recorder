"""Venue adapters behind one :class:`~tape_recorder.adapters.base.VenueAdapter` protocol.

MOEX only (spec revision of 2026-10-29): ``moex_iss`` serves AlgoPack and the public ISS, and
``tbank`` serves the T-Bank Invest API stream. The crypto adapters of v0.1 are in git history.
"""

from __future__ import annotations

from tape_recorder.adapters.base import ParsedEvent, RecordFactory, VenueAdapter
from tape_recorder.adapters.moex import MoexIssAdapter
from tape_recorder.adapters.tbank import TBankAdapter
from tape_recorder.settings import VenueSettings

__all__ = ["ParsedEvent", "RecordFactory", "VenueAdapter", "build_adapter", "build_adapters"]


def build_adapter(venue: str, settings: VenueSettings) -> VenueAdapter:
    match settings.adapter:
        case "moex_iss":
            return MoexIssAdapter(venue, settings)
        case "tbank":
            return TBankAdapter(venue, settings)


def build_adapters(venues: dict[str, VenueSettings]) -> dict[str, VenueAdapter]:
    return {name: build_adapter(name, cfg) for name, cfg in venues.items()}
