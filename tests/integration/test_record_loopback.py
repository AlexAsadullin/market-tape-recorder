"""``record`` against a loopback WebSocket server that plays the T-Bank fixture."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable
from pathlib import Path

import httpx
import orjson
from websockets.asyncio.server import ServerConnection, serve

from tape_recorder.frames import Frame, iter_frame_files, load_frames
from tape_recorder.quality.certify import certify_tree
from tape_recorder.recorder import record
from tape_recorder.replay import replay_files
from tape_recorder.settings import Settings

Frames = Callable[[str], list[Frame]]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def test_record_writes_tape_raw_log_and_serves_status(
    make_settings: Callable[..., Settings], venue_frames: Frames, tmp_path: Path
) -> None:
    frames = venue_frames("moex-tbank")
    payloads = [f.payload for f in frames if f.source == "ws"]
    subscriptions: list[str] = []
    auth: list[str | None] = []

    async def handler(ws: ServerConnection) -> None:
        auth.append(ws.request.headers.get("Authorization") if ws.request else None)
        for _ in range(4):
            subscriptions.append(str(await ws.recv()))
        for p in payloads:
            await ws.send(p)
            await asyncio.sleep(0.002)
        await ws.wait_closed()

    ws_port, http_port = free_port(), free_port()
    s = make_settings(
        TAPE_QUALITY__COVERAGE_WINDOW="observed",
        TAPE_SERVICE__PORT=str(http_port),
        TAPE_SESSION__REDUNDANT_CONNECTIONS="2",
        TBANK_TOKEN="loopback-token",
    )
    # The uid is known up front, because there is no ShareBy server on loopback; the resolve
    # call fails fast against a closed port, which the recorder logs and survives.
    uid = orjson.loads(frames[0].payload)["instrument"]["uid"]
    venue = s.venues["moex-tbank"]
    symbols = {"SBER": venue.symbols["SBER"].model_copy(update={"instrument_uid": uid})}
    venues = dict(s.venues)
    venues["moex-tbank"] = venue.model_copy(
        update={
            "ws_url": f"ws://127.0.0.1:{ws_port}",
            "rest_url": f"http://127.0.0.1:{free_port()}/rest/",
            "symbols": symbols,
        }
    )
    s = s.model_copy(update={"venues": venues})
    out = s.tape_root
    async with serve(handler, "127.0.0.1", ws_port):
        stop = asyncio.Event()
        task = asyncio.create_task(record(s, out, ["moex-tbank"], stop, raw_log=True))
        streams: list[dict[str, object]] = []
        async with httpx.AsyncClient() as client:
            for _ in range(200):
                await asyncio.sleep(0.05)
                try:
                    resp = await client.get(f"http://127.0.0.1:{http_port}/v1/streams")
                except httpx.TransportError:
                    continue
                streams = resp.json()
                if sum(int(str(x["messages"])) for x in streams) >= 30:
                    break
        stop.set()
        await task
    assert {x["channel"] for x in streams} >= {"book_snapshot", "trades"}
    assert len(subscriptions) == 8  # two redundant connections, four subscriptions each
    assert auth == ["Bearer loopback-token"] * 2
    ((cert, _),) = certify_tree(s, out, s.certificates_root)
    # The fixture's exchange times are from another day, so only clock skew may fail.
    assert {c.name for c in cert.checks if not c.passed} <= {"clock_skew_p50_abs_ms"}
    assert cert.check("duplicate_messages").value == 0  # the slower connection was deduped
    raw_files = list(iter_frame_files([s.data_path(s.paths.raw)]))
    # Every frame of both connections; the failed resolution attempt logs nothing.
    assert raw_files and len(load_frames(raw_files[0])) == 2 * len(payloads)
    replayed = replay_files(s, raw_files, "eu", tmp_path / "again")
    # The subscription answer and the ping carry no data, so they produce no duplicate.
    data = [p for p in payloads if "Response" not in p and '"ping"' not in p]
    assert replayed.stats.duplicates == len(data) and replayed.stats.parse_errors == 0
