"""``record`` against a loopback WebSocket server that plays the Hyperliquid fixture."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable
from pathlib import Path

import httpx
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
    payloads = [f.payload for f in venue_frames("hyperliquid")]
    subscriptions: list[str] = []

    async def handler(ws: ServerConnection) -> None:
        for _ in range(3):
            subscriptions.append(str(await ws.recv()))
        for p in payloads:
            await ws.send(p)
            await asyncio.sleep(0.002)
        await ws.wait_closed()

    ws_port, http_port = free_port(), free_port()
    s = make_settings(
        TAPE_QUALITY__COVERAGE_WINDOW="observed",
        TAPE_VENUES__HYPERLIQUID__WS_URL=f"ws://127.0.0.1:{ws_port}",
        TAPE_SERVICE__PORT=str(http_port),
        TAPE_SESSION__REDUNDANT_CONNECTIONS="2",
    )
    out = s.tape_root
    async with serve(handler, "127.0.0.1", ws_port):
        stop = asyncio.Event()
        task = asyncio.create_task(record(s, out, ["hyperliquid"], stop, raw_log=True))
        streams: list[dict[str, object]] = []
        async with httpx.AsyncClient() as client:
            for _ in range(200):
                await asyncio.sleep(0.05)
                try:
                    resp = await client.get(f"http://127.0.0.1:{http_port}/v1/streams")
                except httpx.TransportError:
                    continue
                streams = resp.json()
                if sum(int(str(x["messages"])) for x in streams) >= 40:
                    break
        stop.set()
        await task
    assert {x["channel"] for x in streams} >= {"book_snapshot", "trades"}
    assert len(subscriptions) == 6  # two redundant connections, three subscriptions each
    ((cert, _),) = certify_tree(s, out, s.certificates_root)
    # The fixture's exchange times are from another day, so only clock skew may fail.
    assert {c.name for c in cert.checks if not c.passed} <= {"clock_skew_p50_abs_ms"}
    assert cert.check("duplicate_messages").value == 0  # the slower connection was deduped
    raw_files = list(iter_frame_files([s.data_path(s.paths.raw)]))
    assert raw_files and len(load_frames(raw_files[0])) == 2 * len(payloads)
    replayed = replay_files(s, raw_files, "eu", tmp_path / "again")
    assert replayed.stats.duplicates >= len(payloads) and replayed.stats.parse_errors == 0
