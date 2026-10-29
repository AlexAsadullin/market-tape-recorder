"""Network transports behind small protocols, so sessions can be tested with in-memory fakes."""

from __future__ import annotations

from typing import Protocol

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException
from websockets.typing import Subprotocol

from tape_recorder.frames import RestRequest

TRANSPORT_ERRORS: tuple[type[BaseException], ...] = (
    OSError,
    ConnectionError,
    EOFError,
    TimeoutError,
    WebSocketException,
    httpx.HTTPError,
)
"""Failures that end a connection or a request and are retried with backoff."""


class WsConnection(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self) -> str: ...

    async def close(self) -> None: ...


class WsConnector(Protocol):
    async def __call__(
        self,
        url: str,
        headers: tuple[tuple[str, str], ...] = (),
        subprotocols: tuple[str, ...] = (),
    ) -> WsConnection: ...


class HttpFetcher(Protocol):
    async def __call__(self, request: RestRequest) -> str: ...


class _WebsocketsConnection:
    def __init__(self, conn: ClientConnection) -> None:
        self._conn = conn

    async def send(self, message: str) -> None:
        await self._conn.send(message)

    async def recv(self) -> str:
        data = await self._conn.recv()
        return data if isinstance(data, str) else data.decode()

    async def close(self) -> None:
        await self._conn.close()


class WebsocketsConnector:
    """``websockets`` client; protocol-level pings are handled by the library."""

    def __init__(self, open_timeout_s: float, max_message_bytes: int, user_agent: str) -> None:
        self._open_timeout = open_timeout_s
        self._max_size = max_message_bytes
        self._user_agent = user_agent

    async def __call__(
        self,
        url: str,
        headers: tuple[tuple[str, str], ...] = (),
        subprotocols: tuple[str, ...] = (),
    ) -> WsConnection:
        conn = await connect(
            url,
            open_timeout=self._open_timeout,
            max_size=self._max_size,
            user_agent_header=self._user_agent,
            additional_headers=list(headers) or None,
            subprotocols=[Subprotocol(p) for p in subprotocols] or None,
        )
        return _WebsocketsConnection(conn)


class HttpxFetcher:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def __call__(self, request: RestRequest) -> str:
        headers = dict(request.headers)
        if request.method == "POST":
            headers.setdefault("Content-Type", "application/json")
            resp = await self._client.post(
                request.url,
                params=list(request.params),
                headers=headers,
                content=(request.body or "{}").encode(),
            )
        else:
            resp = await self._client.get(request.url, params=list(request.params), headers=headers)
        resp.raise_for_status()
        return resp.text
