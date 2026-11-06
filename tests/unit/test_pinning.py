"""SPKI pinning: match, key change, wrong host name, unpinned hosts, and pin-show sends nothing."""

from __future__ import annotations

import asyncio
import datetime as dt
import ssl
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from typer.testing import CliRunner

from tape_recorder.cli import app
from tape_recorder.pinning import (
    PinMismatchError,
    check_leaf,
    fetch_leaf,
    hostname_matches,
    normalise_pins,
    spki_pin,
)
from tape_recorder.transport import TlsPolicy

HOST = "api.pinned.test"


@dataclass
class Server:
    port: int
    der: bytes
    received: list[bytes]


def make_cert(tmp: Path, names: list[str]) -> tuple[Path, Path, bytes]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, names[0])])
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=30))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(n) for n in names]), False)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp / "cert.pem", tmp / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path, cert.public_bytes(serialization.Encoding.DER)


@pytest.fixture
def server(tmp_path: Path) -> Iterator[Server]:
    """A TLS server on 127.0.0.1 with a self-signed leaf for HOST; records application bytes."""
    cert, key, der = make_cert(tmp_path, [HOST])
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    received: list[bytes] = []
    loop = asyncio.new_event_loop()
    ready = threading.Event()
    holder: dict[str, int] = {}

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            data = await asyncio.wait_for(reader.read(65536), 2.0)
            if data:
                received.append(data)
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                await writer.drain()
        except (TimeoutError, OSError, ssl.SSLError):
            pass
        finally:
            writer.close()

    async def start() -> asyncio.Server:
        srv = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=ctx)
        holder["port"] = srv.sockets[0].getsockname()[1]
        ready.set()
        return srv

    def run() -> None:
        asyncio.set_event_loop(loop)
        srv = loop.run_until_complete(start())
        loop.run_forever()
        srv.close()
        loop.run_until_complete(srv.wait_closed())

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    ready.wait(5)
    yield Server(holder["port"], der, received)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(5)


async def handshake(port: int, ctx: ssl.SSLContext, host: str = HOST) -> None:
    _, writer = await asyncio.open_connection("127.0.0.1", port, ssl=ctx, server_hostname=host)
    writer.close()


async def test_matching_pin_is_accepted(server: Server) -> None:
    policy = TlsPolicy({HOST: [spki_pin(server.der)]})
    await handshake(server.port, policy.for_host(HOST))


async def test_changed_key_fails_closed(server: Server) -> None:
    other = "sha256/" + "A" * 43 + "="
    policy = TlsPolicy({HOST: [other]})
    with pytest.raises(ssl.SSLError, match=f"pinned key changed for {HOST}: got sha256/"):
        await handshake(server.port, policy.for_host(HOST))


async def test_wrong_hostname_fails_even_with_matching_key(server: Server) -> None:
    pin = spki_pin(server.der)
    policy = TlsPolicy({HOST: [pin], "evil.test": [pin]})
    with pytest.raises(ssl.SSLError, match=r"does not name evil\.test"):
        await handshake(server.port, policy.for_host("evil.test"), host="evil.test")


async def test_unpinned_hosts_keep_chain_validation(server: Server) -> None:
    policy = TlsPolicy({HOST: [spki_pin(server.der)]})
    ctx = policy.for_host("iss.moex.com")
    assert ctx is policy.default and ctx.verify_mode is ssl.CERT_REQUIRED and ctx.check_hostname
    with pytest.raises(ssl.SSLCertVerificationError, match=r"self-signed|unable to get"):
        await handshake(server.port, ctx, host=HOST)
    assert policy.for_url("https://iss.moex.com/iss") is policy.default
    assert policy.for_url(f"wss://{HOST}/ws") is policy.pinned
    assert policy.for_url("ws://127.0.0.1:1/x") is None


async def test_pinned_context_refuses_a_host_it_does_not_pin(server: Server) -> None:
    policy = TlsPolicy({"other.test": [spki_pin(server.der)]})
    assert policy.pinned is not None
    with pytest.raises(ssl.SSLError, match="not a pinned host"):
        await handshake(server.port, policy.pinned, host=HOST)


def test_pin_helpers(tmp_path: Path) -> None:
    _, _, der = make_cert(tmp_path, ["*.tbank.ru", "tbank.ru"])
    assert hostname_matches(der, "invest-public-api.tbank.ru")
    assert hostname_matches(der, "tbank.ru")
    assert not hostname_matches(der, "a.b.tbank.ru")
    assert not hostname_matches(der, "tbank.ru.evil.test")
    check_leaf(der, "tbank.ru", [spki_pin(der), "sha256/" + "B" * 43 + "="])  # rotation: 2 pins
    with pytest.raises(PinMismatchError):
        check_leaf(der, "tbank.ru", ["sha256/" + "B" * 43 + "="])
    with pytest.raises(ValueError, match="sha256/"):
        normalise_pins({"h": ["md5/xyz"]})
    with pytest.raises(ValueError, match="at least one pin"):
        normalise_pins({"h": []})


def test_pin_show_sends_no_token(
    server: Server, monkeypatch: pytest.MonkeyPatch, data_dir: Path
) -> None:
    monkeypatch.setenv("TBANK_TOKEN", "never-send-this-token")
    result = CliRunner().invoke(
        app, ["pin-show", HOST, "--port", str(server.port), "--connect", "127.0.0.1"]
    )
    assert result.exit_code == 0, result.output
    assert spki_pin(server.der) in result.output
    assert "host not pinned" in result.output
    assert server.received == []  # handshake only: no request, no header, no token
    assert fetch_leaf(HOST, server.port, 5.0, "127.0.0.1") == server.der
