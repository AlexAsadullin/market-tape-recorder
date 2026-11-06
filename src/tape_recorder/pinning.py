"""Public-key pinning for hosts whose certificate chain ends at a root the platform does not trust.

T-Bank's API hosts chain to the Russian Trusted Root CA, which the user does not trust anywhere,
not even inside the recorder. For a host listed in ``http.pins`` the recorder therefore skips
chain validation and accepts the TLS connection only when

1. the SHA-256 of the leaf certificate's SubjectPublicKeyInfo matches one of the host's pins
   (``sha256/<base64>``, the HPKP/curl format; several pins allow a planned key rotation), and
2. the leaf's subjectAltName DNS names match the host (one leftmost ``*`` label at most).

Otherwise the handshake fails closed with :class:`PinMismatchError`. Every host without pins
keeps normal certifi chain and hostname validation, and verification is never switched off.

The check runs inside the handshake (``SSLObject.do_handshake``), so it covers the asyncio
connections of both httpx and websockets before any application byte, such as the
``Authorization`` header, is sent.
"""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import socket
import ssl
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import ExtensionOID

PIN_PREFIX = "sha256/"


class PinMismatchError(ssl.SSLCertVerificationError):
    """The leaf of a pinned host carries another key, or names another host."""


def spki_pin(der: bytes) -> str:
    """``sha256/<base64>`` of the certificate's DER-encoded SubjectPublicKeyInfo."""
    cert = x509.load_der_x509_certificate(der)
    spki = cert.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return PIN_PREFIX + base64.b64encode(hashlib.sha256(spki).digest()).decode()


def cert_fingerprint(der: bytes) -> str:
    """SHA-256 of the whole certificate, as browsers show it (``AB:CD:...``)."""
    return ":".join(f"{b:02X}" for b in hashlib.sha256(der).digest())


def _dns_match(pattern: str, host: str) -> bool:
    pattern, host = pattern.lower().rstrip("."), host.lower().rstrip(".")
    if not pattern.startswith("*."):
        return pattern == host
    suffix = pattern[1:]  # ".tbank.ru"
    label = host[: -len(suffix)] if host.endswith(suffix) else ""
    return bool(label) and "." not in label and "*" not in suffix


def hostname_matches(der: bytes, host: str) -> bool:
    """RFC 6125 style: subjectAltName only (DNS names, or IP addresses for an IP host)."""
    cert = x509.load_der_x509_certificate(der)
    try:
        san = cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value
    except x509.ExtensionNotFound:
        return False
    assert isinstance(san, x509.SubjectAlternativeName)
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return any(_dns_match(name, host) for name in san.get_values_for_type(x509.DNSName))
    return ip in san.get_values_for_type(x509.IPAddress)


def check_leaf(der: bytes, host: str, pins: Sequence[str]) -> None:
    """Raise :class:`PinMismatchError` unless the leaf matches a pin and the host name."""
    got = spki_pin(der)
    if got not in pins:
        raise PinMismatchError(
            f"pinned key changed for {host}: got {got}, expected {', '.join(pins)}"
        )
    if not hostname_matches(der, host):
        raise PinMismatchError(f"pinned certificate for {host} does not name {host}")


def normalise_pins(pins: Mapping[str, Sequence[str]]) -> dict[str, tuple[str, ...]]:
    out: dict[str, tuple[str, ...]] = {}
    for host, values in pins.items():
        for pin in values:
            raw = pin.removeprefix(PIN_PREFIX)
            if not pin.startswith(PIN_PREFIX) or len(base64.b64decode(raw, validate=True)) != 32:
                raise ValueError(f"pin for {host} must be sha256/<base64 of 32 bytes>: {pin!r}")
        if not values:
            raise ValueError(f"pinned host {host} needs at least one pin")
        out[host.lower()] = tuple(values)
    return out


def pinned_context(host_pins: Mapping[str, Sequence[str]]) -> ssl.SSLContext:
    """A client context for pinned hosts only: no chain check, the pin check instead.

    It refuses any host that is not in ``host_pins``, so it cannot be used by mistake for an
    unpinned host without validation.
    """
    pins = normalise_pins(host_pins)

    def verify(host: str | None, der: bytes | None) -> None:
        if host is None or host.lower() not in pins:
            raise PinMismatchError(f"{host} is not a pinned host")
        if der is None:
            raise PinMismatchError(f"{host} sent no certificate")
        check_leaf(der, host, pins[host.lower()])

    class PinnedSSLObject(ssl.SSLObject):
        def do_handshake(self) -> None:
            super().do_handshake()  # raises SSLWantReadError until the handshake is complete
            verify(self.server_hostname, self.getpeercert(binary_form=True))

    class PinnedSSLSocket(ssl.SSLSocket):
        def do_handshake(self, block: bool = False) -> None:
            super().do_handshake(block)
            verify(self.server_hostname, self.getpeercert(binary_form=True))

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = False  # checked against the leaf's SAN in verify()
    ctx.verify_mode = ssl.CERT_NONE  # the chain ends at an untrusted root; the pin replaces it
    ctx.sslobject_class = PinnedSSLObject
    ctx.sslsocket_class = PinnedSSLSocket
    return ctx


@dataclass(frozen=True, slots=True)
class CertInfo:
    host: str
    port: int
    subject: str
    issuer: str
    not_before: datetime
    not_after: datetime
    dns_names: tuple[str, ...]
    pin: str
    fingerprint: str
    hostname_ok: bool


def describe(der: bytes, host: str, port: int) -> CertInfo:
    cert = x509.load_der_x509_certificate(der)
    try:
        san = cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value
        assert isinstance(san, x509.SubjectAlternativeName)
        names = tuple(san.get_values_for_type(x509.DNSName))
    except x509.ExtensionNotFound:
        names = ()
    return CertInfo(
        host=host,
        port=port,
        subject=cert.subject.rfc4514_string(),
        issuer=cert.issuer.rfc4514_string(),
        not_before=cert.not_valid_before_utc,
        not_after=cert.not_valid_after_utc,
        dns_names=names,
        pin=spki_pin(der),
        fingerprint=cert_fingerprint(der),
        hostname_ok=hostname_matches(der, host),
    )


def fetch_leaf(
    host: str, port: int = 443, timeout_s: float = 10.0, connect: str | None = None
) -> bytes:
    """TLS handshake only, without trusting the chain, and return the leaf certificate (DER).

    Nothing is sent after the handshake (no HTTP request, so no token can leave), and the
    result must not be trusted by itself: compare the fingerprint with an independent view
    (the browser) before putting the pin in config. ``connect`` overrides the address dialled.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with (
        socket.create_connection((connect or host, port), timeout=timeout_s) as raw,
        ctx.wrap_socket(raw, server_hostname=host) as tls,
    ):
        der = tls.getpeercert(binary_form=True)
    if der is None:
        raise ssl.SSLError(f"{host} sent no certificate")
    return der
