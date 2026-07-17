from __future__ import annotations

import base64
import hashlib
import json
import ssl
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from saveweb_hq import TransportError
from saveweb_hq.transport import JSONTransport


class JSONHandler(BaseHTTPRequestHandler):
    def log_message(self, *_: object) -> None:
        pass

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API.
        encoded = json.dumps({"ok": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)


def create_certificate(directory: Path) -> tuple[Path, Path, str]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(__import__("ipaddress").ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    key_path, cert_path = directory / "server.key", directory / "server.crt"
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    spki = key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    pin = base64.urlsafe_b64encode(hashlib.sha256(spki).digest()).rstrip(b"=").decode()
    return key_path, cert_path, pin


def test_pinned_https_transport(tmp_path: Path) -> None:
    key_path, cert_path, pin = create_certificate(tmp_path)
    server = HTTPServer(("127.0.0.1", 0), JSONHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        transport = JSONTransport(
            f"https://127.0.0.1:{server.server_port}",
            timeout=5,
            allow_http=False,
            spki_pin=pin,
        )
        assert transport.request_json("POST", "/test", {}, {}) == {"ok": True}
        transport.close()

        wrong_pin = (
            base64.urlsafe_b64encode(hashlib.sha256(b"wrong").digest()).rstrip(b"=").decode()
        )
        bad_transport = JSONTransport(
            f"https://127.0.0.1:{server.server_port}",
            timeout=5,
            allow_http=False,
            spki_pin=wrong_pin,
        )
        with pytest.raises(TransportError):
            bad_transport.request_json("POST", "/test", {}, {})
        bad_transport.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_transport_rejects_invalid_endpoint_port() -> None:
    with pytest.raises(TransportError, match="port"):
        JSONTransport(
            "https://example.invalid:not-a-port",
            timeout=1,
            allow_http=False,
        )
