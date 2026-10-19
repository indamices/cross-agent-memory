import sqlite3

import numpy as np
import pytest

from config import SECURITY


def test_idempotent_initialization_and_clone_search(tmp_path, monkeypatch):
    import config
    import db
    # Point every resolver at a throwaway DB: standalone runs must never
    # touch the real service database. _schema_ready is a once-per-process
    # flag (db may already have bootstrapped during import or an earlier
    # test), so reset it to mirror a genuinely fresh process.
    fresh = tmp_path / "fresh.db"
    monkeypatch.setattr(config, "SQLITE_DB", fresh)
    monkeypatch.setattr(db, "SQLITE_DB", fresh)
    # Reset every once-per-process bootstrap flag (base schema, dedup column,
    # vec0 init): earlier tests in the same run may have set them against a
    # different database file.
    monkeypatch.setattr(db, "_schema_ready", False)
    monkeypatch.setattr(db, "_dedup_schema_ready", False)
    monkeypatch.setattr(db, "_vec_initialized", False)
    monkeypatch.setattr(db, "_vec_usable", None)
    monkeypatch.setenv("MEMSVC_DB", str(fresh))
    coll = db.get_or_create_collection("release-fixture")
    assert db.get_or_create_collection("release-fixture") == coll  # bootstrap is idempotent
    vector = np.zeros(1024); vector[0] = 1
    db.upsert_chunks(coll, [{"id": "decision-1", "text": "Use SQLite for persistent memory",
                            "embedding": vector.tolist(), "metadata": {"source": "decision.md"}}])
    clone = tmp_path / "clone.db"
    with sqlite3.connect(fresh) as src, sqlite3.connect(clone) as dst:
        src.backup(dst)
    monkeypatch.setattr(db, "SQLITE_DB", clone)
    monkeypatch.setenv("MEMSVC_DB", str(clone))
    before = clone.read_bytes()
    hits = db.search(coll, "SQLite", vector.tolist(), n_results=1)
    assert hits and hits[0]["id"] == "decision-1"
    assert clone.read_bytes() == before
    with sqlite3.connect(clone) as conn:
        assert conn.execute("select count(*) from chunks").fetchone()[0] == 1


def request(raw):
    """Exercise the real HTTP parser and handler using an in-memory transport."""
    import io
    from server import _Handler
    class Transport:
        def __init__(self):
            self.input = io.BytesIO(raw)
            self.output = io.BytesIO()
        def settimeout(self, seconds):
            assert seconds == 10
        def makefile(self, mode, *args):
            return self.input
        def sendall(self, data):
            self.output.write(data)
    transport = Transport()
    _Handler(transport, ("127.0.0.1", 1), object())
    return int(transport.output.getvalue().split(b" ")[1])


def test_auth_before_body(monkeypatch):
    monkeypatch.setattr(SECURITY, "enabled", True)
    monkeypatch.setattr(SECURITY, "api_token", "test-secret")
    assert request(b"POST /api/reindex HTTP/1.0\r\nContent-Length: 999999999\r\n\r\n") == 401


@pytest.mark.parametrize("length,body,status", [("1048577", b"", 413), ("-1", b"", 400),
    ("bad", b"", 400), ("2", b"[]", 400), ("1", b"\xff", 400)])
def test_request_limits(monkeypatch, length, body, status):
    monkeypatch.setattr(SECURITY, "enabled", False)
    assert request(f"POST /api/search HTTP/1.0\r\nContent-Length: {length}\r\n\r\n".encode() + body) == status


def test_strict_bearer_and_protected_reads(monkeypatch):
    monkeypatch.setattr(SECURITY, "enabled", True)
    monkeypatch.setattr(SECURITY, "api_token", "test-secret")
    monkeypatch.setattr(SECURITY, "read_requires_token", True)
    for token, status in [("xBearer test-secret", 401), ("Bearer test-secret", 200)]:
        assert request(f"GET /api/projects HTTP/1.0\r\nAuthorization: {token}\r\n\r\n".encode()) == status


@pytest.mark.parametrize("headers", [b"Content-Length: 0\r\nContent-Length: 0",
    b"Content-Length: 0\r\nTransfer-Encoding: chunked", b"Host: localhost"])
def test_ambiguous_framing_rejected(monkeypatch, headers):
    monkeypatch.setattr(SECURITY, "enabled", False)
    assert request(b"POST /api/search HTTP/1.0\r\n" + headers + b"\r\n\r\n") == 400
