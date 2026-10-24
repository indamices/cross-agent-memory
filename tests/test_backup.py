#!/usr/bin/env python3
"""Hermetic tests for backup & restore safety and consistency.

Covers:
- restore rejects malformed / traversal names before touching the filesystem
- backup produces a consistent snapshot (SQLite backup API), even under WAL
- a valid round-trip restores the previous state

Runs in a child process with its own environment so it can never interfere
with other test modules' database (single-process unittest discovery).

Run: python3 tests/test_backup.py
"""

import subprocess
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

CHILD = r"""
import math
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.environ["CAM_REPO"])

# config.py binds SQLITE_DB at IMPORT time — the scratch paths must be set
# before importing db/backup, otherwise the child would operate on the repo's
# own database (hard-learned via review round 2).
_TMP = tempfile.mkdtemp(prefix="cam-test-backup-")
os.environ["MEMSVC_DATA_DIR"] = _TMP
os.environ["MEMSVC_DB"] = os.path.join(_TMP, "memory.db")
os.environ["MEMSVC_COLD_DB"] = os.path.join(_TMP, "cold.db")
os.environ["MEMSVC_BACKUP_DIR"] = os.path.join(_TMP, "backups")
os.environ["MEMSVC_SNAPSHOT_DIR"] = os.path.join(_TMP, "api_snapshots")
os.environ["MEMSVC_PROJECTS_DIR"] = os.path.join(_TMP, "projects")
os.environ["MEMORY_MISS_LEDGER"] = os.path.join(_TMP, "miss.jsonl")
os.environ["MEMORY_HITS_LEDGER"] = os.path.join(_TMP, "hits.jsonl")
os.environ["MEMIDX_METRIC_LOG"] = os.path.join(_TMP, "index.jsonl")

import os
os.environ["MEMSVC_HASH_CACHE"] = "0"
import config
import db
import backup
import server
import indexer
from pathlib import Path

root = Path(_TMP).resolve()

path_bindings = {
    "config.DATA_DIR": (config.DATA_DIR, root),
    "config.SQLITE_DB": (config.SQLITE_DB, root / "memory.db"),
    "db.SQLITE_DB": (db.SQLITE_DB, root / "memory.db"),
    "config.PROJECTS_DIR": (config.PROJECTS_DIR, root / "projects"),
    "db.COLD_DB_PATH": (db.COLD_DB_PATH, root / "cold.db"),
    "backup._BACKUP_ROOT": (backup._BACKUP_ROOT, root / "backups"),
    "backup.get_api_snapshot_dir()": (backup.get_api_snapshot_dir(), root / "api_snapshots"),
    "server.MEMORY_MISS_LEDGER": (server.MEMORY_MISS_LEDGER, root / "miss.jsonl"),
    "server.MEMORY_HITS_LEDGER": (server.MEMORY_HITS_LEDGER, root / "hits.jsonl"),
    "indexer._MEMIDX_METRIC_LOG": (indexer._MEMIDX_METRIC_LOG, root / "index.jsonl"),
}

for label, (actual, expected) in path_bindings.items():
    actual_r = Path(actual).resolve()
    expected_r = Path(expected).resolve()
    assert actual_r == expected_r, f"{label}: {actual_r} != {expected_r}"
    assert actual_r.is_relative_to(root), f"{label}: {actual_r} not under {root}"

DIM = 1024


def _emb(seed: float) -> list[float]:
    return [math.sin(seed * (i + 1) / 17.0) for i in range(DIM)]


class TestRestoreGuards(unittest.TestCase):
    def test_rejects_traversal_and_absolute(self):
        for bad in ["..", "../x", "/etc", "../../etc", "a/b", "", "x" * 300,
                    "2026-10-19", "2026-10-19_0940", "2026-10-19_094000/../evil"]:
            r = backup.restore_sqlite(bad)
            assert r.get("status") == "error", f"must reject {bad!r}: {r}"
            assert "Invalid backup name" in r.get("error", ""), r

    def test_accepts_wellformed_but_missing(self):
        r = backup.restore_sqlite("2020-01-01_000000")
        assert r.get("status") == "error", r
        assert "not found" in r.get("error", ""), r


class TestBackupRoundTrip(unittest.TestCase):
    def test_wal_snapshot_and_restore(self):
        coll = "bk"
        db.get_or_create_collection(coll)
        db.upsert_chunks(coll, [{
            "id": "k1", "text": "before backup",
            "embedding": _emb(1.0), "metadata": {"source": "t"},
        }])

        r = backup.backup_sqlite()
        assert r.get("status") == "ok", r
        name = r["backup_name"]

        db.delete_chunks_by_ids(["k1"], coll)
        db.upsert_chunks(coll, [{
            "id": "k2", "text": "after backup",
            "embedding": _emb(2.0), "metadata": {"source": "t"},
        }])
        assert db.count_collection_chunks(coll) == 1

        r2 = backup.restore_sqlite(name)
        assert r2.get("status") == "ok", r2

        conn = db._get_conn()
        try:
            ids = {row[0] for row in conn.execute(
                "SELECT id FROM chunks WHERE collection=?", (coll,))}
        finally:
            conn.close()
        assert ids == {"k1"}, f"restored database must hold pre-backup state, got {ids}"


if __name__ == "__main__":
    try:
        unittest.main(verbosity=2)
    finally:
        shutil.rmtree(_TMP, ignore_errors=True)
"""


class TestBackupSubprocess(unittest.TestCase):
    def test_backup_and_restore_isolated(self):
        import os
        env = dict(os.environ)
        env["CAM_REPO"] = str(REPO)
        # Deliberately do NOT inherit a database path from the parent runner.
        env.pop("MEMSVC_DB", None)
        env.pop("MEMSVC_BACKUP_DIR", None)
        r = subprocess.run(
            [sys.executable, "-c", CHILD],
            cwd=str(REPO), capture_output=True, text=True, timeout=300, env=env,
        )
        out = (r.stdout or "") + (r.stderr or "")
        self.assertEqual(r.returncode, 0, "child failed:\n" + out[-4000:])
        self.assertIn("Ran 3 tests", out)
        self.assertIn("OK", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
