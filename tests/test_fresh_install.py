#!/usr/bin/env python3
"""Hermetic tests for a fresh install: schema bootstrap, write, search, dedup.

No network and no Ollama required: embeddings are supplied directly (the
vector KNN path takes a query embedding as a parameter). Runs against a
throwaway database created in a temp directory.

Run: python3 tests/test_fresh_install.py
 or: python3 -m unittest discover -s tests -p "test_fresh_install.py"
"""

import math
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_TMP = tempfile.mkdtemp(prefix="cam-test-fresh-")
os.environ["MEMSVC_DB"] = str(Path(_TMP) / "memory.db")

import db  # noqa: E402

DIM = 1024


def _emb(seed: float) -> list[float]:
    """Deterministic pseudo-embedding (no model needed)."""
    return [math.sin(seed * (i + 1) / 17.0) for i in range(DIM)]


class TestFreshInstall(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(_TMP, ignore_errors=True)

    def test_01_schema_bootstrap(self):
        conn = db._get_conn()
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            conn.close()
        self.assertIn("chunks", tables)
        self.assertIn("collections", tables)

    def test_02_write_and_search(self):
        coll = "demo"
        db.get_or_create_collection(coll)
        chunks = [
            {
                "id": "c1",
                "text": "The service stores vectors in SQLite for local-first operation.",
                "embedding": _emb(1.0),
                "metadata": {"source": "unit-test"},
            },
            {
                "id": "c2",
                "text": "Tiered storage moves old chunks to a cold database.",
                "embedding": _emb(50.0),
                "metadata": {"source": "unit-test"},
            },
        ]
        db.upsert_chunks(coll, chunks)
        self.assertEqual(db.count_collection_chunks(coll), 2)

        results = db.search(coll, "local-first storage", _emb(1.0), n_results=2)
        self.assertTrue(results, "search must return results")
        self.assertEqual(results[0]["id"], "c1", "nearest chunk must rank first")
        self.assertIn("text", results[0])

    def test_03_dedup_skips_identical_text(self):
        coll = "demo"
        before = db.count_collection_chunks(coll)
        db.upsert_chunks(coll, [{
            "id": "c1-copy",
            "text": "The service stores vectors in SQLite for local-first operation.",
            "embedding": _emb(1.0),
            "metadata": {"source": "unit-test"},
        }])
        self.assertEqual(db.count_collection_chunks(coll), before,
                          "identical text must be deduplicated")

    def test_04_delete_by_ids(self):
        coll = "demo"
        db.delete_chunks_by_ids(["c2"], coll)
        self.assertEqual(db.count_collection_chunks(coll), 1)
        results = db.search(coll, "tiered storage", _emb(50.0), n_results=5)
        self.assertTrue(all(r["id"] != "c2" for r in results))

    def test_05_stats_shape(self):
        stats = db.get_stats()
        self.assertIn("collections", stats)
        names = {c["name"] for c in stats["collections"]}
        self.assertIn("demo", names)


if __name__ == "__main__":
    unittest.main(verbosity=2)
