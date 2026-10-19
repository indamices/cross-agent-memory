#!/usr/bin/env python3
"""One-off backfill: fill ``chunks.text_sha256`` for legacy rows.

Rows written before the dedup column existed have ``text_sha256 IS NULL`` and
are invisible to content-hash deduplication. This script backfills them in
batches (safe to stop and re-run; already-filled rows are skipped).

Usage:
    MEMSVC_DB=/path/to/memory.db python3 scripts/ops/memsvc_backfill_sha256.py [--batch 5000]

Notes:
  - Uses ``db._text_sha256`` so the hash normalization matches the write path.
  - Intentionally NOT part of the request path: backfilling 100k+ rows holds
    the write lock for a while. Run it during a quiet window.
  - If the cold split is active, run this against the main database only;
    cold rows are backfilled by their own migration.
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from db import _get_conn, _text_sha256  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=int, default=5000,
                        help="rows per transaction (default 5000)")
    args = parser.parse_args()

    conn = _get_conn()
    total = conn.execute(
        "SELECT COUNT(*) FROM chunks WHERE text_sha256 IS NULL").fetchone()[0]
    if not total:
        print("nothing to backfill (0 rows with NULL text_sha256)")
        return 0

    print(f"backfilling {total} rows (batch={args.batch}) ...")
    done = 0
    t0 = time.time()
    while True:
        rows = conn.execute(
            "SELECT id, text FROM chunks WHERE text_sha256 IS NULL LIMIT ?",
            (args.batch,)).fetchall()
        if not rows:
            break
        conn.executemany(
            "UPDATE chunks SET text_sha256 = ? WHERE id = ?",
            [(_text_sha256(text), chunk_id) for chunk_id, text in rows])
        conn.commit()
        done += len(rows)
        print(f"  {done}/{total}")
    conn.close()
    print(f"done: {done} rows in {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
