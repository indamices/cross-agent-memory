"""MS-C 一次性 backfill: 把 chunks 表 42万向量灌入 vec0 虚表 vec_chunks.

真相源 = chunks 表; vec_chunks = 派生索引 (可随时本脚本重建).
语义: 按 collection+layer 过滤 + cosine KNN, 取代 Python 整矩阵物化.

用法:
    python3 scripts/migrate_to_vec0.py              # 首次灌入(若 vec_chunks 非空会报错退出)
    python3 scripts/migrate_to_vec0.py --rebuild    # DROP + 重建(派生索引可随意重建)
    python3 scripts/migrate_to_vec0.py --verify     # 只做对账不灌
"""
import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np

try:
    import sqlite_vec
except ImportError:
    print("ERROR: sqlite_vec 未安装. 请先装 sqlite-vec 0.1.9", file=sys.stderr)
    sys.exit(2)

SERVICE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("MSC_STAGING_DB", SERVICE_DIR / "memory.db"))
BATCH = 1000

_GOLD_TOPLEVEL_FILES = frozenset({
    "AGENTS.md", "BOOTSTRAP.md", "DREAMS.md", "KNOWLEDGE.md", "LESSONS.md",
    "MEMORY.md", "METHODOLOGY.md", "SOUL.md", "STANDING-ORDERS.md",
    "TOOLS.md", "USER.md", "WORKFLOW.md", "tools.md",
})


def classify_layer(source: str) -> str:
    if not source:
        return "session"
    s = source
    bname = s.rsplit("/", 1)[-1]
    if "/memory/details/" in s or "/memory/inbox/" in s:
        return "gold"
    if "/decisions/" in s:
        return "gold"
    if "/docs/plans/reviews/" in s:
        return "gold"
    if "/memory/" in s:
        after = s.split("/memory/", 1)[1]
        if "/" not in after:
            return "gold"
    if bname in _GOLD_TOPLEVEL_FILES:
        return "gold"
    if "/agent_team/" in s:
        return "task"
    return "session"


def resolve_layer(metadata_json: str, source: str) -> str:
    """metadata.layer 优先, 缺失则 classify_layer(source). 与 db.py search 同口径."""
    if metadata_json:
        try:
            m = json.loads(metadata_json)
            ml = m.get("layer")
            if ml:
                return ml
        except Exception:
            pass
    return classify_layer(source)


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    return conn


def table_exists(conn, name):
    r = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return r is not None


def create_vec_table(conn):
    # ADR-029 (2026-09-06): DDL 收敛 vec_ddl 单一真相源 (partition 开关 MEMSVC_VEC_PARTITION)
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from vec_ddl import legacy_vec_ddl
    conn.execute(legacy_vec_ddl())
    conn.commit()


def drop_vec_table(conn):
    conn.execute("DROP TABLE IF EXISTS vec_chunks")
    conn.commit()


def count_vec(conn):
    try:
        return conn.execute("SELECT COUNT(*) FROM vec_chunks").fetchone()[0]
    except Exception:
        return -1


def backfill(progress_every=5000):
    conn = get_conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL AND length(embedding)>0").fetchone()[0]
        print(f"[backfill] chunks total = {total}")

        existing = count_vec(conn)
        if existing > 0:
            print(f"[backfill] vec_chunks 已有 {existing} 行. 用 --rebuild 重建, 或先 DROP.")
            return 1

        create_vec_table(conn)
        t0 = time.time()
        done = 0
        last_report = 0

        # 流式读: 避免 fetchall 把 42万 embedding 全载入内存 (那正是要消灭的!)
        cur = conn.execute(
            "SELECT id, collection, embedding, metadata, source "
            "FROM chunks WHERE embedding IS NOT NULL AND length(embedding)>0 "
            "ORDER BY id"
        )

        batch = []
        batch_vec = []
        for rid, coll, blob, meta, source in cur:
            # 防御: 跳过异常 dim (db.py _compute_collection_arrays 同款防御)
            arr = np.frombuffer(blob, dtype=np.float32)
            if arr.shape[0] != 1024:
                continue
            layer = resolve_layer(meta, source)
            batch_vec.append((rid, blob, coll, layer))
            if len(batch_vec) >= BATCH:
                _flush_batch(conn, batch_vec)
                done += len(batch_vec)
                batch_vec = []
                if done - last_report >= progress_every:
                    rate = done / max(time.time() - t0, 0.001)
                    print(f"[backfill] {done}/{total} ({100*done/total:.1f}%) {rate:.0f} rows/s")
                    last_report = done

        if batch_vec:
            _flush_batch(conn, batch_vec)
            done += len(batch_vec)

        elapsed = time.time() - t0
        print(f"[backfill] DONE. {done} rows in {elapsed:.1f}s ({done/max(elapsed,0.001):.0f} rows/s)")
        _verify_counts(conn)
        return 0
    finally:
        conn.close()


def _flush_batch(conn, batch_vec):
    conn.executemany(
        "INSERT INTO vec_chunks(chunk_id, embedding, collection, layer) VALUES (?,?,?,?)",
        batch_vec,
    )
    conn.commit()


def _verify_counts(conn):
    by_coll = conn.execute(
        "SELECT collection, COUNT(*) FROM vec_chunks GROUP BY collection ORDER BY COUNT(*) DESC"
    ).fetchall()
    print("[verify] vec_chunks by collection:")
    for c, n in by_coll:
        print(f"  {c:30s} {n:>8d}")
    # gold 分布
    by_layer = conn.execute(
        "SELECT layer, COUNT(*) FROM vec_chunks GROUP BY layer"
    ).fetchall()
    print("[verify] vec_chunks by layer:")
    for l, n in by_layer:
        print(f"  {l:10s} {n:>8d}")


def verify_only():
    """对账: vec_chunks 行数 vs chunks 行数, 抽样距离对账."""
    conn = get_conn()
    try:
        n_chunks = conn.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL AND length(embedding)>0").fetchone()[0]
        n_vec = count_vec(conn)
        print(f"[verify] chunks={n_chunks} vec_chunks={n_vec} diff={n_chunks - n_vec}")

        # 抽 20 条, numpy cosine vs vec0 cosine 对账
        import random
        rows = conn.execute(
            "SELECT id, collection, embedding FROM chunks "
            "WHERE embedding IS NOT NULL ORDER BY RANDOM() LIMIT 20"
        ).fetchall()
        max_diff = 0.0
        for rid, coll, blob in rows:
            arr = np.frombuffer(blob, dtype=np.float32)
            if arr.shape[0] != 1024:
                continue
            arr = arr / (np.linalg.norm(arr) + 1e-10)
            # vec0 自查 (自身 embedding MATCH 自身)
            r = conn.execute(
                "SELECT distance FROM vec_chunks WHERE chunk_id=? AND embedding MATCH ? ORDER BY distance LIMIT 1",
                (rid, arr.tobytes()),
            ).fetchone()
            if r is None:
                print(f"  WARN {rid} 不在 vec_chunks")
                continue
            # numpy 自己跟自己 cos_dist=0, vec0 也应~0
            diff = abs(r[0] - 0.0)
            max_diff = max(max_diff, diff)
        print(f"[verify] 20-sample self-match max |distance| = {max_diff:.6f} (应~0)")
        return 0
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true", help="DROP + 重建")
    ap.add_argument("--verify", action="store_true", help="只对账不灌")
    args = ap.parse_args()

    if args.verify:
        sys.exit(verify_only())

    if args.rebuild:
        conn = get_conn()
        try:
            print("[rebuild] DROP vec_chunks ...")
            drop_vec_table(conn)
        finally:
            conn.close()

    sys.exit(backfill())


if __name__ == "__main__":
    main()
