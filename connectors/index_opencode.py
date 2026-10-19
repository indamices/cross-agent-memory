"""Embedding-only indexer for opencode.db → SQLite memory.db.

Reads ~/.local/share/opencode/opencode.db, extracts user/assistant
text, chunks, embeds, and stores in memory.db. Does NOT export markdown.

Typically run from cron to keep the embedding index fresh.
"""

import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import _env_num
from db import (
    get_or_create_collection,
    upsert_chunks,
    delete_session_chunks,
    get_existing_embeddings,
    _text_sha256,
    _get_conn,
    _cold_split_active,
    _get_cold_conn,
)
from embedder import embed_batch

logger = logging.getLogger("memory-service.index-embed")

OPENCODE_DB = Path(os.environ.get(
    "OPENCODE_DB", Path.home() / ".local" / "share" / "opencode" / "opencode.db"))
COLLECTION_NAME = "opencode_sessions"
# T-MEMSVC-WALK-SPEEDUP P3: per-run chunk 上限 (env 可调; 治单轮 92% CPU 峰值, 常态日增 ~3k 不受限)
_MAX_CHUNKS = int(_env_num("MEMSVC_EMBED_MAX_CHUNKS", 2000))  # flash 执行审次要: 非法 env 防御
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100

# T-EMBED-INCREMENTAL (2026-07-24): 增量水位 — 只处理 time_updated > 上次水位的 session,
# 热启动(无新 session)秒退, 不再每次全量重扫。冷启动(无水位文件)=0=全量首次。
# 单位=ms(与 DB session.time_updated 一致)。水位=本轮处理过的最大 time_updated(非 now, 防时钟偏差漏边界)。
_LAST_RUN = Path(__file__).parent / ".last_run_opencode"


def _read_last_run() -> float:
    try:
        return float(_LAST_RUN.read_text().strip())
    except (OSError, ValueError):
        return 0.0  # 冷启/损坏 → 全量


def _write_last_run(watermark: float) -> None:
    try:
        tmp = _LAST_RUN.with_name(_LAST_RUN.name + ".tmp")
        tmp.write_text(str(watermark))
        tmp.replace(_LAST_RUN)  # 原子
    except OSError as e:
        logger.warning("水位写入失败(不阻塞): %s", e)


# 2026-06-11 审计A3: 切块统一走 chunking.py 单一实现 (与 watcher 同源)
from chunking import chunk_text as _unified_chunk_text, chunk_hash as _chunk_hash, CHUNKER_VERSION


def _chunk_text(text: str) -> list[str]:
    return _unified_chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP)


def _get_indexed_versions(collection_name: str) -> set[str]:
    conn = _get_conn()
    # T-INDEXER-AXIS-FIX-0831 A3: 旧 LIKE '%"source_type": "opencode_db"%' (带空格)
    # 匹配不到 1,230 个老无空格 JSON 格式 session(68,444 chunks) → version 检测不到,
    # 每次版本漂移整段重嵌。改 json_extract 语义检测, 新旧格式通吃。
    rows = conn.execute(
        "SELECT metadata FROM chunks WHERE collection = ? AND json_extract(metadata, '$.source_type') = ?",
        (collection_name, "opencode_db"),
    ).fetchall()
    conn.close()
    versions = set()
    for (meta_str,) in rows:
        try:
            meta = json.loads(meta_str)
            v = meta.get("session_version", "")
            if v:
                versions.add(v)
        except (json.JSONDecodeError, TypeError):
            continue
    # T-INDEXER-HOLD-D123 D2 同族防御: 冷分库 active 后冷行 version 真相在冷库 —
    # 本侧水位常态推进被掩盖, 但水位一旦归零/回退即触发同款全价重做 (与 _db 侧同根)。
    try:
        if _cold_split_active():
            cconn = _get_cold_conn()
            try:
                crows = cconn.execute(
                    "SELECT metadata FROM chunks WHERE collection = ? AND json_extract(metadata, '$.source_type') = ?",
                    (collection_name, "opencode_db"),
                ).fetchall()
            finally:
                cconn.close()
            for (meta_str,) in crows:
                try:
                    meta = json.loads(meta_str)
                    v = meta.get("session_version", "")
                    if v:
                        versions.add(v)
                except (json.JSONDecodeError, TypeError):
                    continue
    except Exception as e:
        logger.warning("冷库 version 并入失败 (方向=重做不丢数据, 不阻塞): %s", e)
    return versions


# T-DBSPLIT-STAGE1-READSIDE-0830: 多库化 — 支持全局单库 + dbs/*.db 多库布局。
# 水位时间基(ms)跨库成立: 一轮内集齐全部库再统一推水位, 禁逐库独立调用(会互相饿死)。
def _discover_dbs():
    """Discover OpenCode SQLite databases.

    Resolution order:
      1. OPENCODE_DBS env — os.pathsep-separated list of db paths
      2. ~/.local/share/opencode/opencode.db — single global db
      3. ~/.local/share/opencode/dbs/*.db — multi-db layout (if present)
    """
    env = os.environ.get("OPENCODE_DBS", "").strip()
    if env:
        return [Path(p) for p in env.split(os.pathsep) if p]
    base = Path.home() / ".local" / "share" / "opencode"
    dbs = []
    global_db = base / "opencode.db"
    if global_db.exists():
        dbs.append(global_db)
    dbs_dir = base / "dbs"
    if dbs_dir.is_dir():
        dbs.extend(sorted(dbs_dir.glob("*.db")))
    return dbs or [global_db]


def index_embeddings_all(db_list=None, force: bool = False) -> dict:
    """T-DBSPLIT-STAGE1: 多库入口 — 一个水位信封内处理全部库。
    db_list=None → 单点库发现。单库时行为与旧 index_embeddings(db) 逐字段一致。"""
    if db_list is None:
        db_list = _discover_dbs()
    missing = [str(p) for p in db_list if not p.exists()]
    if missing:
        return {"status": "error", "error": f"DB not found: {', '.join(missing)}"}

    last_run = 0.0 if force else _read_last_run()  # ms; 0=冷启全量

    # T-EMBED-INCREMENTAL: 只取 time_updated > 上次水位 (冷启 last_run=0 → 全量)
    per_db = []
    for db_path in db_list:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        # T-INDEXER-AXIS-FIX-0831 A1: 排序轴与水位轴同列。旧 ORDER BY time_created DESC
        # (新者优先)下, cap 提前终止后水位按 time_updated 推进会越过排在后面、
        # time_updated 更小的未访问 session → 永久搁浅(存量 892 个即此沉没)。
        # 升序后未访问行 time_updated 必 ≥ 已访问行, 水位永不越过未处理者。
        rows = conn.execute("""
            SELECT s.id, s.title, s.directory, s.time_updated
            FROM session s
            WHERE s.time_updated > ?
              AND (SELECT count(*) FROM part p WHERE p.session_id = s.id
                   AND p.data LIKE '%"type":"text"%') > 0
            ORDER BY s.time_updated ASC
        """, (last_run,)).fetchall()
        if rows:
            per_db.append((conn, rows))
        else:
            conn.close()
    total_rows = sum(len(rows) for _conn, rows in per_db)

    # 热启动: 无新 session → 秒退 (不 fetch indexed_versions, 不 embed, 不动水位)
    if not per_db:
        return {"status": "ok", "total_sessions": 0, "indexed_sessions": 0,
                "skipped_sessions": 0, "total_chunks": 0, "errors": 0,
                "incremental": True, "last_run": last_run}

    collection_name = get_or_create_collection(COLLECTION_NAME)
    indexed_versions = set() if force else _get_indexed_versions(collection_name)

    total_chunks = 0
    errors = 0
    skipped = 0
    indexed_sessions = 0
    max_watermark = last_run  # 追踪本轮见到的最大 time_updated
    failed_mtimes = []  # 有内容但未入库(embed失败)的 time_updated; 水位须卡在最早失败前保证下轮重试

    # T-DBSPLIT-STAGE1: 展平多库行(库序+库内序保持); conn 跟行走, cap-break 断全部库
    flat = [(conn, row) for conn, rows in per_db for row in rows]
    cap_break_at = None  # T-INDEXER-AXIS-FIX-0831 A2: cap 提前终止时的未访问区间起点(含)
    for pos, (conn, row) in enumerate(flat):
        session_id = row["id"]
        title = row["title"] or "(untitled)"
        directory = row["directory"]
        time_updated = row["time_updated"]
        max_watermark = max(max_watermark, time_updated)  # 每行都算(含skip/no-text), 防下轮重查
        version = f"{session_id}:{time_updated}"

        if version in indexed_versions and not force:
            skipped += 1
            continue

        parts = conn.execute("""
            SELECT p.data
            FROM part p
            WHERE p.session_id = ?
              AND (p.data LIKE '%"type":"text"%' OR p.data LIKE '%"type":"reasoning"%')
            ORDER BY p.time_created ASC
        """, (session_id,)).fetchall()

        texts = []
        for p in parts:
            try:
                data = json.loads(p["data"])
                if data.get("type") in ("text", "reasoning"):
                    text = data.get("text", "").strip()
                    if text and len(text) > 5:
                        texts.append(text)
            except (json.JSONDecodeError, TypeError):
                continue

        if not texts:
            continue

        full_text = "\n\n".join(texts)
        chunks = _chunk_text(full_text)
        if not chunks:
            continue

        chunk_dicts = []
        for i, chunk in enumerate(chunks):
            chunk_id = f"ocdb_{session_id[:16]}_{i}"
            chunk_dicts.append({
                "id": chunk_id,
                "text": chunk,
                "embedding": None,
                "metadata": {
                    "source_type": "opencode_db",
                    # 2026-06-11 审计A3: 补 source/chunk_hash — 此前本 collection
                    # 14338 条 100% 无 hash 无 source (不可去重不可溯源)
                    "source": f"opencode_db:{session_id}",
                    "chunk_hash": _chunk_hash(chunk),
                    "chunker_version": CHUNKER_VERSION,
                    "session_id": session_id,
                    "session_title": title,
                    "session_directory": directory,
                    "session_version": version,
                    "chunk_index": i,
                    "chunk_total": len(chunks),
                    "file_mtime": time_updated / 1000,
                },
            })

        # T-MEMINDEX-STEP2 (2026-08-06, maintainer-approved plan B): 会话级删旧重插 + hash 复用向量
        # 消灭混合版本孤儿。顺序 (flash 二审 D1 认可): embed 失败 abort, 旧块完整保留。
        hashes = [_text_sha256(c["text"]) for c in chunk_dicts]
        existing_embs = get_existing_embeddings(collection_name, hashes)
        embed_reused = 0
        for c, h in zip(chunk_dicts, hashes):
            if h in existing_embs:
                blob = existing_embs[h]
                # D3: 显式归一为 bytes 再 .copy() 切视图 (db.py:1344 范式)
                c["embedding"] = np.frombuffer(bytes(blob), dtype=np.float32).copy()
                embed_reused += 1

        need_embed = [c for c in chunk_dicts if c["embedding"] is None]
        if need_embed:
            embeddings = embed_batch([c["text"] for c in need_embed])
            for c, emb in zip(need_embed, embeddings):
                if emb is not None:
                    c["embedding"] = emb

        valid = [c for c in chunk_dicts if c["embedding"] is not None]
        # 失败保护 (D1): 部分失败 → 整 session 不 delete 不 upsert, 旧块原样保留
        # 对齐原 embed.py:181-188 防 silent 丢块; STEP2 进一步: 失败时不删旧块 (D1 修订)
        if len(valid) < len(chunk_dicts):
            failed_mtimes.append(time_updated)
            if len(valid) > 0:
                errors += 1
            logger.warning(
                "STEP2 embed 失败 sid=%s: valid=%d/%d, 旧块保留不删 (D1 非破坏)",
                session_id[:16], len(valid), len(chunk_dicts),
            )
            continue

        if valid:
            upsert_chunks(collection_name, valid, skip_dedup=True)
            total_chunks += len(valid)
            indexed_sessions += 1
            # T-MEMSVC-WALK-SPEEDUP P3 (flash D4/CC GateA): per-run chunk 上限, 文件级原子 —
            # 只在 session 完整处理后检查 (embed 失败 abort 优先, 不截断半 session);
            # watermark 停在最后完整 session (失败回退语义不变), 剩余下轮 4h 网格接力。
            if total_chunks >= _MAX_CHUNKS:
                logger.info(
                    "P3 chunk cap reached (%d >= %d), stop; remaining sessions deferred to next run",
                    total_chunks, _MAX_CHUNKS,
                )
                cap_break_at = pos + 1
                break
        if embed_reused:
            logger.info(
                "STEP2 sid=%s 复用 %d/%d 块 embedding (跨 session hash, 不调 ollama)",
                session_id[:16], embed_reused, len(chunk_dicts),
            )

        # 删孤儿: 该 session 不在 new id 集的旧 chunk
        new_ids = {c["id"] for c in valid}
        orphans = delete_session_chunks(
            collection_name, session_id, except_ids=new_ids
        )
        if orphans:
            logger.info(
                "STEP2 sid=%s 删孤儿 %d 块 (旧版本残留, 不在新 chunks 集内)",
                session_id[:16], orphans,
            )

    for conn, _rows in per_db:
        conn.close()

    # 推进水位 — T-INDEXER-AXIS-FIX-0831 A2: 三个边界都不得越过, 取最小:
    #   embed 失败(已访问) → min(failed)-1 保证下轮重试;
    #   cap 提前终止(未访问) → 显式取 min(未访问行 time_updated)-1, 不依赖排序假设
    #     (与 A1 的 ASC 排序双保险: 同值 tie 边界也不会被 `>` 过滤吞掉);
    #   正常完成 → max_watermark。已入库者下轮靠 version 跳过, 廉价。
    # 无条件写(force 也推进)。
    wm_bounds = []
    if failed_mtimes:
        wm_bounds.append(min(failed_mtimes) - 1)
    if cap_break_at is not None and cap_break_at < len(flat):
        # 边界: cap 恰在最后一行触发时无未访问行, 语义=全部处理完 → 不加界, 走 max_watermark
        wm_bounds.append(min(r["time_updated"] for _c, r in flat[cap_break_at:]) - 1)
    _write_last_run(min(wm_bounds) if wm_bounds else max_watermark)

    return {
        "status": "ok",
        "total_sessions": total_rows,
        "indexed_sessions": indexed_sessions,
        "skipped_sessions": skipped,
        "total_chunks": total_chunks,
        "errors": errors,
        "watermark": max_watermark,
        "cap_break": cap_break_at is not None,
    }


def index_embeddings(db_path: Path, force: bool = False) -> dict:
    """兼容包装: 单库 = index_embeddings_all([db_path])。"""
    return index_embeddings_all([db_path], force=force)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    # 防递归: 非阻塞文件锁。上一轮没跑完时, cron 下一轮直接退出,
    # 避免实例堆积 + 多进程并发写库损坏索引。
    import fcntl
    _lock_fp = open("/tmp/index_opencode_embed.lock", "w")
    try:
        fcntl.flock(_lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.getLogger("memory-service.index-embed").warning(
            "另一个 index_opencode_embed 实例仍在运行, 本次跳过"
        )
        raise SystemExit(0)
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    # T-DBSPLIT-STAGE1: 库清单走单点发现 (全局+dbs/*.db); 发现失败=明确报错 exit 2
    try:
        dbs = _discover_dbs()
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e), "errors": 1},
                         ensure_ascii=False, indent=2))
        raise SystemExit(2)
    result = index_embeddings_all(dbs, force=args.force)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    # 2026-06-11 审计A3: errors>0 退出非零接入既有告警链 (防静默失败)
    if result.get("errors", 0) > 0:
        raise SystemExit(2)
