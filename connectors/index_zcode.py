"""Embedding-only indexer for ZCode tasks-index.sqlite → SQLite memory.db.

Scans ~/.zcode/v2/tasks-index.sqlite, extracts searchable_text from native
ZCode sessions (excludes claude-import-* migrated records to avoid double
indexing — those already live in claude_code_sessions), chunks, embeds, and
stores in memory.db under collection `zcode_sessions`.

Mirrors index_opencode_embed.py (SQLite source) + index_claude_code_embed.py
robustness (try/except, active-skip, --reset). Fixes two defects carried by
both existing indexers:
  - defect A: embed_batch failure returns [] not None; `if emb is not None`
    is always True for []. This indexer uses `if emb` (empty list is falsy).
  - cron bare-call (no cron_wrapper) — handled at crontab layer, not here.

Cron-driven (daily in the reference deployment), idempotent on
(task_id + updated_at), so re-runs on the same data are no-ops.

Three-way reviewed 2026-06-24 (Opus 4.8 + DeepSeek-V4-Pro, GLM-5.2 as
author not self-reviewing). Claude line-by-line audit PASS WITH FIXES
(F1/F3/F4 applied). See docs/specs/T-ZCODE-MEMORY-INDEX-spec.md.
"""

import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_or_create_collection, upsert_chunks, _get_conn
from embedder import embed_batch

logger = logging.getLogger("memory-service.index-zcode")

# ZCode Electron app stores all sessions in a single SQLite db.
DB_PATH = Path(os.environ.get(
    "ZCODE_DB", Path.home() / ".zcode" / "v2" / "tasks-index.sqlite"))
COLLECTION_NAME = "zcode_sessions"
SOURCE_TYPE = "zcode"
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100
ACTIVE_SKIP_SECONDS = 300  # skip sessions updated <5min ago (in-progress, content mutating)

# T-EMBED-INCREMENTAL (2026-07-24): 增量水位 — 只处理 updated_at > 上次水位的 task, 热启动秒退。
# 冷启动(无水位文件)=0=全量。单位=ms(与 tasks.updated_at 一致)。active 行不计入水位(下轮重考)。
_LAST_RUN = Path(__file__).parent / ".last_run_zcode"


def _read_last_run() -> float:
    try:
        return float(_LAST_RUN.read_text().strip())
    except (OSError, ValueError):
        return 0.0


def _write_last_run(watermark: float) -> None:
    try:
        tmp = _LAST_RUN.with_name(_LAST_RUN.name + ".tmp")
        tmp.write_text(str(watermark))
        tmp.replace(_LAST_RUN)
    except OSError as e:
        logger.warning("水位写入失败(不阻塞): %s", e)

# 切块统一走 chunking.py 单一实现 (与 watcher / 其他 indexer 同源, 2026-06-11 审计B1)
from chunking import chunk_text as _unified_chunk_text, chunk_hash as _chunk_hash, CHUNKER_VERSION


def _chunk_text(text: str) -> list[str]:
    return _unified_chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP)


def _get_indexed_versions(collection_name: str) -> set[str]:
    """Return set of session_version strings already indexed for this collection.

    Filtered by source_type='zcode' so we don't pull versions from other
    collections that share the chunks table.
    """
    conn = _get_conn()
    rows = conn.execute(
        "SELECT metadata FROM chunks WHERE collection = ? AND metadata LIKE ?",
        (collection_name, '%"source_type": "zcode"%'),
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
    return versions


def _delete_session_chunks(collection_name: str, session_id: str) -> int:
    """Delete all chunks belonging to one session (any version).

    Used before re-indexing a session whose updated_at changed, so stale
    chunks from the old version (which may have had a different chunk_total)
    don't linger as orphans. (fix#4, DeepSeek must-fix #2)
    """
    conn = _get_conn()
    cur = conn.execute(
        "DELETE FROM chunks WHERE collection = ? "
        "AND json_extract(metadata, '$.session_id') = ?",
        (collection_name, session_id),
    )
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def _delete_collection_chunks(collection_name: str) -> int:
    """DELETE all chunks in this collection (for --reset)."""
    conn = _get_conn()
    cur = conn.execute("DELETE FROM chunks WHERE collection = ?", (collection_name,))
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def index_zcode(force: bool = False, reset: bool = False) -> dict:
    if not DB_PATH.exists():
        return {"status": "error", "error": f"DB not found: {DB_PATH}"}

    # R1 (.zcode 易失): schema 可能被 ZCode 升级改动 — 读失败时报错不崩
    try:
        collection_name = get_or_create_collection(COLLECTION_NAME)
    except Exception as e:
        return {"status": "error", "error": f"get_or_create_collection fail: {e}"}

    deleted_chunks = 0
    if reset:
        deleted_chunks = _delete_collection_chunks(collection_name)
        logger.info("reset: deleted %d existing chunks from %s", deleted_chunks, collection_name)

    last_run = 0.0 if (force or reset) else _read_last_run()  # ms; 0=冷启全量

    # fix#3 (Opus should-fix + DeepSeek must-fix #1): 只读 URI 连接防写/锁 ZCode 的库;
    # WAL 模式下 reader 不阻塞 writer, busy_timeout 防 SQLITE_BUSY 瞬断。
    # 先 fetchall 快照全部行 + 立即 close, 再进 embed 循环 (不长持读连接妨碍 WAL checkpoint)。
    now_ms = int(time.time() * 1000)
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        conn.execute("PRAGMA busy_timeout=5000")
        # T-EMBED-INCREMENTAL: 只取 updated_at > 上次水位 (冷启 last_run=0 → 全量)
        rows = conn.execute("""
            SELECT task_id, searchable_text, workspace_path, provider, model,
                   created_at, updated_at, task_status
            FROM tasks
            WHERE deleted = 0
              AND task_id NOT LIKE 'claude-import-%'
              AND length(searchable_text) > 5
              AND updated_at > ?
            ORDER BY updated_at
        """, (last_run,)).fetchall()
        conn.close()
    except sqlite3.Error as e:  # F4 (Claude audit): 覆盖 DatabaseError(库损坏) 不只 OperationalError
        logger.error("read tasks-index.sqlite fail: %s", e)
        return {"status": "error", "error": f"read source DB fail: {e}"}

    # 热启动: 无新 task → 秒退 (不 fetch indexed_versions, 不 embed, 不动水位)
    if not rows:
        return {"status": "ok", "total_sessions": 0, "indexed_sessions": 0,
                "skipped_sessions": 0, "total_chunks": 0, "errors": 0,
                "incremental": True, "last_run": last_run}

    indexed_versions: set[str] = set() if (force or reset) else _get_indexed_versions(collection_name)

    total_chunks = 0
    errors = 0
    skipped = 0
    skipped_active = 0
    indexed_sessions = 0
    max_watermark = last_run  # 本轮见到的最大 updated_at (仅非active行计入, active下轮重考)
    failed_mtimes = []  # 有内容但未入库(embed/upsert失败)的 updated_at; 水位卡在最早失败前保证重试

    for row in rows:
        task_id, searchable_text, workspace_path, provider, model, \
            created_at, updated_at, task_status = row

        # fix (claude_code 模板特性): 活跃跳过 — 会话还在进行, 内容会变, 避免索引半截对话
        if now_ms - updated_at < ACTIVE_SKIP_SECONDS * 1000:
            skipped_active += 1
            continue  # active: 不入水位(会话还在变, 下轮重考)

        max_watermark = max(max_watermark, updated_at)  # 非active才计入水位(含skip/no-text)
        version = f"{task_id}:{updated_at}"
        if version in indexed_versions and not force:
            skipped += 1
            continue

        # searchable_text 是纯文本 (用户消息 + assistant 回复交替, 无角色标记)
        chunks = _chunk_text(searchable_text)
        if not chunks:
            continue

        chunk_dicts = []
        for i, chunk in enumerate(chunks):
            # fix#2 (Opus must-fix #2): 用 full task_id 不截断 — ZCode task_id 是完整 UUID,
            # 模板的 [:16] 截断对 claude-import-* 共享前缀会碰撞。full id 保证唯一。
            chunk_id = f"zc_{task_id}_{i}"
            chunk_dicts.append({
                "id": chunk_id,
                "text": chunk,
                "embedding": None,
                "metadata": {
                    "source_type": SOURCE_TYPE,
                    # F3 (Claude audit): 逐会话 source, 对齐 opencode/claude_code,
                    # 避免 delete_by_source 误删整库
                    "source": f"zcode_db:{task_id}",
                    "chunk_hash": _chunk_hash(chunk),
                    "chunker_version": CHUNKER_VERSION,
                    "session_id": task_id,
                    "session_version": version,  # 幂等键
                    "task_status": task_status or "",
                    "workspace_path": workspace_path or "",
                    # Q1 会审定论: 保留 provider/model, 未来多 provider 过滤维度, 无害
                    "provider": provider or "",
                    "model": model or "",
                    "chunk_index": i,
                    "chunk_total": len(chunks),
                    "file_mtime": updated_at / 1000,  # db.search 的 time_decay 要秒级
                },
            })

        # fix#5 (DeepSeek must-fix #3) + defect A 修正:
        # embed_batch 失败返回空 list [] 而非 None; 现有 indexer 用 `if emb is not None`
        # 恒为 True, 空 list 漏过去被 db.upsert_chunks 静默 reject, 但 session_version
        # 已标记完成 → 失败块永久丢失。这里用 `if emb` (空 list falsy) 正确判失败。
        try:
            embeddings = embed_batch([c["text"] for c in chunk_dicts])
        except Exception as e:
            logger.warning("embed_batch fail %s: %s", task_id, e)
            errors += 1
            continue

        for i, emb in enumerate(embeddings):
            if emb:  # defect A fix: 空 list 为 falsy
                chunk_dicts[i]["embedding"] = emb

        valid = [c for c in chunk_dicts if c.get("embedding")]
        # 部分失败整体跳过: session_version 保持缺失, 下轮重试 (失败多为 Ollama 瞬断)
        # T-EMBED-INCREMENTAL: 未全部成功(部分 或 全失败=Ollama断) → 未入库 → 记 mtime 卡水位保重试
        if len(valid) < len(chunk_dicts):
            failed_mtimes.append(updated_at)
            if len(valid) > 0:
                logger.warning(
                    "partial embed fail %s: %d/%d 成功, 本轮整体跳过待重试",
                    task_id, len(valid), len(chunk_dicts))
                errors += 1
            continue
        if valid:
            try:
                # F1 (Claude audit, 中危): 删旧块移到 embed 成功后、upsert 前。
                # 原先放在 embed 前会导致 embed 失败时旧块已删新块没进 → 会话临时搜不到。
                # 现在只有 embed 全部成功的 valid 块准备好, 才删旧块再 upsert, 失败则旧块完好。
                # fix#4 (DeepSeek must-fix #2): 防孤儿块 (chunk_total 变小时高位旧块残留)
                if not force and not reset:
                    _delete_session_chunks(collection_name, task_id)
                upsert_chunks(collection_name, valid)
                total_chunks += len(valid)
                indexed_sessions += 1
            except Exception as e:
                logger.warning("upsert fail %s: %s", task_id, e)
                errors += 1
                failed_mtimes.append(updated_at)  # upsert 失败也未入库 → 卡水位重试

    # 推进水位: 有失败 → 卡在最早失败前(保证下轮重试所有失败, 不丢数据); 否则推进到本轮最大。
    # 无条件写(force 也推进)。已入库者下轮靠 version 跳过廉价。
    _write_last_run((min(failed_mtimes) - 1) if failed_mtimes else max_watermark)

    return {
        "status": "ok",
        "scanned_sessions": len(rows),
        "indexed_sessions": indexed_sessions,
        "skipped_versions": skipped,
        "skipped_active": skipped_active,
        "deleted_chunks_on_reset": deleted_chunks,
        "errors": errors,
        "total_chunks": total_chunks,
        "watermark": max_watermark,
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    # 防递归: 非阻塞文件锁。上一轮没跑完时, cron 下一轮直接退出,
    # 避免实例堆积 + 多进程并发写损坏索引。
    import fcntl
    _lock_fp = open("/tmp/index_zcode_embed.lock", "w")
    try:
        fcntl.flock(_lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.getLogger("memory-service.index-zcode").warning(
            "另一个 index_zcode_embed 实例仍在运行, 本次跳过"
        )
        raise SystemExit(0)
    import argparse
    parser = argparse.ArgumentParser(description="Index ZCode sessions into memory.db.")
    parser.add_argument("--force", action="store_true",
                        help="reindex all sessions (keep existing chunks, upsert by id)")
    parser.add_argument("--reset", action="store_true",
                        help="DELETE all collection chunks first, then full reindex from scratch")
    args = parser.parse_args()
    result = index_zcode(force=args.force, reset=args.reset)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    # errors>0 或 status=error 退出非零, 让 cron_wrapper/watchdog 既有告警链抓到
    # (此前 errors 只进 JSON 没人看 = 静默失败, internal convention 红线)
    # GAP2 HOLD 修复(2026-10-05): 135/141/171 早退路径返回 {"status":"error"} 不带 errors 键
    # → rc=0 (05-fail-write.txt 实测违约); status=error 同样必须 exit 2。
    if result.get("errors", 0) > 0 or result.get("status") == "error":
        raise SystemExit(2)
