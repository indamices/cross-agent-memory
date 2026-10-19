"""Embedding-only indexer for Claude Code session JSONL files → memory.db.

Scans ~/.claude/projects/<encoded-cwd>/<session-id>.jsonl, extracts user
messages and assistant text/tool calls, chunks, embeds, stores in memory.db
under collection `claude_code_sessions`.

Mirrors index_opencode_embed.py: cron-driven (hourly), idempotent on
sessionId+mtime, so re-runs on the same data are no-ops.
"""

import hashlib
import json
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import _env_num
from db import get_or_create_collection, upsert_chunks, _get_conn
from embedder import embed_batch

logger = logging.getLogger("memory-service.index-claude-code")

PROJECTS_ROOT = Path(os.environ.get(
    "CLAUDE_PROJECTS_DIR", Path.home() / ".claude" / "projects"))
COLLECTION_NAME = "claude_code_sessions"
# T-MEMSVC-WALK-SPEEDUP P3: per-run chunk 上限 (env 可调; 与 index_opencode_embed 同语义:
# 文件级原子 — 只在 session 完整入库后检查, 不截断半 session; 水位停在最后完整 session)
_MAX_CHUNKS = int(_env_num("MEMSVC_EMBED_MAX_CHUNKS", 2000))  # flash 执行审次要: 非法 env 防御
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100
ACTIVE_SKIP_SECONDS = 300

# T-EMBED-INCREMENTAL (2026-07-24): 增量水位 — 只处理 st_mtime > 上次水位的 jsonl, 热启动秒退,
# 跳过未变文件的昂贵解析。冷启动(无水位文件)=0=全量。单位=秒(与 st_mtime 一致)。active 不计入水位。
_LAST_RUN = Path(__file__).parent / ".last_run_claude_code"


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


# 2026-06-11 审计B1: prefix_hash + DEDUP_PREFIX_CHARS 移至 chunking.py 单一真相源,
# indexer.py (watcher) 同步使用, 消灭双路径 (6/9 回灌根因)。
from chunking import (
    chunk_text as _unified_chunk_text,
    chunk_hash as _chunk_hash,
    prefix_hash as _prefix_hash,
    content_hash as _content_hash,                       # MEMIDX change#1
    pilot_blacklist_filter as _pilot_blacklist_filter,   # MEMIDX change#2 (共享单一真相源)
    CHUNKER_VERSION,
    DEDUP_PREFIX_CHARS,
)
import pilot_config  # MEMIDX 试点门控 (T-MEMIDX-PILOT-IMPL; fail-safe 默认全关; 与 watcher 同门控)


def _chunk_text(text: str) -> list[str]:
    return _unified_chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP)


def _get_indexed_versions(collection_name: str) -> set[str]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT metadata FROM chunks WHERE collection = ? AND metadata LIKE ?",
        (collection_name, '%"source_type": "claude_code"%'),
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


def _extract_text_from_content(content) -> str:
    """Claude messages: content is str or list[block]. Extract user-meaningful text."""
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        btype = block.get("type")
        if btype == "text":
            t = block.get("text", "")
            if isinstance(t, str) and t.strip():
                parts.append(t.strip())
        elif btype == "tool_use":
            name = block.get("name", "?")
            parts.append(f"[tool:{name}]")
        elif btype == "tool_result":
            # tool_result is verbose (file contents, command output) — skip to keep index focused.
            continue
    return "\n".join(parts).strip()


def _is_system_reminder(content) -> bool:
    """User messages embedding system reminders are not user input."""
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                return True
    return False


def _parse_session_file(jsonl_path: Path) -> tuple[str, str, str, list[str]]:
    """Return (session_id, cwd, git_branch, joined_texts)."""
    session_id = ""
    cwd = ""
    git_branch = ""
    texts: list[str] = []

    try:
        raw = jsonl_path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        logger.warning("read fail %s: %s", jsonl_path.name, e)
        return session_id, cwd, git_branch, texts

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(d, dict):
            continue

        t = d.get("type")
        if not session_id:
            session_id = d.get("sessionId") or ""
        if not cwd:
            cwd = d.get("cwd") or ""
        if not git_branch:
            git_branch = d.get("gitBranch") or ""

        if t == "user":
            msg = d.get("message", {})
            if not isinstance(msg, dict):
                continue
            content = msg.get("content", "")
            if _is_system_reminder(content):
                continue
            text = _extract_text_from_content(content)
            if text and len(text) > 2:
                texts.append(f"USER: {text}")
        elif t == "assistant":
            msg = d.get("message", {})
            if not isinstance(msg, dict):
                continue
            text = _extract_text_from_content(msg.get("content", ""))
            if text:
                texts.append(f"ASSISTANT: {text}")

    return session_id, cwd, git_branch, texts


def _delete_collection_chunks(collection_name: str) -> int:
    conn = _get_conn()
    cur = conn.execute("DELETE FROM chunks WHERE collection = ?", (collection_name,))
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def _load_existing_prefix_hashes(collection_name: str) -> set[str]:
    # B1: 委托 db.load_collection_prefix_hashes (单一真相源), 与 indexer.py 共路径
    from db import load_collection_prefix_hashes
    return load_collection_prefix_hashes(collection_name)


def index_claude_code(force: bool = False, reset: bool = False) -> dict:
    if not PROJECTS_ROOT.exists():
        return {"status": "error", "error": f"projects dir not found: {PROJECTS_ROOT}"}

    collection_name = get_or_create_collection(COLLECTION_NAME)

    # MEMIDX 试点门控 (T-MEMIDX-PILOT-IMPL; scope 用 project 名 'claude-code-sessions', 与 watcher 一致)
    _pilot = pilot_config.pilot_for("claude-code-sessions")
    _ver = _pilot["chunker_version_tag"] if _pilot else CHUNKER_VERSION       # 版本标(两路径都 v2-pilot)
    _use_full_hash = bool(_pilot and (_pilot.get("dedup") or {}).get("full_content_hash"))
    _bl = (_pilot.get("blacklist") if _pilot else None)
    _bl = _bl if (isinstance(_bl, dict) and _bl.get("enabled")) else None      # change#2 黑名单(共享 chunking 版)

    deleted_chunks = 0
    if reset:
        deleted_chunks = _delete_collection_chunks(collection_name)
        logger.info("reset: deleted %d existing chunks from %s", deleted_chunks, collection_name)

    last_run = 0.0 if (force or reset) else _read_last_run()  # 秒; 0=冷启全量
    all_files = list(PROJECTS_ROOT.glob("*/*.jsonl"))
    now = time.time()

    # T-EMBED-INCREMENTAL: 一次 stat 出 (fp, mtime), 只留 mtime > 上次水位的(冷启=0=全部),
    # 跳过未变文件的昂贵解析(旧版对每个 jsonl 都 _parse_session_file 再按 version 跳=99%白工)。
    # 文件消失(OSError)忽略 — 非 embed 错误。
    scored = []
    for fp in all_files:
        try:
            mt = fp.stat().st_mtime
        except OSError:
            continue
        if mt > last_run:
            scored.append((fp, mt))

    # 热启动: 无新文件 → 秒退 (不 fetch indexed_versions/prefix_hashes, 不解析, 不动水位)
    if not scored:
        return {"status": "ok", "total_files": len(all_files), "indexed_sessions": 0,
                "skipped_versions": 0, "skipped_active": 0, "total_chunks": 0, "errors": 0,
                "incremental": True, "last_run": last_run}

    indexed_versions: set[str] = set() if (force or reset) else _get_indexed_versions(collection_name)
    seen_prefix_hashes: set[str] = set() if reset else _load_existing_prefix_hashes(collection_name)
    seeded_hashes = len(seen_prefix_hashes)

    total_chunks = 0
    skipped = 0
    skipped_active = 0
    skipped_dedup = 0
    indexed_sessions = 0
    errors = 0
    max_watermark = last_run  # 本轮见到的最大非active mtime
    failed_mtimes = []  # 有内容但未入库(embed/upsert失败)的 mtime; 水位卡在最早失败前保证重试

    for fp, mtime in scored:
        if now - mtime < ACTIVE_SKIP_SECONDS:
            skipped_active += 1
            continue  # active: 不入水位(下轮重考)
        max_watermark = max(max_watermark, mtime)  # 非active才计入

        session_id, cwd, git_branch, texts = _parse_session_file(fp)
        if not session_id:
            session_id = fp.stem
        version = f"{session_id}:{int(mtime)}"

        if version in indexed_versions and not force:
            skipped += 1
            continue

        if not texts:
            continue

        full_text = "\n\n".join(texts)
        if _bl:  # MEMIDX change#2 门控: 黑名单过滤(共享 chunking 版, 与 watcher 一致)
            full_text = _pilot_blacklist_filter(full_text, _bl)
        chunks = _chunk_text(full_text)
        if not chunks:
            continue

        project_path = fp.parent.name

        chunk_dicts = []
        for i, chunk in enumerate(chunks):
            # MEMIDX change#1 门控: 指纹 content_hash(抓长模板)否则 prefix_hash; 去重键=(版本, 指纹)元组
            # —— 与 watcher(indexer)及 db.load_collection_prefix_hashes 的元组口径一致(修此前 str/tuple 失配)。
            _fp = _content_hash(chunk) if _use_full_hash else _prefix_hash(chunk)
            dedup_key = (str(_ver), _fp)
            if dedup_key in seen_prefix_hashes:
                skipped_dedup += 1
                continue
            seen_prefix_hashes.add(dedup_key)
            chunk_id = f"cc_{session_id[:16]}_{i}"
            chunk_dicts.append({
                "id": chunk_id,
                "text": chunk,
                "embedding": None,
                "metadata": {
                    "source_type": "claude_code",
                    # 2026-06-11 审计A3: 对齐 watcher schema — 此前 cron 路径不写
                    # source/chunk_hash, 是全库 22% 元数据腐烂(无法去重/溯源)的来源
                    "source": str(fp),
                    "chunk_hash": _chunk_hash(chunk),
                    "chunker_version": _ver,  # MEMIDX: 试点=v2-pilot(与 watcher 一致, 两路径不互相覆盖)
                    "session_id": session_id,
                    "session_version": version,
                    "project_path": project_path,
                    "cwd": cwd,
                    "git_branch": git_branch,
                    "chunk_index": i,
                    "chunk_total": len(chunks),
                    "file_mtime": mtime,
                    "file_path": str(fp),
                    "prefix_hash": _fp,  # MEMIDX: 试点下=content_hash(留作调试; 去重真相源是 db.load 重算)
                },
            })
        if not chunk_dicts:
            continue

        try:
            embeddings = embed_batch([c["text"] for c in chunk_dicts])
        except Exception as e:
            logger.warning("embed fail %s: %s", session_id, e)
            errors += 1
            failed_mtimes.append(mtime)  # 未入库 → 卡水位重试
            continue

        for i, emb in enumerate(embeddings):
            if emb is not None:
                chunk_dicts[i]["embedding"] = emb

        valid = [c for c in chunk_dicts if c["embedding"] is not None]
        # 2026-06-11 审计A3: 部分/全失败时整个 session 跳过不入库 (失败多为 Ollama 瞬断)。
        # T-EMBED-INCREMENTAL: 未入库者记 mtime, 水位卡在最早失败前保证下轮重试(不再靠全量重扫)。
        if len(valid) < len(chunk_dicts):
            failed_mtimes.append(mtime)
            if len(valid) > 0:
                logger.warning(
                    "partial embed fail %s: %d/%d 成功, 本轮整体跳过待重试",
                    session_id, len(valid), len(chunk_dicts))
                errors += 1
            continue
        if valid:
            try:
                upsert_chunks(collection_name, valid)
                total_chunks += len(valid)
                indexed_sessions += 1
                # T-MEMSVC-WALK-SPEEDUP P3: 上限检查在 session 完整入库后 (文件级原子, flash D4)
                if total_chunks >= _MAX_CHUNKS:
                    logger.info(
                        "P3 chunk cap reached (%d >= %d), stop; remaining files deferred to next run",
                        total_chunks, _MAX_CHUNKS,
                    )
                    break
            except Exception as e:
                logger.warning("upsert fail %s: %s", session_id, e)
                errors += 1
                failed_mtimes.append(mtime)  # upsert 失败也未入库

    # 推进水位: 有失败 → 卡在最早失败前(保证下轮重试所有失败, 不丢数据, 且只重扫最早失败起而非全量);
    # 否则推进到本轮最大。无条件写(force 也推进)。EPS=1秒(st_mtime 单位), 保证最早失败下轮被 > 捞回。
    _write_last_run((min(failed_mtimes) - 1) if failed_mtimes else max_watermark)

    return {
        "status": "ok",
        "scanned_files": len(scored),  # 本轮增量过滤后(mtime>水位)的文件数, 非全量
        "indexed_sessions": indexed_sessions,
        "skipped_versions": skipped,
        "skipped_active": skipped_active,
        "skipped_dedup": skipped_dedup,
        "seeded_prefix_hashes": seeded_hashes,
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
    # 避免实例堆积 + 多进程并发写库损坏索引。
    import fcntl
    _lock_fp = open("/tmp/index_claude_code_embed.lock", "w")
    try:
        fcntl.flock(_lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.getLogger("memory-service.index-embed").warning(
            "另一个 index_claude_code_embed 实例仍在运行, 本次跳过"
        )
        raise SystemExit(0)
    import argparse
    parser = argparse.ArgumentParser(description="Index Claude Code session JSONLs.")
    parser.add_argument("--force", action="store_true", help="reindex all sessions (keep existing chunks, upsert by id)")
    parser.add_argument("--reset", action="store_true", help="DELETE all collection chunks first, then full reindex from scratch (use with prefix-hash dedup)")
    args = parser.parse_args()
    result = index_claude_code(force=args.force, reset=args.reset)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    # 2026-06-11 审计A3: errors>0 退出非零, 让 cron_wrapper/watchdog 既有告警链抓到
    # (此前 errors 只进 JSON 没人看 = 静默失败, internal convention 红线)
    if result.get("errors", 0) > 0:
        raise SystemExit(2)
