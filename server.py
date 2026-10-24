"""REST API server for memory-service.

Pure SQLite backend: vectors live in sqlite-vec vec0 tables (with a
numpy brute-force fallback for legacy databases). No external vector DB.
"""

import hashlib
import hmac
import json
import logging
import os
import socket
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from config import SERVER, EMBEDDING, MONITOR, discover_projects, PROJECTS_DIR, DATA_DIR
from config import load_project_config, SECURITY
from db import (
    VecFallbackError,
    get_or_create_collection,
    search as db_search,
    search_cross_collection,
    get_stats,
    get_client,
    upsert_chunks,
    _safe_name,
    record_feedback,
    list_collections,
    collection_count,
    search_openclaw_memory,
    classify_layer,
    layered_select,
    cold_db_health,
    is_path_query,
    search_by_source,
    _path_route_enabled,
    _seg_begin as db_seg_begin,
    _seg_collect as db_seg_collect,
)
from embedder import embed_single, health_check as embedding_health, cache_stats
from indexer import index_project, start_watching, register_project, get_index_metrics
import monitor
import backup

logger = logging.getLogger("memory-service.server")

# --- 检索缺失台账 (N2 需求侧, 2026-07-26) ----------------------------------
# 口径: session_context 数据地图段 (6/11 审计实测) — distance<0.25 强相关,
# 0.25-0.38 可用, >0.38 基本是噪音。结果 0 条 OR 最佳 distance>此阈 = "检索缺失",
# 追加一行到 MEMORY_MISS_LEDGER (jsonl), 供下游 memory_miss_digest 聚合"补写候选"。
# 失败绝不影响 /api/search 正常响应 (fail-silent append)。
MISS_NOISE_THRESHOLD = float(os.environ.get("MEMORY_MISS_DISTANCE", "0.38"))
MEMORY_MISS_LEDGER = os.environ.get("MEMORY_MISS_LEDGER", "")

# --- 检索命中采样账本 (Y12, 2026-08-02) ------------------------------------
# 口径: 每次 /api/search 记录 top-N (默认3) 命中的 chunk_id/collection/distance
# → hits_ledger.jsonl, 供聚合"死重量占比"(被命中 chunk 频次 / 全集 chunk)供全面实施决策。
# 轻量无阻塞 fail-safe: 失败绝不影响 /api/search 正常响应 (同 miss ledger 契约)。
HITS_SAMPLE_TOPN = int(os.environ.get("MEMORY_HITS_TOPN", "10"))  # MEMEXEC-W2: 3→10 top-3口径偏置(0.36%下界伪影), 回滚=revert本行
MEMORY_HITS_LEDGER = os.environ.get("MEMORY_HITS_LEDGER", "")

_observers = []
_initialized_projects = set()
_indexing_count = 0  # Number of projects currently being indexed
_indexing_lock = threading.Lock()  # Protects _indexing_count

_API_SNAPSHOT_DIR = backup.get_api_snapshot_dir()
_API_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Project helpers
# ---------------------------------------------------------------------------


def _validate_project(proj) -> bool:
    """Skip invalid project configs: root not a directory, no watch targets."""
    root = Path(proj.root)
    if not root.is_dir():
        logger.warning(
            f"Skipping project '{proj.name}': root is not a directory ({proj.root})"
        )
        return False
    if not proj.watch_dirs and not proj.watch_files:
        logger.warning(
            f"Skipping project '{proj.name}': no watch_dirs or watch_files configured"
        )
        return False
    return True


def _init_project(proj):
    """Initialize a single project: start watcher + initial index."""
    global _indexing_count
    if not _validate_project(proj):
        return
    # Balance every validated initialization, including watcher startup errors.
    with _indexing_lock:
        _indexing_count += 1
    try:
        _observers.append(start_watching(proj))
        _initialized_projects.add(proj.name)
        result = index_project(proj, force=False, cleanup=True)  # T-MEMSVC-WALK-SPEEDUP: walk 路径清死缓存 (flash 条件3)
        logger.info(
            f"Initial index {proj.name}: indexed={result['indexed']}, "
            f"skipped={result['skipped']}, files={result['files']}"
        )
    except Exception:
        logger.exception(f"Failed to initialize project '{proj.name}'")
    finally:
        with _indexing_lock:
            _indexing_count -= 1


def _init_projects():
    """Discover and initialize all configured projects sequentially."""
    projects = discover_projects()
    logger.info(f"Discovered {len(projects)} project configs")
    # T-MEMSVC-WALK-SPEEDUP CC GateA C2: 缓存-DB 绑定兜底 — walk 前核验
    # (memory.db 被重建/恢复而 hash 缓存还在 → 全 skip → 检索永久缺数据)。
    try:
        from indexer import verify_hash_cache_against_db
        dropped = verify_hash_cache_against_db(projects)
        if dropped:
            logger.warning("hash cache verified against DB: dropped %d drifted entries", dropped)
    except Exception:
        logger.exception("verify_hash_cache_against_db top-level failed (continue walk)")
    for proj in projects:
        if proj.name in _initialized_projects:
            continue
        _init_project(proj)


# ---------------------------------------------------------------------------
# Collection health check
# ---------------------------------------------------------------------------


def _check_collections():
    """Validate known collections and clean up orphans."""
    try:
        existing = {name: collection_count(name) for name in list_collections()}
        yaml_collection_names = {_safe_name(p.name) for p in discover_projects()}

        for col_name, cnt in existing.items():
            if col_name not in yaml_collection_names:
                if cnt > 0:
                    logger.warning(
                        f"Orphan collection '{col_name}' has {cnt} chunks — "
                        f"skipping delete. Register it in projects/ if needed."
                    )
                else:
                    logger.info(f"Empty orphan collection '{col_name}' — can be ignored")

        for proj in discover_projects():
            col_name = _safe_name(proj.name)
            if col_name in existing:
                cnt = existing[col_name]
                logger.debug(f"Collection '{col_name}' OK: {cnt} chunks")
            else:
                get_or_create_collection(proj.name)
                logger.info(f"Created collection '{col_name}' for project '{proj.name}'")
    except Exception:
        logger.exception("Collection health check failed")


# ---------------------------------------------------------------------------
# Search helper
# ---------------------------------------------------------------------------


def _do_search(
    query: str,
    project_name: str,
    n_results: int,
    min_distance: float | None = None,
    where: dict | None = None,
    use_mmr: bool = True,
    use_feedback: bool = True,
    mode: str | None = None,
    phase: dict | None = None,
) -> list[dict]:
    """Execute a vector similarity search against a single project collection.

    T-MEMSVC-SLOWQUERY-CACHEMISS: 可选 ``phase`` dict — 回填 embed_ms/search_ms
    分解 (slow-query 归因: 区分"ollama embedding 慢" vs "db 检索慢")。
    """
    collection = get_or_create_collection(project_name)
    if collection_count(collection) == 0:
        return []
    _t0 = time.perf_counter()
    embedding = embed_single(query, EMBEDDING.long_text_model)
    _t1 = time.perf_counter()
    if phase is not None:
        phase["embed_ms"] = (_t1 - _t0) * 1000.0
    results = db_search(
        collection,
        query,
        embedding,
        n_results=n_results,
        min_distance=min_distance,
        where=where,
        use_mmr=use_mmr,
        use_feedback=use_feedback,
        mode=mode,
        seg_meta=phase,  # T-MEMSVC-IO-FIX-0930 #4: db 分段计时并入 slow 归因
    )
    if phase is not None:
        phase["search_ms"] = (time.perf_counter() - _t1) * 1000.0
    return results


# ---------------------------------------------------------------------------
# Search-miss ledger (N2 demand-side, 2026-07-26)
# ---------------------------------------------------------------------------


def _record_memory_miss(query, n_results, best_distance, caller_hint):
    """Append one miss record to MEMORY_MISS_LEDGER (jsonl).

    Fail-silent: IO errors are contained; /api/search response is never
    affected. Single-line append is atomic on POSIX for lines < PIPE_BUF.
    """
    if not MEMORY_MISS_LEDGER:
        return
    from datetime import timezone, timedelta

    _cst = timezone(timedelta(hours=8))   # 北京时区, 防 UTC/local tz 误判坑
    rec = {
        "ts": datetime.now(_cst).strftime("%Y-%m-%d %H:%M:%S"),
        "query": (query or "")[:500],
        "n_results": int(n_results) if n_results is not None else 0,
        "best_distance": (round(float(best_distance), 6)
                          if best_distance is not None else None),
        "caller_hint": (caller_hint or "unknown")[:120],
    }
    line = json.dumps(rec, ensure_ascii=False)
    try:
        with open(MEMORY_MISS_LEDGER, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        logger.debug("memory_miss_ledger append failed", exc_info=True)


def _maybe_record_search_miss(handler, query, results, data):
    """Decide + record a search miss. Called from _handle_search (both branches).

    Miss condition: 0 results OR best distance > MISS_NOISE_THRESHOLD.
    caller_hint derived (in priority) from body ``caller``/``project`` fields,
    else HTTP User-Agent — a hint, not authoritative.
    """
    try:
        n = len(results)
        # MEMEXEC-W2/P0-2 (2026-08-18): best=结果集最小 distance, 非 rank1 — 排序键是
        # score(:783, layered 金层优先), rank1 距离≠最小距离 (实测窗 55.4% hits 行
        # rank1>min; ADR 案: rank1=0.448 误判 miss 而真最近邻 0.3005≤0.38)。
        best = min(r.get("distance", 1) for r in results) if results else None
        if n == 0 or (best is not None and best > MISS_NOISE_THRESHOLD):
            caller = (data.get("caller") or data.get("project") or
                      handler.headers.get("User-Agent", "") or "unknown")
            _record_memory_miss(query, n, best, caller)
    except Exception:
        logger.debug("maybe_record_search_miss failed", exc_info=True)


def _record_search_hits(query, results, project_hint, topn, mode, gold_admission):
    """Append one hit record (top-N hits) to MEMORY_HITS_LEDGER (jsonl).

    One line per query, embedding the top-N hits as an array so a single
    POSIX append suffices (lighter IO than N lines). ``collection`` read
    from the result ``collection`` field (set by db.search from the actual
    collection_name — covers indexers that don't set metadata.project, e.g.
    opencode_sessions/claude_code_sessions); falls back to ``metadata.project``
    then the request's ``project`` param when absent.

    ``mode``/``gold_admission`` record the actual ranking path taken
    (R540 §7.1 / backlog item 674): mode=None=纯距离(默认)/"layered"=两段式金层优先;
    gold_admission is the gate value used by layered_select — recorded as
    None when mode != "layered" (gate is dead param in pure-distance path,
    avoids misleading jq consumers with a "0.50" that never fired).

    Fail-silent: IO errors are contained; /api/search response is never
    affected. Single-line append is atomic on POSIX for lines < PIPE_BUF.
    """
    if not MEMORY_HITS_LEDGER:
        return
    from datetime import timezone, timedelta

    _cst = timezone(timedelta(hours=8))   # 北京时区, 防 UTC/local tz 误判坑
    top = []
    for r in results[:topn]:
        meta = r.get("metadata") or {}
        col = r.get("collection") or meta.get("project") or project_hint or "unknown"
        top.append({
            "chunk_id": r.get("id"),
            "collection": col,
            "distance": r.get("distance"),
        })
    rec = {
        "ts": datetime.now(_cst).strftime("%Y-%m-%d %H:%M:%S"),
        "query": (query or "")[:500],
        "project": (project_hint or "")[:120],
        "mode": mode,                                            # None|"layered" 原值不翻译, 单一真相源
        "gold_admission": gold_admission if mode == "layered" else None,  # 非 layered 时是死参数, 记 None 防误导分析
        "top": top,
    }
    line = json.dumps(rec, ensure_ascii=False)
    try:
        with open(MEMORY_HITS_LEDGER, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        logger.debug("memory_hits_ledger append failed", exc_info=True)


def _maybe_record_search_hits(query, results, project_hint, mode, gold_admission):
    """Sample + record the top-N hits for a query. Called from _handle_search.

    Records only when there are hits (the miss ledger already covers the
    0-result / noise-distance cases). Kept fail-silent.
    """
    try:
        if not results:
            return
        _record_search_hits(query, results, project_hint, HITS_SAMPLE_TOPN, mode, gold_admission)
    except Exception:
        logger.debug("maybe_record_search_hits failed", exc_info=True)


# ---------------------------------------------------------------------------
# Direct memory capture — writes to the memory store + snapshot file
# ---------------------------------------------------------------------------


def _capture_memory_item(
    project_name: str,
    item_type: str,
    content: str,
    source: str = "api",
    tags: list[str] | None = None,
) -> dict:
    """Write a single memory item directly to SQLite + snapshot MD file.

    Returns dict with ``chunk_id``, ``indexed`` (bool), ``error`` (str|None).
    """
    now = datetime.now()
    ts = now.strftime("%Y%m%d_%H%M%S")
    chunk_id = hashlib.md5(f"{source}#{item_type}#{ts}".encode()).hexdigest()[:16]
    content_hash = hashlib.md5(content.encode()).hexdigest()[:12]

    # 1. Embed and upsert to SQLite directly (instant availability)
    try:
        collection = get_or_create_collection(project_name)
        embedding = embed_single(content, EMBEDDING.long_text_model)
        chunk = {
            "id": chunk_id,
            "text": content,
            "embedding": embedding,
            "metadata": {
                "source": f"api://{source}",
                "project": project_name,
                "type": item_type,
                "tags": ",".join(tags) if tags else "",
                "chunk_index": 0,
                "total_chunks": 1,
                "chunk_hash": content_hash,
                "file_name": f"api_{ts}",
                "file_mtime": now.timestamp(),
                "layer": classify_layer(f"api://{source}"),
            },
        }
        upsert_chunks(collection, [chunk])
    except Exception as e:
        logger.error(f"Failed to embed/upsert memory item: {e}")
        return {"chunk_id": chunk_id, "indexed": False, "error": str(e)}

    # 2. Also write snapshot MD file for redundancy (file watcher will also index)
    try:
        snapshot_path = _API_SNAPSHOT_DIR / f"api_{ts}_{chunk_id}.md"
        with open(snapshot_path, "w", encoding="utf-8") as f:
            f.write(f"# API Memory Capture\n\n")
            f.write(f"**Time**: {now.isoformat()}\n")
            f.write(f"**Source**: {source}\n")
            f.write(f"**Type**: {item_type}\n\n")
            f.write(f"---\n\n")
            f.write(f"## {item_type.capitalize()}\n\n")
            f.write(f"- {content}\n")
    except Exception as e:
        logger.warning(f"Failed to write snapshot file: {e}")

    logger.info(f"Captured memory: [{item_type}] {content[:60]}...")
    return {"chunk_id": chunk_id, "indexed": True, "error": None}


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------


def _bg_index_project(proj):
    """Background thread target: index a newly registered project."""
    result = index_project(proj, force=False)
    logger.info(f"Background index complete for {proj.name}: {result}")


def _bg_reindex_project(proj, force: bool):
    """Background thread target: reindex a single project."""
    result = index_project(proj, force=force)
    logger.info(f"Reindex {proj.name}: {result}")


def _bg_reindex_all(force: bool):
    """Background thread target: reindex all discovered projects."""
    total = {"indexed": 0, "skipped": 0, "files": 0}
    for proj in discover_projects():
        result = index_project(proj, force=force)
        total["indexed"] += result["indexed"]
        total["skipped"] += result["skipped"]
        total["files"] += result["files"]
    logger.info(f"Reindex all: {total}")


# ---------------------------------------------------------------------------
# HTTP handler — route-dispatched design
# ---------------------------------------------------------------------------

# T-MEMSVC-HANG-ROOTCAUSE (2026-08-14): health 路径 stats TTL 缓存。
# 病根: _handle_health 直接跑 get_stats(), 7.9GB/701k chunks 上负载期 >10s
# → 看门狗判 silent-hang kickstart (当日 8 次)。get_stats 查询已重写 (0.02s),
# 本缓存是机制级第二道保险: 即使未来 db 再出现慢查询, health 也永远不吃它 —
# stale-while-revalidate (旧值秒回 + 后台刷新), 无缓存时省略 db 字段
# (与既有 indexing 分支省略 db 的行为一致, 消费者已容忍缺省)。
try:
    _STATS_TTL_S = float(os.environ.get("MEMSVC_STATS_TTL_S", "60"))
except ValueError:  # flash P3: env 误设非数字不致 server 起不来
    _STATS_TTL_S = 60.0
_stats_cache: dict = {"ts": 0.0, "value": None}
_stats_refresh_lock = threading.Lock()


def _refresh_stats_cache():
    with _stats_refresh_lock:
        # double-check: 排队进锁后可能已被前一线程刷新过 (flash P2: 防过期窗口 N 连刷新)
        if _stats_cache["value"] is not None and time.time() - _stats_cache["ts"] < _STATS_TTL_S:
            return
        try:
            value = get_stats()
            _stats_cache["value"] = value
            _stats_cache["ts"] = time.time()
        except Exception:
            logger.exception("stats cache refresh failed")


def _get_stats_cached():
    """TTL 缓存的 get_stats; 过期返回旧值并后台刷新; 无缓存返回 None (省略 db 字段)。"""
    now = time.time()
    if _stats_cache["value"] is not None:
        if now - _stats_cache["ts"] < _STATS_TTL_S:
            return _stats_cache["value"]
        threading.Thread(target=_refresh_stats_cache, daemon=True).start()
        return _stats_cache["value"]  # stale-while-revalidate
    threading.Thread(target=_refresh_stats_cache, daemon=True).start()
    return None


class _Handler(BaseHTTPRequestHandler):
    """HTTP request handler with route-table dispatch."""

    _GET_ROUTES = {
        "/api/health": "_handle_health",
        "/api/stats": "_handle_stats",
        "/api/projects": "_handle_projects",
        "/api/monitor": "_handle_monitor",
        "/api/ping": "_handle_ping",
    }

    _POST_ROUTES = {
        "/api/search": "_handle_search",
        "/api/memories": "_handle_memories",
        "/api/feedback": "_handle_feedback",
        "/api/register": "_handle_register",
        "/api/reindex": "_handle_reindex",
        "/api/backup": "_handle_backup",
        "/api/restore": "_handle_restore",
        "/api/archive-snapshots": "_handle_archive_snapshots",
    }

    def log_message(self, format, *args):
        logger.debug(format, *args)

    def _json_response(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def setup(self):
        self.request.settimeout(10)
        super().setup()

    def _read_body(self):
        if self.headers.get("Transfer-Encoding"):
            raise ValueError("Transfer-Encoding is unsupported")
        lengths = self.headers.get_all("Content-Length", [])
        if len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdigit():
            raise ValueError("one non-negative Content-Length required")
        length = int(lengths[0])
        if length > 1024 * 1024:
            raise OverflowError("request body exceeds 1 MiB")
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise ValueError("incomplete request body")
        return raw.decode("utf-8")

    # --- Auth middleware -------------------------------------------------

    # Endpoints that are always public (no token required even if security enabled)
    _PUBLIC_ENDPOINTS = {"/api/health", "/api/stats"}

    def _check_auth(self, path: str, is_read: bool = True) -> bool:
        """Check if request has valid auth token when security is enabled.

        Public endpoints (``/api/health``, ``/api/stats``) are always allowed.
        For other endpoints, if ``SECURITY.enabled`` is True, the request must
        include a valid token via the ``Authorization: Bearer <token>`` header.

        Args:
            path: Request URL path (e.g. ``/api/search``).
            is_read: Whether this is a read (GET) request.

        Returns:
            True if the request is authorized, False otherwise (401 sent).
        """
        if not SECURITY.enabled:
            return True

        if SECURITY.enabled and not SECURITY.api_token:
            logger.warning(
                "SECURITY.enabled is True but api_token is empty — "
                "all non-public requests will be rejected"
            )

        # Public endpoints are always accessible
        if path in self._PUBLIC_ENDPOINTS:
            return True

        # Read-only endpoints only require token if configured
        if is_read and not SECURITY.read_requires_token:
            return True

        # Extract token from Authorization header
        auth = self.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else ""

        if not token or not hmac.compare_digest(token.encode(), SECURITY.api_token.encode()):
            self._json_response({"error": "unauthorized"}, 401)
            return False

        return True

    # --- GET dispatch + handlers -----------------------------------------

    def do_GET(self):
        path = urlparse(self.path).path
        t0 = time.time()
        success = True
        try:
            handler_name = self._GET_ROUTES.get(path)
            if handler_name:
                if not self._check_auth(path, is_read=True):
                    return
                getattr(self, handler_name)()
            else:
                self._json_response({"error": "not found"}, 404)
        except Exception:
            success = False
            raise
        finally:
            duration_ms = (time.time() - t0) * 1000
            monitor.record_request(path, duration_ms, success)

    def _handle_ping(self):
        """GET /api/ping — lightweight liveness check (no DB access)."""
        self._json_response({"pong": True, "time": time.time()})

    def _handle_health(self):
        """GET /api/health — service health check.

        When indexing is in progress, skips DB-dependent calls
        (get_stats) to avoid SQLite lock contention timeouts.
        """
        ollama_ok = embedding_health()
        metrics = monitor.get_metrics()
        disk = monitor.disk_info()

        # Determine overall status.
        # 2026-06-25: "磁盘满"和"查询慢"是两件独立的事, 必须分开标注原因, 否则运维会把
        # 磁盘满误读成查询慢 (6/24 就是: 磁盘 90.5% 触发 degraded, slow_count=0, 真延迟问题
        # 反被掩盖). degraded_reasons 列出每一类触发因, 一眼看清是磁盘还是延迟.
        # 2026-08-15 T-MEMSVC-DEGRADED-FIX: slow_query 改滚动窗口判定 (根治粘性) —
        # 窗内总样本 >= degraded_min_samples 且 slow 次数 >= degraded_min_slow
        # 且 slow_ratio > degraded_slow_ratio 才触发; slow 事件滚出窗口后此处
        # 自动转阴, health 自愈, 无需 scheduled_restart。
        search_m = metrics.get("search", {})
        window_count = search_m.get("window_count", 0)
        slow_count = search_m.get("slow_count", 0)
        slow_ratio = search_m.get("slow_ratio", 0.0)
        disk_full = disk.get("used_pct", 0) > 95
        degraded_reasons = []
        if disk_full:
            degraded_reasons.append({
                "type": "disk",
                "detail": f"磁盘占用 {disk.get('used_pct', 0):.1f}% > 95% 阈值 "
                          f"(可用 {disk.get('free_gb', '?')}GB)",
            })
        if (
            window_count >= MONITOR.degraded_min_samples
            and slow_count >= MONITOR.degraded_min_slow
            and slow_ratio > MONITOR.degraded_slow_ratio
        ):
            degraded_reasons.append({
                "type": "slow_query",
                "detail": (
                    f"慢查询 窗内 {slow_count}/{window_count} 次 "
                    f"(ratio {slow_ratio * 100:.1f}% > {MONITOR.degraded_slow_ratio * 100:.0f}%, "
                    f"阈值 {metrics.get('slow_threshold_ms', '?')}ms, "
                    f"窗口 {search_m.get('window_seconds', '?')}s)"
                ),
            })

        if not ollama_ok:
            status = "error"
        elif _indexing_count > 0:
            status = "ok"  # Indexing is normal startup behavior
        elif degraded_reasons:
            status = "degraded"
        else:
            status = "ok"

        response = {
            "status": status,
            "degraded_reasons": degraded_reasons,
            "ollama": ollama_ok,
            "disk": disk,
            "uptime_seconds": metrics["uptime_seconds"],
            "recent_latency_ms": {
                "search_avg": metrics["search"]["avg_ms"],
                "search_max": metrics["search"]["max_ms"],
            },
            "indexer": get_index_metrics(),
            "projects": list(_initialized_projects),
            "watchers": len(_observers),
            "vec_fallback_count": _get_vec_fallback_count(),  # P1-1 (GLM复审建议3): 大集合vec0降级计数暴露监控
            "bit_recall": _bit_recall_health(),  # MEMEXEC-W1: bit粗召回状态暴露 (监控/验收)
            "search_guard": _search_guard_health(),  # T-MEMSVC-PARTITION-IMPL-0927: KNN闸+降温钟
            # T-MEMDB-COLD-SPLIT: 冷库状态 (state/行数/降级计数) — 冷库不可读必须可见
            "cold_db": cold_db_health(),
            # T-MEMSVC-SLOWQUERY-CACHEMISS: 矩阵缓存命中率/加载耗时/等锁 (与 slow 同窗口)。
            # 注意与 embedder 的 "cache" (文本→向量 LRU) 不同物 — 这是 db.py numpy 矩阵 LRU。
            "matrix_cache": metrics.get("matrix_cache"),
        }

        if _indexing_count > 0:
            response["indexing"] = True
        else:
            stats = _get_stats_cached()
            if stats is not None:
                response["db"] = stats
            else:
                # T-MEMSVC-HANG-ROOTCAUSE: 首次调用缓存未热, 不阻塞 health
                response["db_warming"] = True

        self._json_response(response)

    def _handle_stats(self):
        """GET /api/stats — database statistics."""
        self._json_response(get_stats())

    def _handle_projects(self):
        """GET /api/projects — list all configured projects."""
        projects = discover_projects()
        self._json_response(
            {
                "projects": [
                    {
                        "name": p.name,
                        "root": p.root,
                        "watch_files": p.watch_files,
                        "watch_dirs": p.watch_dirs,
                        "indexed": p.name in _initialized_projects,
                    }
                    for p in projects
                ]
            }
        )

    def _handle_monitor(self):
        """GET /api/monitor — service monitoring metrics."""
        metrics = monitor.get_metrics()
        metrics["cache"] = cache_stats()
        metrics["disk"] = monitor.disk_info()
        # L0804-S2: 暴露 503 过载拒绝计数 + 并发上限 (运维观测 fail-silent 退化)。
        if hasattr(self.server, "rejected_503_count"):
            metrics["bounded_server"] = {
                "max_concurrency": getattr(self.server, "max_concurrency", None),
                "rejected_503_count": self.server.rejected_503_count,
            }
        self._json_response(metrics)

    # --- POST dispatch + handlers ----------------------------------------

    def do_POST(self):
        path = urlparse(self.path).path
        self.close_connection = True
        if path not in self._POST_ROUTES:
            self._json_response({"error": "not found"}, 404)
            return
        if not self._check_auth(path, is_read=False):
            return
        try:
            body = self._read_body()
        except OverflowError as exc:
            self._json_response({"error": str(exc)}, 413)
            return
        except (ValueError, UnicodeDecodeError) as exc:
            self._json_response({"error": str(exc)}, 400)
            return
        except (TimeoutError, socket.timeout):
            self._json_response({"error": "request body timeout"}, 408)
            return
        t0 = time.time()
        success = True
        # T-MEMSVC-DEGRADED-FIX: search 慢查询逐条日志需要 project/query 上下文
        search_detail = None
        # R1335 (W1.5 防御): /api/search 进场登记 in-flight, stall watchdog 据此
        # 在卡顿进行中抓栈 (90s 慢查再现时外部 HTTP 探针不触发)。finally 必达注销。
        inflight_tok = monitor.inflight_begin("search") if path == "/api/search" else None
        try:
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                self._json_response({"error": "invalid JSON"}, 400)
                return

            if not isinstance(data, dict):
                self._json_response({"error": "JSON object required"}, 400)
                return

            if path == "/api/search" and isinstance(data, dict):
                search_detail = {
                    "project": str(data.get("project") or ""),
                    "query": str(data.get("query") or ""),
                }

            handler_name = self._POST_ROUTES.get(path)
            if handler_name:
                # All POST endpoints require auth when security is enabled
                if not self._check_auth(path, is_read=False):
                    return
                getattr(self, handler_name)(data)
            else:
                self._json_response({"error": "not found"}, 404)
        except Exception:
            success = False
            raise
        finally:
            if inflight_tok is not None:
                monitor.inflight_end(inflight_tok)
            duration_ms = (time.time() - t0) * 1000
            # T-MEMSVC-SLOWQUERY-CACHEMISS: 合并 phase 分解进 detail (slow 日志用)。
            # ThreadingHTTPServer 每请求一个 handler 实例, self._search_phase 天然请求隔离。
            _phase = getattr(self, "_search_phase", None)
            if isinstance(_phase, dict) and isinstance(search_detail, dict):
                _phase.pop("search_t0", None)
                search_detail["phase_ms"] = _phase
            monitor.record_request(path, duration_ms, success, detail=search_detail)

    # --- POST /api/search (enhanced) -------------------------------------

    def _handle_search(self, data):
        """POST /api/search — vector similarity search.

        Enhanced with ``min_distance`` threshold, ``where`` metadata filter,
        time decay scoring, MMR diversity, and feedback weighting.
        """
        query = data.get("query", "")
        project = data.get("project", "")
        n_results = data.get("n_results", 5)
        min_distance = data.get("min_distance", None)
        source_glob = data.get("source_glob", "")
        use_mmr = data.get("use_mmr", True)
        use_feedback = data.get("use_feedback", True)
        mode = data.get("mode", None)  # None=纯距离(默认), 'layered'=两段式金层优先(L3灰度)
        gold_admission = data.get("gold_admission_distance", 0.50)
        # GLM-5.2 互审 #3 防: 清上一请求可能残留的 phase (当前 HTTP/1.0 每请求一实例
        # 不残留; 若未来启用 keep-alive 复用实例, 此行防 stale phase 串进 400-early-return)。
        self._search_phase = None

        if not query:
            self._json_response({"error": "query is required"}, 400)
            return

        # T-MEMSVC-IO-FIX-0930 #3: 纯路径 query 走 source 精确/后缀检索 (判据
        # db.is_path_query — 路径形态+扩展名+无空白), 免 embed 免向量 (调查 ⑧:
        # 当日 7 条 >8s 慢查全是此类, 语义价值低)。killswitch MEMSVC_PATH_ROUTE=off
        # 或 /tmp/memsvc_path_route_off 旗标 (R1: touch 即关免重启); 库无 source
        # 索引或精确+尾缀全 0 命中时 search_by_source 返回 None → 照走向量
        # (R1: 19% 空结果查询回退向量拿近邻)。min_distance/where/mode 对精确
        # 检索不适用 (文档化); 结果带 route=path 供观测。
        if _path_route_enabled() and is_path_query(query):
            import db as _db_mod  # R1-3: 降级计数差值探测 (模块属性取实时值)
            _t_p0 = time.perf_counter()
            _seg_owned = db_seg_begin()  # R1-3: 开分段上下文, path_ms 进 slow-query 行
            _cold_cnt_before = getattr(_db_mod, "_cold_degraded_count", 0)
            try:
                _prows = search_by_source(query, collection=(project or None),
                                          n_results=n_results)
            except Exception:
                # 路径检索是加速器不是关键路径 — 任何异常 (锁/IO) 回退向量路径,
                # 不放倒请求 (与向量路径 degraded 语义同款保守)
                logger.exception("[path-route] 路径检索异常 → 回退向量路径")
                _prows = None
            finally:
                _seg = db_seg_collect(_seg_owned)
            if _prows is not None:
                self._search_phase = {
                    "embed_ms": 0.0,
                    "search_ms": (time.perf_counter() - _t_p0) * 1000.0,
                }
                if isinstance(_seg, dict) and _seg:
                    self._search_phase["db_seg_ms"] = _seg
                _maybe_record_search_miss(self, query, _prows, data)
                _maybe_record_search_hits(query, _prows, project, mode,
                                          gold_admission)
                self._json_response({
                    "results": _prows, "total": len(_prows),
                    "degraded": False, "route": "path",
                    # T-MEMDB-COLD-SPLIT 口径: 冷库不可读显式回执 (禁静默少返回);
                    # 单集合路径同款计数差值探测 (R1-3)
                    "cold_db_degraded": (getattr(_db_mod, "_cold_degraded_count", 0)
                                         > _cold_cnt_before),
                })
                return

        _vec_degraded = False  # GLM执行审REJECT修: 函数级初始化, 单集合分支也要用

        # Build metadata filter
        where = None
        if source_glob:
            where = {"source": {"$like": f"%{source_glob}%"}}

        if not project:
            _t_e0 = time.perf_counter()
            embedding = embed_single(query, EMBEDDING.long_text_model)
            _t_e1 = time.perf_counter()
            # T-MEMSVC-SLOWQUERY-CACHEMISS: phase 分解 (slow 归因)
            self._search_phase = {
                "embed_ms": (_t_e1 - _t_e0) * 1000.0,
                "search_ms": 0.0,
                "search_t0": _t_e1,
            }
            if not embedding:
                self._json_response({"error": "embedding failed"}, 500)
                return
            # MS-C: vec 模式单次全库扫 (477ms) 替代逐集合扫 (2.7s).
            # search_cross_collection 内部判 vec/legacy 分发, 返回合并 items.
            # CC 终审残留7: 大集合 vec0 故障护栏 → VecFallbackError 异常回执 (per-request, 并发无串扰)
            # W3: tier_meta 出参收集分层路由元数据 (explored_cold/cold_hits/scan_bytes,
            # 设计补2a 增量字段, 三家客户端零改)。
            tier_meta = {"query": query[:200]}
            try:
                all_results = search_cross_collection(
                    embedding,
                    n_results=n_results,
                    where=where,
                    min_distance=min_distance,
                    use_mmr=use_mmr,
                    use_feedback=use_feedback,
                    mode=mode,
                    tier_meta=tier_meta,
                )
            except VecFallbackError as _ve:
                # GLM执行审HOLD修: per-request 异常回执 (并发下全局计数差值会串扰)
                logger.error(f"[search] 大集合 vec0 护栏拦截: {_ve.collection}")
                _vec_degraded = True
                all_results = []
            except Exception as _se:
                logger.error(f"[search] 搜索异常 (degraded): {_se}")
                _vec_degraded = True
                all_results = []
            if isinstance(getattr(self, "_search_phase", None), dict):
                self._search_phase["search_ms"] = (
                    time.perf_counter() - self._search_phase.pop("search_t0")
                ) * 1000.0
                # T-MEMSVC-IO-FIX-0930 #4: db 分段 (bit fetch/COUNT/EXISTS/向量/合并)
                # 并进 slow-query 归因 phase
                _seg = tier_meta.get("seg_ms")
                if isinstance(_seg, dict):
                    self._search_phase["db_seg_ms"] = _seg
            # layered: 全库合并后做两段式选择 (单 collection 金层会被 session 淹没, 设计 §0)
            if mode == "layered":
                all_results = layered_select(all_results, n_results, gold_admission)[:n_results]
            else:
                all_results.sort(key=lambda x: x.get("score", x.get("distance", 1)))
                all_results = all_results[:n_results]
            if _vec_degraded:
                logger.warning("[search] degraded 空结果 — 跳过 miss 落账 (防污染闭环统计, CC终审REJECT-B必修)")
            else:
                _maybe_record_search_miss(self, query, all_results, data)
            _maybe_record_search_hits(query, all_results, project, mode, gold_admission)
            self._json_response(
                {
                    "results": all_results,
                    "total": len(all_results),
                    "degraded": _vec_degraded,  # CC残留7: True=大集合vec0故障护栏拦(空结果非真没有)
                    # W3 (设计补2a): 分层路由增量元数据 — explored_cold=弱结果自动加扫了冷层,
                    # cold_hits=冷层进池 chunk 数, tier_scan_bytes=默认路径扫描字节 (机检锚)。
                    "tier": tier_meta.get("tier"),
                    "explored_cold": tier_meta.get("explored_cold"),
                    "cold_hits": tier_meta.get("cold_hits"),
                    "tier_scan_bytes": tier_meta.get("scan_bytes"),
                    # T-MEMDB-COLD-SPLIT: 冷库不可读的显式降级回执 (禁静默少返回)
                    "cold_db_degraded": tier_meta.get("cold_db_degraded", False),
                }
            )
        else:
            # GLM执行审HOLD修 (2026-08-15): 指定 project 分支同样可能命中大集合 vec0 护栏
            # (a 230k-row project also goes vec0) — 无 try 会裸抛 → connection reset, 比修前更糟
            _phase: dict = {}  # T-MEMSVC-SLOWQUERY-CACHEMISS: embed/search 分解
            # T-MEMDB-COLD-SPLIT: 单集合路径无 tier_meta 通道 → 用降级计数差值探测
            import db as _db_mod
            _cold_cnt_before = getattr(_db_mod, "_cold_degraded_count", 0)
            try:
                results = _do_search(
                    query,
                    project,
                    n_results,
                    min_distance=min_distance,
                    where=where,
                    use_mmr=use_mmr,
                    use_feedback=use_feedback,
                    mode=mode,
                    phase=_phase,
                )
            except VecFallbackError as _ve2:
                logger.error(f"[search] 单集合大集合 vec0 护栏拦截: {_ve2.collection}")
                results = []
                _vec_degraded = True
            except Exception as _se2:
                logger.error(f"[search] 单集合搜索异常 (degraded): {_se2}")
                results = []
                _vec_degraded = True
            if mode == "layered":
                results = layered_select(results, n_results, gold_admission)[:n_results]
            self._search_phase = _phase  # 归因: {embed_ms, search_ms}
            _cold_cnt_after = getattr(_db_mod, "_cold_degraded_count", 0)
            if _vec_degraded:
                logger.warning("[search] degraded 空结果 — 跳过 miss 落账")
            else:
                _maybe_record_search_miss(self, query, results, data)
            _maybe_record_search_hits(query, results, project, mode, gold_admission)
            self._json_response({
                "results": results, "total": len(results), "degraded": _vec_degraded,
                # T-MEMDB-COLD-SPLIT: 本次查询若踩到冷库故障, 显式回执 (禁静默少返回)
                "cold_db_degraded": _cold_cnt_after > _cold_cnt_before,
            })

    # --- POST /api/feedback (record helpful/not-helpful) --------------------

    def _handle_feedback(self, data):
        """POST /api/feedback — record user feedback for a search result.

        Request body::

            {"id": "chunk_id", "helpful": true}
        """
        chunk_id = data.get("id", "")
        helpful = data.get("helpful")

        if not chunk_id:
            self._json_response({"error": "id is required"}, 400)
            return
        if helpful is None or not isinstance(helpful, bool):
            self._json_response({"error": "helpful (boolean) is required"}, 400)
            return

        record_feedback(chunk_id, helpful)
        self._json_response({"status": "ok", "id": chunk_id, "helpful": helpful})

    # --- POST /api/memories (NEW — direct capture) ----------------------

    def _handle_memories(self, data):
        """POST /api/memories — directly capture memory items.

        Bypasses the file system: embeds and writes to the memory store immediately,
        then also writes a snapshot MD file for redundancy.

        Request body::

            {
              "items": [
                {
                  "type": "decision",        // decision|discovery|insight|error
                  "content": "text here",
                  "project": "opencode-memory",  // optional
                  "source": "session-end",       // optional
                  "tags": ["architecture"]       // optional
                }
              ]
            }
        """
        items = data.get("items", [])
        if not items or not isinstance(items, list):
            self._json_response({"error": "items array is required"}, 400)
            return

        results = []
        for item in items:
            item_type = item.get("type", "")
            content = item.get("content", "")
            project = item.get("project", "opencode-memory")
            source = item.get("source", "api")
            tags = item.get("tags", [])

            if not item_type or not content:
                results.append(
                    {"error": "each item needs 'type' and 'content'", "indexed": False}
                )
                continue

            if item_type not in ("decision", "discovery", "insight", "error"):
                results.append(
                    {"error": f"invalid type: {item_type}", "indexed": False}
                )
                continue

            result = _capture_memory_item(
                project_name=project,
                item_type=item_type,
                content=content,
                source=source,
                tags=tags,
            )
            results.append(result)

        accepted = sum(1 for r in results if r.get("indexed"))
        self._json_response(
            {
                "status": "ok",
                "accepted": accepted,
                "total": len(results),
                "items": results,
            }
        )

    # --- POST /api/register ----------------------------------------------

    def _handle_register(self, data):
        """POST /api/register — register a new project for indexing."""
        project_root = data.get("root", "")
        config_path = data.get("config_path", "")

        if config_path:
            proj = load_project_config(config_path)
        elif project_root:
            proj = load_project_config(project_root)
        else:
            self._json_response({"error": "root or config_path required"}, 400)
            return

        if not _validate_project(proj):
            self._json_response(
                {
                    "error": "Invalid project config: root must be a directory "
                    "and must have watch_dirs or watch_files"
                },
                400,
            )
            return

        register_project(proj)
        _observers.append(start_watching(proj))
        _initialized_projects.add(proj.name)
        threading.Thread(target=_bg_index_project, args=(proj,), daemon=True).start()
        self._json_response(
            {
                "status": "registered",
                "project": proj.name,
                "message": "indexing in background",
            }
        )

    # --- POST /api/reindex -----------------------------------------------

    def _handle_reindex(self, data):
        """POST /api/reindex — trigger reindexing."""
        project = data.get("project", "")
        force = data.get("force", False)

        if project:
            for proj in discover_projects():
                if proj.name == project:
                    threading.Thread(
                        target=_bg_reindex_project,
                        args=(proj, force),
                        daemon=True,
                    ).start()
                    self._json_response({"status": "reindexing", "project": project})
                    return
            self._json_response({"error": f"project not found: {project}"}, 404)
        else:
            threading.Thread(target=_bg_reindex_all, args=(force,), daemon=True).start()
            self._json_response({"status": "reindexing", "message": "all projects"})

    # --- POST /api/backup ------------------------------------------------

    def _handle_backup(self, data):
        """POST /api/backup — create a database backup."""
        result = backup.backup_sqlite()
        status_code = 200 if result.get("status") == "ok" else 500
        self._json_response(result, status_code)

    # --- POST /api/restore -----------------------------------------------

    def _handle_restore(self, data):
        """POST /api/restore — restore the database from a named backup.

        **WARNING**: This endpoint is destructive. It replaces the active
        database file. The service should be stopped before restoring to
        avoid corruption from open file handles.

        Request body::

            {"backup": "2026-03-29_153000"}
        """
        backup_name = data.get("backup", "")
        if not backup_name:
            self._json_response({"error": "backup name is required"}, 400)
            return
        result = backup.restore_sqlite(backup_name)
        status_code = 200 if result.get("status") == "ok" else 400
        self._json_response(result, status_code)

    # --- POST /api/archive-snapshots -------------------------------------

    def _handle_archive_snapshots(self, data):
        """POST /api/archive-snapshots — compress old snapshot files.

        Request body (optional)::

            {"days": 30}
        """
        days = data.get("days")
        result = backup.archive_snapshots(days)
        self._json_response(result)


# ---------------------------------------------------------------------------
# Bounded concurrency HTTP server
# ---------------------------------------------------------------------------


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer + 有界并发信号量。

    L0804-S2 root cause ② (止血): 原生 ThreadingHTTPServer 每来一个请求 spawn 一
    线程, 无上限。S1 实测 4 次 hang 时线程 28→62→70→97 递增, 全堵在
    lock_PyThread_acquire_lock。本类用 BoundedSemaphore 把"在途请求"封顶;
    超出上限的连接在主 accept 线程内联回 HTTP 503 并关连接 (不 spawn 线程),
    客户端快速失败而非无限堆积 → 消费端的 fail-silent fallback (session_context
    等本就有本地 ADR fallback) 接管。

    取值: 默认 max_concurrency=8 (env MEMSVC_MAX_CONCURRENCY 可调)。
    8 的依据: ollama bge-m3 embed 单查询 ~200ms, 8 并发 ≈ 40 qps 峰值;
    超过说明上游在刷屏 (如三家启动同时注入), 503 让调用方降级比全员卡死强。
    压测后可上调; 设过大 (>16) 会重新出现 GIL 争用 + 内存回升。
    """

    daemon_threads = True  # 进程退出不等 worker 线程 (防 hang 时 join 阻塞退出)

    def __init__(self, server_address, RequestHandlerClass, max_concurrency=8):
        super().__init__(server_address, RequestHandlerClass)
        self._concurrency_sem = threading.BoundedSemaphore(max_concurrency)
        self.max_concurrency = max_concurrency
        # L0804-S2: 503 拒绝计数 (观测过载; /api/monitor 暴露). 不阻塞不报错,
        # 只计数 — 让"消费端 fail-silent 退化"可被运维看到 (用户要求: 让退化可观测)。
        self.rejected_503_count = 0
        logger.info(
            f"[bounded-server] max_concurrency={max_concurrency} "
            f"(超出请求 → 503 fast-fail, 不 spawn 线程)"
        )

    def process_request(self, request, client_address):
        # 非阻塞取槽位; 满则内联 503 不 spawn 线程 (直接治线程堆积根因)。
        if not self._concurrency_sem.acquire(blocking=False):
            self._reject_503(request)
            return
        try:
            t = threading.Thread(
                target=self._process_request_bounded,
                args=(request, client_address),
                daemon=True,
            )
            t.start()
        except Exception:
            self._concurrency_sem.release()
            raise

    def _process_request_bounded(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)
            self._concurrency_sem.release()

    def _reject_503(self, request):
        # 主 accept 线程内联写一行 503 立即关连接 (不 spawn 线程)。
        self.rejected_503_count += 1
        logger.warning(
            f"[bounded-server] 503 rejected (in-flight={self.max_concurrency}, "
            f"total_rejected={self.rejected_503_count})"
        )
        body = b'{"error":"server overloaded","retry_after":1}\n'
        head = (
            b"HTTP/1.1 503 Service Unavailable\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Connection: close\r\n"
            b"Retry-After: 1\r\n"
            b"\r\n"
        )
        try:
            request.sendall(head + body)
        except OSError:
            pass  # 客户端已断开, 正常
        try:
            self.shutdown_request(request)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _get_vec_fallback_count() -> int:
    """P1-1 (GLM复审建议3): 大集合 vec0 降级计数 (0=无降级, >0 需关注).
    REJECT修3: 必须 import db 模块取实时值 — from db import X 按值拷贝 import 时刻快照, 恒 0 死指标."""
    try:
        import db as _db  # noqa: F401  # 吞: import 失败仅降级 0 (health 不因计数挂)
        return _db._vec_fallback_count
    except Exception as _e:
        logger.error(f"[memsvc] vec_fallback_count 读取失败: {_e}")
        return 0


def _bit_recall_health() -> dict:
    """MEMEXEC-W1: bit 粗召回索引状态 (state/rows/构建耗时/刷新计数), health 只读不阻塞。"""
    try:
        import db as _db
        return _db._bit_index.status()
    except Exception as _e:
        # 吞: health 不因 bit 状态读取失败而挂 (状态面板缺一项, 不是服务故障)
        logger.error(f"[memsvc] bit_recall 状态读取失败: {_e}")
        return {"state": "unknown", "error": str(_e)}


# ── T-MEMSVC-PARTITION-IMPL-0927 P2: W4 降温钟后台线程 ──────────────────────────
# 每 MEMSVC_TIER_COOLDOWN_INTERVAL_S (默认 900s) 一拍, 每拍至多 MEMSVC_TIER_COOLDOWN_MAX_ROWS
# (默认 2000) 行 hot→warm, 每 ≤500 行一个事务 (持 _write_lock, 与 indexer upsert 串行)。
# 首拍延迟 MEMSVC_TIER_COOLDOWN_FIRST_DELAY_S (默认 600s, 让道启动期 bit 构建与初始索引)。
# killswitch: MEMSVC_TIER_COOLDOWN=off (每拍检查, 免重启)。
_tier_cooldown_last: dict = {"state": "not_started"}


def _tier_cooldown_loop():
    import db as _db
    interval = float(os.environ.get("MEMSVC_TIER_COOLDOWN_INTERVAL_S", "900"))
    max_rows = int(float(os.environ.get("MEMSVC_TIER_COOLDOWN_MAX_ROWS", "2000")))
    time.sleep(float(os.environ.get("MEMSVC_TIER_COOLDOWN_FIRST_DELAY_S", "600")))
    while True:
        t0 = time.time()
        try:
            if _db._tier_cooldown_enabled():
                r = _db.tier_cooldown_run(max_rows=max_rows)
                r.update(ts=t0, took_s=round(time.time() - t0, 2), state="ok")
                _tier_cooldown_last.clear()
                _tier_cooldown_last.update(r)
                if r.get("demoted") or r.get("marked_gold"):
                    logger.info(
                        f"[tier-cooldown] hot→warm {r['demoted']} 行, gold 入保护 "
                        f"{r['marked_gold']} 行 (扫 {r['scanned']}, {r['took_s']}s)")
            else:
                _tier_cooldown_last.update(state="disabled", ts=t0)
        except Exception as e:  # 命名: 吞的是单拍降温失败 — 下拍重试, 不影响检索
            _tier_cooldown_last.update(state="error", ts=t0, error=str(e)[:200])
            logger.exception("[tier-cooldown] 本拍失败 (下拍重试)")
        time.sleep(interval)


def _search_guard_health() -> dict:
    """P2/P4 观测: KNN 闸等待 + 降温钟最近一拍 (health 只读, 失败不挂 health)。"""
    try:
        import db as _db
        return {"knn_gate": _db.knn_gate_stats(), "tier_cooldown": dict(_tier_cooldown_last)}
    except Exception as _e:
        return {"error": str(_e)}



def _install_signal_handlers():
    """2026-05-16: 优雅关闭 — 收 SIGTERM (launchctl kickstart 默认信号) 时,
    给 in-flight HTTP 请求 5s grace, 再退出. 防止 SQLite WAL 在 INSERT 中途
    被 -9 截断 (DeepSeek 评审建议)."""
    import signal as _signal
    _shutdown = threading.Event()
    def _handler(signum, frame):
        if _shutdown.is_set():
            # 第二次收到信号 → 立即退出
            logger.warning(f"signal {signum} again, exiting immediately")
            os._exit(1)
        _shutdown.set()
        logger.info(f"signal {signum} received, graceful shutdown (5s grace)")
        # 等 5 秒让 in-flight 请求完成
        def _delayed_exit():
            time.sleep(5)
            logger.info("graceful shutdown grace expired, exiting")
            os._exit(0)
        threading.Thread(target=_delayed_exit, daemon=True).start()
    _signal.signal(_signal.SIGTERM, _handler)
    _signal.signal(_signal.SIGINT, _handler)

    # 2026-08-05 R434 (R404 方案B followup): faulthandler.dump on SIGUSR2 —
    # 免 attach 抓 Python 栈. footprint_monitor 检测到 hang 时发 `kill -USR2 <pid>`,
    # 本进程把全线程 Python traceback dump 到 stderr (→ launchd_stderr.log), 进程
    # 继续运行不退出 (faulthandler.register 非致命). 免 sudoers/py-spy attach (R404).
    # 仅诊断, 不触碰任何请求处理逻辑; 失败只 warn 不阻断启动.
    if hasattr(_signal, "SIGUSR2"):
        try:
            import faulthandler
            faulthandler.register(_signal.SIGUSR2, all_threads=True)
            logger.info(
                "faulthandler registered for SIGUSR2 "
                "(dump all-thread Python stack on `kill -USR2 <pid>`)"
            )
        except (ValueError, OSError, RuntimeError) as e:
            logger.warning(
                f"faulthandler.register(SIGUSR2) failed: {e} (non-fatal, diagnostics only)"
            )

    logger.info("signal handlers installed (SIGTERM, SIGINT → graceful 5s)")

    # B4 (2026-08-05): tracemalloc leak hunt — env MEMSVC_TRACEMALLOC=1 gated.
    # SIGUSR1 = dump snapshot to /tmp/memsvc_tracemalloc/ + top-30 to stderr.
    # Zero impact when env not set (production default). Offline diff:
    #   python3 tracemalloc_debug.py --diff <snap1> <snap2>
    try:
        import tracemalloc_debug
        if tracemalloc_debug.maybe_start():
            logger.info("tracemalloc enabled (kill -USR1 <pid> = snapshot)")
    except Exception as e:
        logger.warning(f"tracemalloc_debug init skipped: {e} (non-fatal)")


def _warmup_embeddings_cache(top_n: int | None = None, delay_s: float = 5.0):
    """启动后预热最大 top_n 个 collection 的向量缓存 (2026-06-11 审计A2)。

    冷查询 15-30s 的根因是懒加载: 首次搜索才从 1GB SQLite 把几万条向量读进
    numpy 缓存。预热后冷查询消失。top_n=9=全部 collection: 无过滤搜索本就会
    遍历全部并把它们留在缓存里 (LRU上限9恰好全装下), 稳态内存与懒加载后相同,
    预热只是把加载提前 (实测全量 RSS ~700MB, watchdog 阈值 3GB, 5h 定时重启重置)。
    任何异常静默, 不影响服务。killswitch: touch SERVICE_DIR/.no_warmup
    """
    import time as _time
    try:
        _time.sleep(delay_s)
        if (DATA_DIR / ".no_warmup").exists():
            logger.info("[warmup] .no_warmup 存在, 跳过预热")
            return
        from db import _load_collection_embeddings, list_collections, collection_count
        # MS-C hybrid: 大集合走 vec0 不需矩阵缓存, 只预热小集合 (legacy 路径).
        # 旧逻辑全量预热 → RSS 3GB (矩阵泄漏源). 现只预热 < 阈值 的集合 (~160MB).
        try:
            from db import _VEC_HYBRID_THRESHOLD
        except ImportError:
            _VEC_HYBRID_THRESHOLD = 25000
        try:
            import resource
            rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // (1024 * 1024)
        except Exception:
            rss_before = -1
        all_cols = list_collections()
        # top_n=None → 预热全部"小"集合 (大集合由 vec0 负责, 不进缓存).
        effective_top_n = len(all_cols) if top_n is None else top_n
        cols = sorted(
            ((c, collection_count(c)) for c in all_cols),
            key=lambda x: -x[1])[:effective_top_n]
        # MS-C: 过滤掉大集合 (它们走 vec0, 预热浪费内存)
        small_cols = [(c, n) for c, n in cols if n < _VEC_HYBRID_THRESHOLD]
        skipped_large = len(cols) - len(small_cols)
        if skipped_large:
            logger.info(f"[warmup] MS-C: 跳过 {skipped_large} 个大集合 (≥{_VEC_HYBRID_THRESHOLD} vecs, 走 vec0)")
        t0 = _time.time()
        # W3: tiered 布局默认路径小集合走 hw 过滤矩阵 (`::hw` 键) — 预热同键,
        # 免首查懒加载; 全量键留给 where/mode 调用方按需加载。
        try:
            from db import _tiered_layout
            hw = _tiered_layout()
        except ImportError:
            hw = False
        for name, cnt in small_cols:
            try:
                _load_collection_embeddings(name, tiers=("hot", "warm") if hw else None)
                logger.info(f"[warmup] {name} ({cnt} chunks, {'hw' if hw else 'full'}) 已预热")
            except Exception as e:
                logger.warning(f"[warmup] {name} 预热失败: {e}")
        try:
            import resource
            rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // (1024 * 1024)
        except Exception:
            rss_after = -1
        logger.info(
            f"[warmup] 完成 {len(small_cols)} collections (大集合走vec0), 耗时 {_time.time()-t0:.1f}s, "
            f"RSS {rss_before}MB -> {rss_after}MB")
    except Exception:
        logger.exception("[warmup] 预热线程异常 (不影响服务)")


def main():
    """Start the memory-service HTTP server."""
    logging.basicConfig(
        level=getattr(logging, SERVER.log_level, logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 2026-05-16: 优雅关闭, 防 SQLite 损坏 (scheduled restart 配套)
    _install_signal_handlers()

    # Fresh-install bootstrap: ensure the data dir exists; db.py creates the
    # schema lazily on first connection (idempotent). Fail fast on a security
    # misconfiguration instead of silently rejecting every request.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if SECURITY.enabled and not SECURITY.api_token:
        raise RuntimeError("MEMSVC_AUTH requires MEMSVC_API_TOKEN")

    # Startup lock: prevent duplicate instances by checking port availability
    try:
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_sock.settimeout(1)
        test_sock.connect((SERVER.host, SERVER.port))
        test_sock.close()
        logger.info(
            f"Port {SERVER.port} already in use — another instance is running. Exiting."
        )
        return
    except OSError:
        pass  # Port is free, we can proceed

    # PID file for additional duplicate detection
    pid_file = DATA_DIR / ".memory-service.pid"
    try:
        existing_pid = pid_file.read_text().strip()
        if existing_pid:
            # Check if the process is actually running
            try:
                os.kill(int(existing_pid), 0)
                logger.info(
                    f"Another instance (PID {existing_pid}) is running. Exiting."
                )
                return
            except (OSError, ValueError, SystemError):
                pass  # Process doesn't exist, stale PID file
    except OSError:
        pass
    pid_file.write_text(str(os.getpid()))

    logger.info("Starting memory-service...")
    logger.info(f"Embedding: {EMBEDDING.ollama_url} ({EMBEDDING.long_text_model})")
    logger.info(f"DB: {SERVER.host}:{SERVER.port}")
    logger.info(f"Projects dir: {PROJECTS_DIR}")

    # MEMEXEC-W1 (2026-08-17): bit 粗召回索引预热 — 刻意早于 collection 健康检查与
    # _init_projects 初始索引风暴: 3.24GB 顺序读优先吃 IO 窗口 (重启后冷查判据靠它)。
    try:
        from db import bit_recall_warmup_start
        bit_recall_warmup_start()
    except Exception:
        logger.exception("[bit-recall] 预热启动失败 (不影响服务)")

    # T-MEMSVC-IO-FIX-0930 #1: 主库 cold 注册行探测预热 — 无 tier 索引库的首个
    # EXISTS 全扫 (I/O 停滞期 10s+) 离开首个搜索线程 (每日 5 次计划重启各有收益)。
    try:
        from db import prewarm_main_cold_probe
        prewarm_main_cold_probe()
    except Exception:
        logger.exception("[cold-probe] 启动预热失败 (不影响服务)")

    # R1335 (W1.5 防御): in-flight 慢查 watchdog — 90s stall 再现时进行中抓全线程栈
    # (killswitch MEMSVC_STALL_WATCHDOG=off; 阈值 MEMSVC_STALL_STACK_SECS, 默认 30s)。
    try:
        monitor.start_stall_watchdog()
    except Exception:
        logger.exception("[stall-watchdog] 启动失败 (不影响服务)")

    logger.info("Running collection health check...")
    _check_collections()

    # 2026-06-25: 启动时按当前集合数自适应缓存上限 + sanity check, 防 LRU 抖动复发.
    try:
        from db import _init_cache_max, assert_cache_covers_collections
        _init_cache_max()
        assert_cache_covers_collections()
    except Exception:
        logger.exception("[cache] 启动期缓存上限自适应/校验异常 (不影响服务)")

    server = BoundedThreadingHTTPServer(
        (SERVER.host, SERVER.port),
        _Handler,
        max_concurrency=int(os.environ.get("MEMSVC_MAX_CONCURRENCY", "8")),
    )
    logger.info(f"Server listening on {SERVER.host}:{SERVER.port}")

    def _init_projects_delayed():
        # MEMEXEC-W1: 初始索引风暴让道 bit 构建 — 3.24GB 顺序读在无争抢窗内 ~2.5s
        # 完成, 否则风暴争抢下 5s+ 且重启后首查撞 float 回退(冷查判据破)。代价仅是
        # 重启后索引 catch-up 晚 N 秒 (后台本就异步)。0 = 禁用 (恢复旧行为)。
        delay = float(os.environ.get("MEMSVC_INDEXER_DELAY_S", "6"))
        if delay > 0:
            time.sleep(delay)
        _init_projects()

    threading.Thread(target=_init_projects_delayed, daemon=True).start()
    logger.info("Initial project indexing started in background")
    # T-MEMSVC-PARTITION-IMPL-0927 P2: W4 降温钟 (hot→warm), 首拍延迟见 _tier_cooldown_loop
    threading.Thread(target=_tier_cooldown_loop, name="tier-cooldown", daemon=True).start()
    # 2026-06-11 审计A2: 启动预热top collections向量缓存, 消灭冷查询15-30s悬崖。
    # 只预热最大3个(内存约束, 会审: 全量~530MB+撞内存前科); killswitch=触摸 .no_warmup
    # L0804-S2 fix ⑤: delay 5s→1s. 首用户不再吃满 14s 冷查询; footprint_monitor 的
    # etime>60s hang-kickstart 护栏保证预热期内不会被误杀。
    threading.Thread(target=_warmup_embeddings_cache, kwargs={"delay_s": 1.0}, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for obs in _observers:
            obs.stop()
        server.server_close()
    except Exception:
        logger.exception("Server crashed")
        raise
    finally:
        try:
            pid_file.unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    main()
