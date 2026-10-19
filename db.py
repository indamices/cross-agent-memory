"""SQLite vector storage with sqlite-vec (vec0) primary, numpy legacy fallback.

Schema is compatible with OpenClaw's memory system (same embedding dim/model).
Embeddings stored as float32 BLOBs in `chunks`, indexed by collection name.

Search path (MS-C, 2026-08):
  - Primary: sqlite-vec vec0 virtual table (`vec_chunks`, derived from `chunks`).
    Big collections (≥25000 vecs) run on vec0 — no full-matrix materialization.
    See MS-C block below + scripts/migrate_to_vec0.py (rebuild derived index).
  - Legacy fallback: brute-force cosine via numpy (~7ms for 91k vectors), used
    when sqlite_vec not installed (MEMSVC_SEARCH_MODE=legacy).

NOTE (2026-08-05, B2 注释清源): earlier docstrings said "Pure SQLite + numpy,
no ChromaDB" and "not sqlite-vec". That was accurate at the time (6/4 T-MEMSVC
probe) but is now REVERSED — vec0 is deployed (see [vec0] log lines + vec_chunks
table). The stale wording misled a deputy into proposing a numpy-only fix;
kept here as a breadcrumb so the evolution is traceable.
"""

import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

from config import SQLITE_DB, SEARCH, SERVICE_DIR, OPENCLAW_MEMORY_DIR

# ── MS-C (sqlite-vec): 派生 vec0 索引, 消灭整矩阵物化 ──────────────────────
# 真相源仍是 chunks 表; vec_chunks 是派生索引 (scripts/migrate_to_vec0.py 可重建).
# env MEMSVC_SEARCH_MODE=vec (默认) | legacy (秒级回滚).
# 启动时 _init_vec0() 建表, _check_vec_usable() 判是否有数据可走 vec 路径.
try:
    import sqlite_vec as _sqlite_vec
    _VEC_AVAILABLE = True
except ImportError:
    _VEC_AVAILABLE = False
    logging.getLogger("memory-service.db").warning(
        "[vec0] sqlite_vec 未安装 — 只能走 legacy 矩阵路径"
    )

logger = logging.getLogger("memory-service.db")

_feedback: dict[str, dict] = {}
_feedback_lock = threading.Lock()
_FEEDBACK_PATH = SERVICE_DIR / SEARCH.feedback_file

_conn_lock = threading.Lock()
_write_lock = threading.Lock()

# ── T-MEMSVC-IO-FIX-0930 #4: db_search 分段计时 (slow-query 归因不再靠 dump 猜) ──
# 线程局部段累加器: 检索入口 _seg_begin (嵌套不清外层), 热点段 _seg_add
# (bit fetch / COUNT / EXISTS / 向量 / 合并), 入口 _seg_collect 外露到
# server phase["db_seg_ms"] / tier_meta["seg_ms"] → monitor [slow-query] db[...]。
# 分工线程 (partition pools / _search_one worker) 各自 thread-local 为空 → 段
# 不计入外层, 由编排线程的外层段墙钟覆盖; 段单位 ms。无 ctx 时 _seg_add 为 no-op,
# 非检索调用点 (如 /api/stats) 零开销零污染。
_seg_ctx = threading.local()


def _seg_begin() -> bool:
    """开启分段计时上下文; 返回本层是否为创建者 (嵌套层不得带走外层数据)."""
    if getattr(_seg_ctx, "cur", None) is None:
        _seg_ctx.cur = {}
        return True
    return False


def _seg_add(key: str, seconds: float) -> None:
    ctx = getattr(_seg_ctx, "cur", None)
    if ctx is not None:
        ctx[key] = round(ctx.get(key, 0.0) + seconds * 1000.0, 1)


def _seg_collect(owned: bool) -> dict | None:
    if not owned:
        return None
    seg = getattr(_seg_ctx, "cur", None)
    _seg_ctx.cur = None
    return seg

# ── R1427 (2026-09-08, backlog item 1427 = 8/16 过渡期缓解第4项): 写峰×检索争抢缓解 ──
# 实证: 8/20 起 99.9% slow 为 db_search 主导 (62de7b7 phase 埋点); 全写者口径下
# slow ±300s 写相邻 38% → 写峰期 fsync/页缓存挤压是真实分量 (部分非全量)。
# 治法 = 机制层两件: ① upsert 分批+跨调用最小间隔 (见 upsert_chunks); ② 连接级
# WAL 调参 (见 _get_conn/_get_cold_conn)。全部 env 可回退, 旧值行为 = 置回默认。
# env 解析失败 (空串/非数值) 一律回退默认, 对齐 69c4bd4 "env 空串防崩" 先例。


def _env_num_r1427(name: str, default: float, kind=float):
    try:
        return kind(float(os.environ.get(name, "") or default))
    except (TypeError, ValueError):
        return default


_UPSERT_BATCH_MAX_ROWS = int(_env_num_r1427("MEMSVC_UPSERT_BATCH_MAX_ROWS", 500.0))
_UPSERT_MIN_INTERVAL_S = _env_num_r1427("MEMSVC_UPSERT_MIN_INTERVAL_MS", 150.0) / 1000.0
_WAL_AUTOCHECKPOINT = int(_env_num_r1427("MEMSVC_WAL_AUTOCHECKPOINT", 4000.0))
_JOURNAL_SIZE_LIMIT = int(_env_num_r1427("MEMSVC_JOURNAL_SIZE_LIMIT", 268435456.0))
_DB_SYNCHRONOUS = int(_env_num_r1427("MEMSVC_DB_SYNCHRONOUS", 1.0))
# 上次 upsert commit 的 monotonic 时刻 (受 _write_lock 保护; 进程内限流用。
# 跨进程写者各自节流 — 无协调器的机制层可达上限, 聚合写速已从无界降为有界)。
_upsert_last_commit_monotonic: float = 0.0

# ── T-MEMSVC-PARTITION-IMPL-0927 P4: vec0 KNN 并发闸 ──────────────────────────
# 0925 诊断 (T-MEMSVC-LATENCY-0925 §2): 慢查 = 每查扫描量 × 检索并发 (IO/CPU 争抢
# 超线性恶化)。闸只罩"执行一条 KNN SQL + fetch"叶子段 (_knn_fetch); 持闸期间不等待
# 任何 future / 不再取闸 → 结构上无死锁 (段并行的父线程只等 future, 不持闸)。
# env MEMSVC_KNN_CONCURRENCY: 默认 2; ≤0 = 不限 (秒级回退, 重启生效)。
_KNN_CONCURRENCY = int(_env_num_r1427("MEMSVC_KNN_CONCURRENCY", 2.0))
_knn_gate = threading.BoundedSemaphore(_KNN_CONCURRENCY) if _KNN_CONCURRENCY > 0 else None
_knn_gate_waits: deque = deque(maxlen=5000)   # (ts, wait_ms) — 只记等过闸的 (≥1ms)


def _knn_fetch(conn: sqlite3.Connection, sql: str, params: tuple) -> list:
    """vec0 KNN 统一执行入口 (P4 并发闸)。调用方负责连接生命周期。"""
    if _knn_gate is None:
        return conn.execute(sql, params).fetchall()
    t0 = time.perf_counter()
    with _knn_gate:
        waited_ms = (time.perf_counter() - t0) * 1000.0
        if waited_ms >= 1.0:
            _knn_gate_waits.append((time.time(), waited_ms))
        return conn.execute(sql, params).fetchall()


def knn_gate_stats(window_secs: float = 3600.0) -> dict:
    """KNN 闸观测 (health 低频读): 窗内等闸次数与等待分位。"""
    cutoff = time.time() - window_secs
    waits = sorted(w for ts, w in _knn_gate_waits.copy() if ts >= cutoff)
    return {
        "limit": _KNN_CONCURRENCY if _knn_gate is not None else 0,
        "waited": len(waits),
        "wait_ms": {"p50": _pctl(waits, 0.50), "p95": _pctl(waits, 0.95),
                    "max": _pctl(waits, 1.0)},
    }
# 2026-05-18: LRU cache (OrderedDict, 最多保留 N 个 collection 的 embeddings 常驻).
# 之前 dict 无淘汰, 多 collection 同时驻 → 内存堆积. 现在最近用过的留 N 个, 其他 evict.
# 单 collection ~100MB-2GB 量级, N=3 控制总常驻 < 5GB.
# 2026-06-08 (T-MEMSVC-SPEEDUP maintainer approval): 3→9. 实测 9 collections 总 embeddings 仅 538.9 MB
# (claude_code_sessions 189.2 / openclaw_sessions 141.0 / project_docs 77.8 / app_data 60.5
# / opencode_sessions 55.9 / agent_team 7.5 / opencode_memory 5.3 / opencode_workspace 1.4
# / opencode_config 0.3), 远 < 注释假设的 5GB 上限. 改 3→9 消除全库 search 的 cache miss
# IO 加载, 预期 12.5s → 1-3s (T-MEMSVC-READ-CONCURRENCY 实证 latency_breakdown).
#
# 2026-06-25 (T-MEMSVC-LRU-THRASH maintainer approval): 9→16, 且改为自适应.
# 根因: 9 是 6/8 按"当时恰好 9 个集合"硬编码的魔法数, 与集合数耦合却无校验.
# 6/24 16:47 新增第 10 个集合 zcode_sessions 后, 10 > 9 → 每次全库搜索 LRU 抖动,
# 重载 200MB 量级集合 ≈ 1.4s = 全部延迟. 修法: 上限 = max(16, 当前集合数+余量).
# 自适应防递归: 以后再加集合也不会撞线; 配 _assert_cache_covers_collections() 启动校验.
# 内存安全: 全 10 集合常驻实测 737MB, 留余量到 16 仍 ~750MB; watchdog 阈值 3GB + 5h 重启.
# LRU 缓冲含 W3 hw 变体条目余量 (小集合 tiered 路径的 `name::hw` 键)
_EMBEDDINGS_CACHE_FLOOR = 16  # 固定下限, 留未来加集合的余量
_EMBEDDINGS_CACHE_BUFFER = 10  # 自适应时在集合数之上再留的缓冲 (含 ::hw 变体)

# 启动时按当前集合数派生上限 (见 _init_cache_max), 避免硬编码魔法数与集合数脱钩.
_EMBEDDINGS_CACHE_MAX = _EMBEDDINGS_CACHE_FLOOR
# 缓存条目: (embs_ndarray, ids_list, snapshot_version)
# snapshot_version = 该快照加载时刻的 _collection_versions[name]; 用于检测快照过期
# (R403 治本: 让失效粒度从"全库 clear"收敛到"单集合过期").
_embeddings_cache: OrderedDict[str, tuple[np.ndarray, list[str], int]] = OrderedDict()

# Per-collection 写版本号 (R403 "numpy 快照版本号"): 每次 upsert/delete 命中某集合时
# 自增. _load_collection_embeddings 比对 "缓存快照版本 vs 当前版本", 不一致即重载.
# 作用: ① 显式标记过期 (不依赖"记得 pop"); ② 为后续增量合并 (不重载只 np.concatenate
# 新向量) 预留扩展点; ③ 日志可观测哪个集合被写了几次.
_collection_versions: dict[str, int] = {}

# 金层 id 缓存 (layered 模式专用, 避免每次 search 全表 json_extract 扫描).
# 按 collection 粒度随 _invalidate_cache(name) 失效. 首次 layered 慢, 后续命中缓存.
_gold_ids_cache: dict[str, set[str]] = {}

# L0804-S2 root cause ① (止血): 缓存结构锁.
# 背景: _embeddings_cache / _gold_ids_cache / _collection_versions 之前无锁, 并发
# search(HTTP 线程) × upsert(indexer 线程) 抢同一份 OrderedDict → 4 次 hang sample
# (data/memsvc_hang_dumps) 全堵在 lock_PyThread_acquire_lock, 线程 28→97 递增.
#
# 设计 (读多写少, 双检):
#   - 命中快路径 (cache hit): 无锁读 (GIL 保证单次 dict.get 原子); 返回的 numpy 数组
#     只读共享, 多线程并行余弦计算安全. move_to_end 用 try/except 容忍并发 pop 的 KeyError.
#   - 未命中慢路径 (cache miss): _cache_lock 串行化"DB读+numpy组装+store+evict".
#     ① 杜绝两个线程并发各加载 337MB (内存尖峰根因 ③, peak 10.7GB); ② 结构写原子.
#     代价: 不同集合的 miss 也串行, 但 warmup 后 miss 罕见, 可接受.
#   - 不锁 search 的矩阵乘 (embs @ q) —— 那是"锁范围放太大"红线; 只锁缓存结构本身.
_cache_lock = threading.Lock()

# ── T-MEMSVC-SLOWQUERY-CACHEMISS (2026-08-18): 矩阵缓存观测 (证伪/证实归因用) ──
# 问题: 慢查询(8-20s)是否由 "cache miss 持 _cache_lock 加载大集合" 引起?
# 铁约束 (L0804-S2 历史踩坑, 见 _load_collection_embeddings 注释): **hit 路径绝不
# 加锁** — 曾因给 hit 的 move_to_end 加锁导致 8 并发 avg 9s。
# 手法 (无锁累加): 只用 deque.append — CPython 下是 GIL 内原子 C 调用 (~百 ns):
#   - hit: 1 次 append(time.time()) 计数, 无共享 int += (非原子会丢计数)
#   - miss: 本就持 _cache_lock, 顺路记 wait_ms(等锁,含"等别人加载")/load_ms(自己加载)
#   - 读侧 (health 低频): len()/重算 + 按窗剪枝, 与 monitor._prune_search_events 同模式
# 事件元组: (ts, dur_s, collection, wait_ms, load_ms, rows, mb, outcome)
#   outcome: 'loaded'=自己加载 / 'peer_loaded'=等锁期间被别的线程先加载 (双检命中)
_matrix_cache_hits: deque = deque(maxlen=50000)
_matrix_cache_misses: deque = deque(maxlen=20000)


def _pctl(sorted_vals: list, p: float) -> float:
    if not sorted_vals:
        return 0.0
    i = min(int(len(sorted_vals) * p), len(sorted_vals) - 1)
    return round(sorted_vals[i], 1)


def matrix_cache_stats(window_secs: float = 3600.0) -> dict:
    """矩阵缓存滚动窗口统计 (读侧剪枝; health/monitor 低频调用)。"""
    cutoff = time.time() - window_secs
    while _matrix_cache_hits and _matrix_cache_hits[0] < cutoff:
        _matrix_cache_hits.popleft()
    while _matrix_cache_misses and _matrix_cache_misses[0][0] < cutoff:
        _matrix_cache_misses.popleft()
    # GLM-5.2 互审 #1 修: Python 3.14 deque 迭代遇并发 mutation 抛 RuntimeError,
    # 一律 .copy() 快照 (单条 C 调用不释放 GIL, 不走迭代器协议, 实证免疫) 后再遍历。
    snap = _matrix_cache_misses.copy()
    n_hit, n_miss = len(_matrix_cache_hits), len(snap)
    total = n_hit + n_miss
    loads = sorted(e[4] for e in snap)
    waits = sorted(e[3] for e in snap)
    by_coll: dict[str, int] = {}
    for e in snap:
        by_coll[e[2]] = by_coll.get(e[2], 0) + 1
    return {
        "window_seconds": window_secs,
        "hits": n_hit,
        "misses": n_miss,
        "hit_rate": round(n_hit / total, 4) if total else None,
        "load_ms": {"p50": _pctl(loads, 0.50), "p95": _pctl(loads, 0.95), "max": _pctl(loads, 1.0)},
        "wait_ms": {"p50": _pctl(waits, 0.50), "p95": _pctl(waits, 0.95), "max": _pctl(waits, 1.0)},
        "miss_by_collection": by_coll,
        "largest_miss_mb": round(max((e[6] for e in snap), default=0.0), 1),
        "recent_misses": [
            {
                "ts": round(e[0], 1), "collection": e[2],
                "wait_ms": round(e[3], 1), "load_ms": round(e[4], 1),
                "rows": e[5], "mb": round(e[6], 1), "outcome": e[7],
            }
            for e in list(snap)[-10:]
        ],
    }


def matrix_cache_overlaps(t_start: float, t_end: float) -> list[dict]:
    """返回 [t_start, t_end] 期间发生过的 miss 加载 (slow-query 归因判据)。

    并发安全 (GLM-5.2 互审 #1): Python 3.14 deque 裸迭代遇跨线程 append/popleft
    (stats 剪枝或 maxlen 隐式淘汰) 会抛 RuntimeError — 故一律 `.copy()` 快照后遍历
    (copy 是单条 C 调用, 持 GIL 不走迭代器协议, 8 线程混战实证零异常)。
    量级即判据: overlap 里最大 load_ms/wait_ms 若远小于查询耗时, 则慢因不在缓存加载。
    口径注: peer_loaded 事件的 dur_s 在等待者返回时截止, 低估 peer 加载窗口末端
    (归因偏保守=漏报方向, 不误报)。
    """
    out = []
    for e in _matrix_cache_misses.copy():
        m_start, m_end = e[0], e[0] + e[1]
        if m_start <= t_end and m_end >= t_start:
            out.append({
                "collection": e[2], "wait_ms": round(e[3], 1),
                "load_ms": round(e[4], 1), "rows": e[5],
                "mb": round(e[6], 1), "outcome": e[7],
            })
    return out


def _get_gold_ids(collection_name: str) -> set[str]:
    """返回该 collection 的金层 chunk id 集 (带缓存)."""
    # 快路径无锁读 (L0804-S2: 同 _load_collection_embeddings 的双检模式).
    cached = _gold_ids_cache.get(collection_name)
    if cached is not None:
        return cached
    conn = _get_conn()
    rows = conn.execute(
        "SELECT id FROM chunks WHERE collection=? "
        "AND json_extract(metadata,'$.layer')='gold'",
        (collection_name,),
    ).fetchall()
    conn.close()
    gold = {r[0] for r in rows}
    with _cache_lock:
        # 双检: 等锁期间另一线程可能已填入; 不覆盖 (set 语义等价).
        prior = _gold_ids_cache.get(collection_name)
        if prior is not None:
            return prior
        _gold_ids_cache[collection_name] = gold
    return gold


def _init_cache_max() -> int:
    """按当前集合数自适应设缓存上限 = max(FLOOR, n_collections + BUFFER).

    根治"加新集合忘了调缓存上限"的递归隐患 (金字塔'改X没更新依赖Y'闭环缺口):
    把'集合数'从硬编码魔法数改成运行时派生. 启动时 + 集合变更后调用.
    """
    global _EMBEDDINGS_CACHE_MAX
    try:
        n = len(list_collections())
    except Exception as e:
        logger.warning(f"[cache] _init_cache_max 取集合数失败, 用下限 {_EMBEDDINGS_CACHE_FLOOR}: {e}")
        n = 0
    new_max = max(_EMBEDDINGS_CACHE_FLOOR, n + _EMBEDDINGS_CACHE_BUFFER)
    if new_max != _EMBEDDINGS_CACHE_MAX:
        logger.info(f"[cache] 缓存上限自适应: {_EMBEDDINGS_CACHE_MAX} -> {new_max} (集合数={n})")
    _EMBEDDINGS_CACHE_MAX = new_max
    return _EMBEDDINGS_CACHE_MAX


def assert_cache_covers_collections() -> None:
    """启动期 sanity check: 缓存上限必须 >= 集合数, 否则全库搜索必抖动.

    防递归告警: 不满足直接 WARN (而非静默), 让运维一眼看到"又撞上限了".
    """
    try:
        n = len(list_collections())
    except Exception as e:
        logger.warning(f"[cache] assert 取集合数失败: {e}")
        return
    if _EMBEDDINGS_CACHE_MAX < n:
        logger.warning(
            f"[cache][ALERT] 缓存上限 {_EMBEDDINGS_CACHE_MAX} < 集合数 {n} → 全库搜索将 LRU 抖动! "
            f"请调高 _EMBEDDINGS_CACHE_FLOOR 或检查 _init_cache_max 是否被调用."
        )
    else:
        logger.info(f"[cache] sanity ok: 上限 {_EMBEDDINGS_CACHE_MAX} >= 集合数 {n}")


def _evict_lru_if_needed() -> None:
    """淘汰最久未访问的 collection.

    L0804-S2: 调用方必须已持有 _cache_lock (在 _load_collection_embeddings /
    _invalidate_cache 的 with 块内调用). 独立调用会破坏锁不变量。
    """
    while len(_embeddings_cache) > _EMBEDDINGS_CACHE_MAX:
        old_key, _ = _embeddings_cache.popitem(last=False)
        logger.info(f"LRU evict: {old_key} (cache size {len(_embeddings_cache)}/{_EMBEDDINGS_CACHE_MAX})")


def load_feedback() -> dict:
    global _feedback
    if _FEEDBACK_PATH.exists():
        try:
            with open(_FEEDBACK_PATH, "r", encoding="utf-8") as f:
                _feedback = json.load(f)
            logger.info(f"Loaded feedback for {len(_feedback)} chunks")
        except Exception as e:
            logger.warning(f"Failed to load feedback: {e}")
            _feedback = {}
    else:
        _feedback = {}
    return _feedback


def save_feedback() -> None:
    try:
        _FEEDBACK_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_FEEDBACK_PATH, "w", encoding="utf-8") as f:
            json.dump(_feedback, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Failed to save feedback: {e}")


def record_feedback(chunk_id: str, helpful: bool) -> None:
    with _feedback_lock:
        if chunk_id not in _feedback:
            _feedback[chunk_id] = {"helpful": 0, "not_helpful": 0}
        if helpful:
            _feedback[chunk_id]["helpful"] += 1
        else:
            _feedback[chunk_id]["not_helpful"] += 1
        save_feedback()
    logger.info(f"Feedback recorded: {chunk_id} helpful={helpful}")


def get_feedback_factor(chunk_id: str) -> float:
    with _feedback_lock:
        stats = _feedback.get(chunk_id, {})
        helpful = stats.get("helpful", 0)
        not_helpful = stats.get("not_helpful", 0)
    total = helpful + not_helpful
    if total == 0:
        return 1.0
    ratio = helpful / total
    return 0.7 + 0.6 * ratio


load_feedback()


# ── Fresh-install schema bootstrap ─────────────────────────────────────────
# Production databases were originally created by a migration script; a fresh
# install must bootstrap itself. Idempotent, runs once per process on the
# first connection. The dedup column/index (text_sha256) is added lazily by
# _ensure_dedup_schema on the first upsert.
_BASE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS collections (
    name TEXT PRIMARY KEY,
    config TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
    id TEXT PRIMARY KEY,
    collection TEXT NOT NULL,
    text TEXT NOT NULL,
    embedding BLOB NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    source TEXT NOT NULL DEFAULT '',
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chunks_collection ON chunks(collection);
CREATE INDEX IF NOT EXISTS idx_chunks_source ON chunks(source);
"""

_schema_ready = False
_schema_lock = threading.Lock()


def _ensure_base_schema(conn: sqlite3.Connection) -> None:
    """Create the base tables on a fresh database (idempotent, once/process)."""
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        conn.executescript(_BASE_SCHEMA_SQL)
        conn.commit()
        _schema_ready = True


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(SQLITE_DB), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    # R1427 WAL 调参 (连接级, 不改库文件持久属性):
    # - synchronous NORMAL: WAL 下 commit 不再逐个 fsync (fsync 收敛到 checkpoint) —
    #   写峰 IO 压力主削减项。代价: 仅 OS 崩溃/断电丢最后未 checkpoint 的事务
    #   (app 崩溃不丢); 记忆索引可由源文件重灌自愈 + 每日 04:40 备份兜底。回退=2。
    # - journal_size_limit 256MB: checkpoint 后截 WAL, 防读阻塞窗内无限涨。回退=-1。
    # - wal_autocheckpoint 4000 页(~16MB): 巨批写时 checkpoint 中断事件 4×减少。回退=1000。
    conn.execute(f"PRAGMA synchronous={_DB_SYNCHRONOUS}")
    conn.execute(f"PRAGMA journal_size_limit={_JOURNAL_SIZE_LIMIT}")
    conn.execute(f"PRAGMA wal_autocheckpoint={_WAL_AUTOCHECKPOINT}")
    # MS-C: vec0 虚表查询/写入需连接加载扩展 (无 vec_chunks 时这是 no-op, 极轻量 C 调用).
    if _VEC_AVAILABLE:
        try:
            conn.enable_load_extension(True)
            _sqlite_vec.load(conn)
        except Exception:
            logger.debug("[vec0] extension load failed on this conn", exc_info=True)
    _ensure_base_schema(conn)
    return conn


def _safe_name(name: str) -> str:
    return name.replace("-", "_").replace(".", "_").lower()[:63]


# ── MS-C: vec0 索引初始化与可用性判断 ──────────────────────────────────────
_vec_initialized = False
_vec_usable: bool | None = None  # None=未检; True/False=已判定 (进程内缓存)


def _init_vec0() -> None:
    """幂等建 vec_chunks 虚表 (IF NOT EXISTS). 不灌数据 (灌由 migrate_to_vec0.py)."""
    global _vec_initialized
    if _vec_initialized or not _VEC_AVAILABLE:
        return
    try:
        conn = _get_conn()
        conn.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS vec_chunks USING vec0("
            "chunk_id TEXT PRIMARY KEY, "
            "embedding float[1024] distance_metric=cosine, "
            "collection TEXT, "
            "layer TEXT"
            ")"
        )
        conn.commit()
        conn.close()
        _vec_initialized = True
        logger.info("[vec0] vec_chunks 虚表就绪 (init ok)")
    except Exception as e:
        logger.warning(f"[vec0] init 建表失败, 仅走 legacy: {e}")
        _vec_initialized = True  # 不再重试 (避免每次 search 都撞异常)


def _check_vec_usable() -> bool:
    """是否走 vec 搜索路径: ① sqlite_vec 已装 ② env != legacy ③ vec_chunks 有数据.

    进程内缓存 (部署序列: 先跑 migrate_to_vec0.py 再启服务 → 启动即就绪).
    若 migrate 未跑, 返回 False 自动降级 legacy, 不会空查.
    """
    global _vec_usable
    if _vec_usable is not None:
        return _vec_usable
    if not _VEC_AVAILABLE:
        _vec_usable = False
        return False
    if os.environ.get("MEMSVC_SEARCH_MODE", "vec").lower() == "legacy":
        logger.info("[vec0] MEMSVC_SEARCH_MODE=legacy → 强制走 legacy 路径")
        _vec_usable = False
        return False
    try:
        _init_vec0()
        conn = _get_conn()
        # W3: 按活动表集判 (tiered → hot 表; legacy → vec_chunks)
        probe_tbl = _active_vec_tables()[0]
        r = conn.execute(f"SELECT EXISTS(SELECT 1 FROM {probe_tbl} LIMIT 1)").fetchone()
        conn.close()
        _vec_usable = bool(r[0])
        if _vec_usable:
            logger.info(f"[vec0] {probe_tbl} 有数据 → 启用 vec 搜索路径")
        else:
            logger.warning(f"[vec0] {probe_tbl} 空 — 走 legacy (请跑 scripts/migrate_to_vec0.py 或 scripts/tier_rebuild.py)")
    except Exception as e:
        _vec_usable = False
        logger.warning(f"[vec0] 可用性检查失败 — 走 legacy: {e}")
    return _vec_usable


def verify_vec_reconcil(threshold: float = 0.001) -> dict:
    """vec 索引 vs chunks 行数对账 — 双路径完整性不变量校验.

    部署纪律→机制保证: 双写失败/颠倒部署/漏 migrate 都会造成行数偏差,
    本函数给监控提供对账依据 (偏差 > threshold 即告警).

    W3: tiered 布局对账三张分表并集 vs chunks (总数/ID 差集/跨表重复/分集合),
    另附 chunk_tier 注册表一致性与分层行数 — 机检"三表行数对账=真相层全量"的
    运行时锚。legacy 布局保持原 vec_chunks 单表口径。

    T-VECRECONCIL-TIERED (2026-08-22): tiered 是**全量物化**设计 (三表去重并集
    应等于 chunks 全量, chunk_tier 注册表逐 chunk 记录所在层 — 依据 W3 设计书 §五
    + tier_rebuild.verify 同口径)。故 tiered "对齐" = 三方一致:
      chunks ↔ 三表去重并集 ↔ chunk_tier 注册表 (行数与 tier 双向)
    外加保护不变量 gold_in_cold=0 (gold 越龄置热, 冷层出现 gold 即保护失效)。
    全部对账读包在单个显式读事务内 (WAL 快照隔离) — 否则 Python sqlite3 各 SELECT
    独立快照, 索引器写入落在两读之间会造瞬态 dup/unassigned 假差 (策划审 D1 同族)。
    旧 dup 实现 (两遍原始 COUNT 和相减) 静态库恒 0 (真跨表重复盲区)、活库测的是
    写入活动 (误报源) — 已改为 原始和−去重并集 (与 tier_rebuild.verify 同口径)。
    返回 dict 只增键不删键; rebuild 进行中的不完整快照属部署过渡态, 由调用方
    (scripts/verify_vec_reconcil.py) 按进程检测抑制, 本函数只报事实。
    """
    try:
        if not _VEC_AVAILABLE:
            # vec0 未安装/未激活 → 单路径, 无双写, 对账不适用 (如实报告, 不误报)
            return {"chunks": -1, "vec_chunks": -1, "drift": 0.0, "ok": True,
                    "threshold": threshold, "vec_enabled": False,
                    "error": "sqlite_vec 未安装 — 当前走 legacy 单路径"}
        _init_vec0()
        conn = _get_conn()
        # 布局单次捕获 (_active_vec_tables 有 60s TTL 缓存, 同次对账两次调用可能中途
        # 翻转 — 部署期撞上会造成 BEGIN/分支不对称)
        tiered = _tiered_layout()
        if tiered:
            # 单读事务: BEGIN 后首读起锁定 WAL 快照, 所有计数/差集在同一一致视图上
            conn.execute("BEGIN")
        n_chunks = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]

        if tiered:
            # 执行审D1: 读段全程 try/finally — 中途异常也必须 ROLLBACK+close
            # (fd 泄漏+读事务悬空到 GC, CC终审残留4 同族), tier_counts 起手建保 finally 可用
            tier_counts = {}
            try:
                for tr, t in _TIER_TABLE_BY_NAME.items():
                    tier_counts[tr] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                raw_sum = sum(tier_counts.values())
                conn.execute(
                    "CREATE TEMP TABLE tvec(chunk_id TEXT PRIMARY KEY, collection TEXT, tier TEXT)")
                for tr, t in _TIER_TABLE_BY_NAME.items():
                    conn.execute(
                        f"INSERT OR IGNORE INTO tvec SELECT chunk_id, collection, '{tr}' FROM {t}")
                n_vec = conn.execute("SELECT COUNT(*) FROM tvec").fetchone()[0]  # 去重并集
                # 跨表真重复 = 原始行数和 − 去重并集 (同 tier_rebuild.verify 口径)。
                # 稳态应恒 0: upsert 改层=三表全删再插+同事务, 翻热=INSERT hot+DELETE cold
                # +registry 同事务 → dup>0 即搬运残留/部分写崩溃等真异常。
                dup = raw_sum - n_vec
                miss = conn.execute(
                    "SELECT COUNT(*) FROM chunks c WHERE NOT EXISTS (SELECT 1 FROM tvec v WHERE v.chunk_id = c.id)"
                ).fetchone()[0]
                orphan = conn.execute(
                    "SELECT COUNT(*) FROM tvec v WHERE NOT EXISTS (SELECT 1 FROM chunks c WHERE c.id = v.chunk_id)"
                ).fetchone()[0]
                # chunk_tier 注册表一致性 (行数 + tier 与所在表一致)
                n_reg = conn.execute(f"SELECT COUNT(*) FROM {_TIER_REGISTRY}").fetchone()[0]
                reg_mismatch = conn.execute(
                    f"SELECT COUNT(*) FROM tvec v WHERE NOT EXISTS ("
                    f"SELECT 1 FROM {_TIER_REGISTRY} r WHERE r.chunk_id = v.chunk_id AND r.tier = v.tier)"
                ).fetchone()[0]
                # 反向差集 (诊断真因用): chunk 无注册行 = 写路径/分派缺; 注册行有但 tier 表缺行
                unassigned = conn.execute(
                    f"SELECT COUNT(*) FROM chunks c WHERE NOT EXISTS ("
                    f"SELECT 1 FROM {_TIER_REGISTRY} r WHERE r.chunk_id = c.id)"
                ).fetchone()[0]
                reg_no_vec = conn.execute(
                    f"SELECT COUNT(*) FROM {_TIER_REGISTRY} r WHERE NOT EXISTS ("
                    f"SELECT 1 FROM tvec v WHERE v.chunk_id = r.chunk_id)"
                ).fetchone()[0]
                # 保护不变量: gold 越龄置热 (upsert/rebuild 同规则), 冷层出现 gold 即保护失效
                gold_in_cold = conn.execute(
                    f"SELECT COUNT(*) FROM {_TIER_COLD_TABLE} WHERE layer='gold'"
                ).fetchone()[0]
                per_col = {}
                for row in conn.execute(
                    "SELECT collection, SUM(n_c), SUM(n_v) FROM ("
                    "SELECT collection, COUNT(*) AS n_c, 0 AS n_v FROM chunks GROUP BY collection "
                    "UNION ALL "
                    "SELECT collection, 0 AS n_c, COUNT(*) AS n_v FROM tvec GROUP BY collection"
                    ") GROUP BY collection"):
                    col, n_c, n_v = row
                    per_col[col] = {"chunks": n_c or 0, "vec": n_v or 0}
                drift = (n_vec - n_chunks) / max(n_chunks, 1)
                col_drift = max(
                    (abs(v["chunks"] - v["vec"]) / max(v["chunks"], 1)) for v in per_col.values()
                ) if per_col else 0.0
                # 三方一致 (chunks ↔ 去重并集 ↔ 注册表) + 保护不变量; threshold 语义不变
                # (只用于 drift/col_drift, 不放宽 — 换的是测量对象: 全量物化下并集=chunks)
                ok = (abs(drift) <= threshold and miss == 0 and orphan == 0
                      and dup == 0 and col_drift <= threshold
                      and n_reg == n_vec and reg_mismatch == 0
                      and unassigned == 0 and reg_no_vec == 0 and gold_in_cold == 0)
                # T-MEMDB-COLD-SPLIT: 冷库方对账 (仅 state=active; copying=迁移中途,
                # 部分拷贝是过渡态非故障 → 跳过防假警)。口径: 冷库三方一致 +
                # 主库 cold 注册行数 ≤ 冷库 chunks (cleanup 后=0≤N; 过渡期=N≤N;
                # delta 落后=N_main>N_cold → 报警提示补跑 migrate)。
                cold_report = None
                if _cold_split_state() == "active":
                    try:
                        cconn = _get_cold_conn()
                        cconn.execute("BEGIN")
                        c_chunks = cconn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
                        c_vec = cconn.execute(
                            f"SELECT COUNT(*) FROM {_TIER_COLD_TABLE}").fetchone()[0]
                        c_reg = cconn.execute(
                            f"SELECT COUNT(*) FROM {_TIER_REGISTRY} WHERE tier='cold'"
                        ).fetchone()[0]
                        c_gold = cconn.execute(
                            f"SELECT COUNT(*) FROM {_TIER_COLD_TABLE} WHERE layer='gold'"
                        ).fetchone()[0]
                        cconn.execute("ROLLBACK")
                        cconn.close()
                        main_cold_reg = tier_counts.get(_TIER_COLD_TABLE, 0)
                        cold_ok = (c_chunks == c_vec == c_reg and c_gold == 0
                                   and main_cold_reg <= c_chunks)
                        cold_report = {"chunks": c_chunks, "vec": c_vec,
                                       "registry_cold": c_reg, "gold_in_cold": c_gold,
                                       "main_cold_reg": main_cold_reg, "ok": cold_ok}
                        ok = ok and cold_ok
                    except Exception as e:
                        cold_report = {"ok": False, "error": str(e)}
                        ok = False
            finally:
                try:
                    # 读事务回滚 (TEMP tvec 一并回收; 对账只读无需提交)
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass  # 吞: BEGIN 可能因连接态失败/中途异常已断, ROLLBACK 非关键
                conn.close()
            return {"chunks": n_chunks, "vec_chunks": n_vec, "drift": drift,
                    "id_missing": miss, "id_orphan": orphan, "cross_tier_dup": dup,
                    "tier_counts": tier_counts, "tier_registry": n_reg,
                    "tier_registry_mismatch": reg_mismatch,
                    "unassigned": unassigned, "reg_no_vec": reg_no_vec,
                    "gold_in_cold": gold_in_cold, "raw_rows": raw_sum,
                    "col_max_drift": col_drift, "collections": len(per_col),
                    "ok": ok, "threshold": threshold, "vec_enabled": True,
                    "cold_db": cold_report,
                    "layout": "tiered"}

        n_vec = conn.execute("SELECT COUNT(*) FROM vec_chunks").fetchone()[0]
        # CC 终审残留3+4 (2026-08-15): ID 差集 + 分 collection 对账 (防行数相同ID不同/集合间抵消).
        # vec0 虚表普通列无索引 (73万×73万探针死慢), 先物化 ID 到临时普通表 (PK索引, ~11s).
        conn.execute("CREATE TEMP TABLE tvec(chunk_id TEXT PRIMARY KEY, collection TEXT)")
        conn.execute("INSERT INTO tvec SELECT chunk_id, collection FROM vec_chunks")
        miss = conn.execute(
            "SELECT COUNT(*) FROM chunks c WHERE NOT EXISTS (SELECT 1 FROM tvec v WHERE v.chunk_id = c.id)"
        ).fetchone()[0]
        orphan = conn.execute(
            "SELECT COUNT(*) FROM tvec v WHERE NOT EXISTS (SELECT 1 FROM chunks c WHERE c.id = v.chunk_id)"
        ).fetchone()[0]
        # CC终审REJECT-D必修: 逐集合对比 (tvec.collection 独立于 chunks 归属) —
        # 抓"vec 集合标错"(A 集合向量标成 B 集合); 双 GROUP BY + UNION 聚合, 索引友好
        per_col = {}
        try:
            for row in conn.execute(
                "SELECT collection, SUM(n_c), SUM(n_v) FROM ("
                "SELECT collection, COUNT(*) AS n_c, 0 AS n_v FROM chunks GROUP BY collection "
                "UNION ALL "
                "SELECT collection, 0 AS n_c, COUNT(*) AS n_v FROM tvec GROUP BY collection"
                ") GROUP BY collection"):
                col, n_c, n_v = row
                per_col[col] = {"chunks": n_c or 0, "vec": n_v or 0}
        finally:
            # CC终审残留4 (2026-08-15): 对账中途异常也关连接 (fd 泄漏, TEMP 表随连接销毁)
            try:
                conn.execute("DROP TABLE IF EXISTS tvec")
            except sqlite3.OperationalError:
                pass  # 吞: tvec 可能未创建 (对账早期异常), DROP 非关键
            conn.close()
        drift = (n_vec - n_chunks) / max(n_chunks, 1)
        col_drift = max((abs(v["chunks"] - v["vec"]) / max(v["chunks"], 1)) for v in per_col.values()) if per_col else 0.0
        ok = abs(drift) <= threshold and miss == 0 and orphan == 0 and col_drift <= threshold
        return {"chunks": n_chunks, "vec_chunks": n_vec, "drift": drift,
                "id_missing": miss, "id_orphan": orphan,
                "col_max_drift": col_drift, "collections": len(per_col),
                "ok": ok, "threshold": threshold, "vec_enabled": True,
                "layout": "legacy"}
    except Exception as e:
        logger.warning(f"[vec0] 对账失败: {e}")
        return {"chunks": -1, "vec_chunks": -1, "drift": 1.0, "ok": False,
                "threshold": threshold, "vec_enabled": True, "error": str(e)}


def _batch_delete_vec(conn: sqlite3.Connection, chunk_ids: list[str]) -> None:
    """分批删除 vec0 行 (SQLite 变量上限 900, 实际 999). 调用方管事务.

    W3: 活动表集全删 (tiered 三表或 legacy vec_chunks) + chunk_tier 同步删 —
    chunk 的层随写入漂移 (re-index 换层/翻热), 删除必须覆盖所有可能位置。
    """
    tables = list(_active_vec_tables())
    for i in range(0, len(chunk_ids), 900):
        batch = chunk_ids[i:i + 900]
        ph = ",".join("?" * len(batch))
        for t in tables:
            conn.execute(f"DELETE FROM {t} WHERE chunk_id IN ({ph})", batch)
        if _tiered_layout():
            conn.execute(f"DELETE FROM {_TIER_REGISTRY} WHERE chunk_id IN ({ph})", batch)


def _invalidate_cache(collection_name: str | None = None):
    """按 collection 粒度失效缓存 (R403 治本).

    根因 (data/research/p01b_hang_rootcause_20260803): 旧实现无参 .clear() 全库,
    任一集合写都连带清空其它 ~10 个集合 (~1.5GB) → 下次 search 全冷盘 pread 风暴
    → HTTP 超时卡死 (实测 dump 95% sample 在 pread). 改为只丢被写的那个集合,
    其它集合快照不动 → 跨集合污染消除.

    实现: ① 自增该集合版本号 (numpy 快照版本号, 标记过期); ② 从 LRU 缓存 pop 该
    集合快照 (立即释放内存, 缓解 RSS 峰值 7.4GB 引发的 OS 磁盘缓存驱逐); ③ 清该
    集合金层 id 缓存. collection_name=None 时退化为全清 (仅启动期/异常兜底用; 正常
    写路径三处调用点都传具体集合名).

    L0804-S2: 结构变更 (_collection_versions / _embeddings_cache / _gold_ids_cache)
    全程在 _cache_lock 内, 与 _load_collection_embeddings 的 miss 路径互斥, 杜绝
    "一边加载一边被 pop" 的竞态 (S1 hang sample 的结构性死锁源).

    T-FIX-T-ALERT-memory-service-degraded: 本函数是写路径统一失效尾钩 —
    同步失效 _main_counts_cache (collection_count 60s TTL 缓存), chunks 行数
    随写随新鲜, 不等 TTL 自然过期 (None 分支 = 全清, 与既有退化语义一致)。

    T-MEMSVC-FIX-E2: 同步失效 _main_cold_rows_cache (主库 cold 注册行探测缓存) —
    R1 修订 (复审 §一.1 回放: 无条件清使 1800s 档探测频率只降 2-3 倍, 大半被
    "写后清缓存"抵消): 只在缓存值为"有冷注册行"(True) 时清。依据 — 分库
    生效时进程内写入只会**减少**主库 cold 注册行 (冷行只写冷库, 见
    _upsert_one_batch cold 分支; 跨龄翻层行跳过主库), 缓存 False 不会被本
    进程写翻真, 保留 False 让无索引档 1800s 真正生效。方向安全: 真新增主库
    cold 行的路径全是外部进程 (tier_rebuild / cold_split 回滚 / 手工 sqlite3),
    由 TTL 兜底 + era 翻转钩子即时清 (见 _main_cold_rows_cache 块头)。
    """
    global _main_cold_rows_cache
    if _main_cold_rows_cache is not None and _main_cold_rows_cache[1]:
        _main_cold_rows_cache = None
    if collection_name is None:
        with _cache_lock:
            _embeddings_cache.clear()
            _gold_ids_cache.clear()
        _main_counts_cache.clear()
        return
    with _cache_lock:
        _collection_versions[collection_name] = _collection_versions.get(collection_name, 0) + 1
        _embeddings_cache.pop(collection_name, None)
        _embeddings_cache.pop(f"{collection_name}::hw", None)  # W3 tier 变体同失效
        _gold_ids_cache.pop(collection_name, None)
    # safe 归一防未来调用点传裸名 (opus5 留痕缺陷4: 现有 5 个调用点均传 safe 名,
    # 不归一时 Foo-Bar 类名字将静默失效不掉计数缓存); _safe_name 幂等。
    _main_counts_cache.pop(_safe_name(collection_name), None)


def _compute_collection_arrays(
    collection_name: str,
    cur_ver: int,
    tiers: tuple[str, ...] | None = None,
) -> tuple[np.ndarray, list[str]]:
    """从 SQLite 读 + numpy 组装 (embs, ids). 不碰缓存. 调用方持 _cache_lock.

    L0804-S2: 从 _load_collection_embeddings 抽出, 让"加载计算"与"缓存结构"分离 ——
    结构操作 (store/evict) 仍由调用方在锁内做, 但纯计算段可被未来优化 (memmap/增量合并)
    替换而不动锁逻辑。

    W3: tiers=('hot','warm') 时只装该层行 (chunk_tier JOIN) — tiered 默认路径的
    小集合矩阵不再滚全量 (一个全冷 60MB 小集合的死扫描即此治); None = 全量 (原行为)。
    """
    conn = _get_conn()
    if tiers is None:
        rows = conn.execute(
            "SELECT id, embedding FROM chunks WHERE collection=? ORDER BY id",
            (collection_name,),
        ).fetchall()
        # T-MEMDB-COLD-SPLIT: tiers=None = 全量语义 → 并入冷库同集合行 (主库优先去重,
        # 过渡期主库残留拷贝先服务, cleanup 后自然只剩冷库行)
        if _cold_split_active():
            try:
                cconn = _get_cold_conn()
                crows = cconn.execute(
                    "SELECT id, embedding FROM chunks WHERE collection=? ORDER BY id",
                    (collection_name,),
                ).fetchall()
                cconn.close()
                have = {r[0] for r in rows}
                rows.extend(r for r in crows if r[0] not in have)
            except Exception as e:
                _note_cold_degraded("compute_arrays_full", e)
    else:
        th = ",".join(f"'{t}'" for t in tiers)
        rows = conn.execute(
            f"SELECT c.id, c.embedding FROM chunks c "
            f"JOIN {_TIER_REGISTRY} t ON t.chunk_id = c.id AND t.tier IN ({th}) "
            f"WHERE c.collection=? ORDER BY c.id",
            (collection_name,),
        ).fetchall()
    conn.close()

    if not rows:
        return np.empty((0, 0), dtype=np.float32), []

    # Defensive load: skip rows with empty/null/wrong-dim embeddings.
    # Historical bug: claude_code_sessions/opencode_sessions accumulated
    # zero-byte embedding blobs from older indexer paths that upserted
    # before embed succeeded. Without this filter, np.array() raises
    # "inhomogeneous shape" and search fails for the entire collection.
    ids: list[str] = []
    emb_list: list[np.ndarray] = []
    expected_dim: int | None = None
    skipped_empty = 0
    skipped_wrongdim = 0
    for rid, blob in rows:
        if not blob:
            skipped_empty += 1
            continue
        e = np.frombuffer(blob, dtype=np.float32)
        if expected_dim is None:
            expected_dim = e.shape[0]
        elif e.shape[0] != expected_dim:
            skipped_wrongdim += 1
            continue
        ids.append(rid)
        emb_list.append(e)

    if not emb_list:
        if skipped_empty or skipped_wrongdim:
            logger.warning(
                f"Collection '{collection_name}' has 0 valid embeddings "
                f"(skipped: empty={skipped_empty}, wrong_dim={skipped_wrongdim})"
            )
        return np.empty((0, 0), dtype=np.float32), []

    embs = np.array(emb_list)
    norms = np.linalg.norm(embs, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embs = embs / norms

    msg = f"Loaded {len(ids)} embeddings for '{collection_name}' ({embs.nbytes/1024/1024:.1f}MB, ver={cur_ver})"
    if skipped_empty or skipped_wrongdim:
        msg += f" [skipped: empty={skipped_empty}, wrong_dim={skipped_wrongdim}]"
    logger.info(msg)
    return embs, ids


def _load_collection_embeddings(
    collection_name: str,
    tiers: tuple[str, ...] | None = None,
) -> tuple[np.ndarray, list[str]]:
    # L0804-S2 root cause ① 止血: 双检 + 锁 miss 路径。
    # 命中 (热路径, search 频繁走这里): 无锁读。GIL 保证单次 dict.get 原子; 返回的 numpy
    #   数组只读共享, 多 HTTP 线程并行做余弦乘 (embs @ q) 安全, 不进锁 → 不串行化查询。
    # 未命中 (冷路径, warmup 后罕见): _cache_lock 串行"读DB+组装+store+evict"。
    #   关键收益: 杜绝两线程并发各加载 337MB (内存尖峰根因 ③, peak 10.7GB → 单加载 337MB)。
    # W3: tiers 非 None → 缓存键加 "::hw" 后缀 (tier 过滤变体); 版本号仍按集合名失效
    # (写路径 _invalidate_cache 同步 pop 两个键)。
    cache_key = collection_name if tiers is None else f"{collection_name}::hw"
    cur_ver = _collection_versions.get(collection_name, 0)
    hit = _embeddings_cache.get(cache_key)
    if hit is not None and hit[2] == cur_ver:
        # LRU: 访问时移到末尾标为"最近用过"; 版本号匹配 = numpy 快照仍新鲜 (R403)。
        # 纯无锁: 单次 OrderedDict.move_to_end 是 GIL 内原子 C 调用, 与另一线程的
        # pop/popitem 不会交错 (GIL 串行 C 调用); 若 key 已被 evict/invalidate 删掉则
        # 抛 KeyError → 容忍 (LRU 顺序略偏, 不影响正确性)。
        # ★关键: 绝不能在此加 _cache_lock — 否则一个 miss 持锁 2s 会让所有并发 hit
        # 的 move_to_end 全堵 (L0804-S2 压测首版踩中: 8 并发 avg 9s)。
        try:
            _embeddings_cache.move_to_end(cache_key)
        except KeyError:
            pass
        # T-MEMSVC-SLOWQUERY-CACHEMISS: 无锁 hit 计数 (deque.append GIL 原子, ~百ns)。
        _matrix_cache_hits.append(time.time())
        return hit[0], hit[1]

    _mc_t0 = time.perf_counter()
    with _cache_lock:
        # 双检: 等锁期间另一线程可能已加载填入。重读版本号 (可能已被 invalidate 自增)。
        _mc_wait_ms = (time.perf_counter() - _mc_t0) * 1000.0
        cur_ver = _collection_versions.get(collection_name, 0)
        hit = _embeddings_cache.get(cache_key)
        if hit is not None and hit[2] == cur_ver:
            _embeddings_cache.move_to_end(cache_key)
            _matrix_cache_misses.append((
                time.time(), time.perf_counter() - _mc_t0, collection_name,
                _mc_wait_ms, 0.0, len(hit[1]), hit[0].nbytes / 1048576.0, "peer_loaded",
            ))
            return hit[0], hit[1]
        # 持锁加载 (串行化 miss): 锁覆盖 DB读+numpy组装+store+evict。
        # 代价: 不同集合的 miss 也互相等, 但 warmup 后 miss 罕见, 远好于内存尖峰。
        embs, ids = _compute_collection_arrays(collection_name, cur_ver, tiers)
        _mc_load_ms = (time.perf_counter() - _mc_t0) * 1000.0 - _mc_wait_ms
        _embeddings_cache[cache_key] = (embs, ids, cur_ver)
        _evict_lru_if_needed()
        _matrix_cache_misses.append((
            time.time(), time.perf_counter() - _mc_t0, collection_name,
            _mc_wait_ms, _mc_load_ms, len(ids), embs.nbytes / 1048576.0, "loaded",
        ))
        return embs, ids


# R1260 (r1198 §4-1/§6-P2): 进程内已建集合名缓存 — 消除搜索路径写锁暴露。
# 原逻辑每调一次都付 INSERT OR IGNORE + commit 写事务 (busy_timeout=5s), 搜索
# 热路径 (server._do_search) 每搜一写, MS-01/walk/backup 写窗内排队放大降级。
# 已建集合短路返回: 零连接零写零提交。
# 线程安全: CPython 下 set 的 in/add 原子; 竞争窗最坏 = 两线程同时 miss →
# 各跑一次 INSERT OR IGNORE (OR IGNORE 幂等), 与原逻辑行为一致, 收敛后零 DB。
# 失效语义: 生产代码无 DELETE FROM collections 路径 (仅测试脚本独立进程直删,
# 不共享本缓存); 若未来新增删除注册行功能, 必须同步 _known_collections.discard。
_known_collections: set[str] = set()


def get_or_create_collection(project_name: str) -> str:
    name = _safe_name(project_name)
    if name in _known_collections:
        return name
    conn = _get_conn()
    cur = conn.execute(
        "INSERT OR IGNORE INTO collections (name, config, created_at) VALUES (?, '{}', ?)",
        (name, time.time()),
    )
    created = cur.rowcount > 0
    conn.commit()
    conn.close()
    _known_collections.add(name)
    # 自适应: 新建集合后重算缓存上限, 杜绝"加集合忘改缓存"的 off-by-one 抖动 (2026-06-25).
    if created:
        _init_cache_max()
    return name


def count_collection_chunks(collection_name: str) -> int:
    """T-MEMSVC-WALK-SPEEDUP C2: 缓存-DB 绑定兜底用 — 返回集合 chunk 总数 (0=空/被重建)。
    沿用 _get_conn/conn.close 现有姿势; 只读一条 COUNT, 代价可忽略。
    T-MEMDB-COLD-SPLIT: 并冷库行数 (口径同 collection_count: 主库排除 cold 层防双算)。"""
    conn = _get_conn()
    _t0 = time.perf_counter()
    try:
        if _cold_split_active():
            # T-MEMSVC-FIX-E2 (C1a): 同 collection_count 短路口径 (零 cold 注册行
            # → plain COUNT 同值; E1 实测反连接 900 倍差)。
            if _main_has_cold_registry_rows(conn):
                n = conn.execute(
                    f"SELECT COUNT(*) FROM chunks c WHERE c.collection = ? "
                    f"AND NOT EXISTS (SELECT 1 FROM {_TIER_REGISTRY} t "
                    f"WHERE t.chunk_id = c.id AND t.tier = 'cold')",
                    (_safe_name(collection_name),),
                ).fetchone()[0]
            else:
                n = conn.execute(
                    "SELECT COUNT(*) FROM chunks WHERE collection = ?",
                    (_safe_name(collection_name),),
                ).fetchone()[0]
        else:
            n = conn.execute(
                "SELECT COUNT(*) FROM chunks WHERE collection = ?",
                (_safe_name(collection_name),),
            ).fetchone()[0]
    finally:
        _seg_add("cnt_ms", time.perf_counter() - _t0)
        conn.close()
    return n + _cold_collection_counts().get(_safe_name(collection_name), 0)


# ── T-INGEST-DEDUP (2026-07-14): 入库侧同库判重, 防 MEM-C5 批删后 re-index 再生副本 ──
#   背景: C5 批1删除后数小时, 同一卡在同 collection 被新会话 re-index 再生副本 → 不堵入库源头批删白删。
#   口径: raw sha256(text.encode utf-8, replace) —— 与 MEM-C5 扫描同口径(实测抽样命中其 dup 组 hash)。
#   不用 metadata.chunk_hash(全库仅 76% 覆盖=24%腐烂, 无法可靠去重); 用 text 现算(text 恒存在)。
#   仅同 collection 内判重; 跨 collection 重复交批任务定期扫(本单不管, 铁丝网)。
_dedup_schema_ready = False


def _text_sha256(text: str) -> str:
    """文本 sha256 (与 MEM-C5 扫描同口径: raw utf-8, errors=replace, 全 64 hex)。"""
    return hashlib.sha256((text or "").encode("utf-8", errors="replace")).hexdigest()


def _ensure_dedup_schema() -> None:
    """幂等一次性(毫秒级): 加 text_sha256 列 + (collection,text_sha256) 索引。首个 upsert 触发。

    R2 (GLM 互审 P0-1 裁定): **回填移出请求路径** —— 268K 行回填放这里会长占 _write_lock 阻塞并发写
    (P0-2 锁竞争)。改由独立离线脚本 `scripts/ops/memsvc_backfill_sha256.py`(白天错峰跑)。此处只建
    schema(ALTER/CREATE INDEX 是元数据级毫秒操作); 若检测到存量 NULL 行 → 日志提醒跑离线脚本,
    **绝不在请求路径自动回填**。回填必要性: 去重要对上 C5 保留的存量副本(它们无 sha256)。"""
    global _dedup_schema_ready
    if _dedup_schema_ready:
        return
    with _write_lock:
        if _dedup_schema_ready:
            return
        conn = _get_conn()
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(chunks)")]
            if "text_sha256" not in cols:
                conn.execute("ALTER TABLE chunks ADD COLUMN text_sha256 TEXT")
                conn.commit()
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_dedup ON chunks(collection, text_sha256)")
            conn.commit()
            n_null = conn.execute("SELECT COUNT(*) FROM chunks WHERE text_sha256 IS NULL").fetchone()[0]
            if n_null:
                logger.warning(
                    f"dedup: {n_null} 行 text_sha256 为空 —— 判重对这些存量行会漏判。"
                    f"请离线跑 `python3 scripts/ops/memsvc_backfill_sha256.py` 回填(不在请求路径自动回填)。"
                )
        finally:
            conn.close()
        _dedup_schema_ready = True


def upsert_chunks(collection_name: str, chunks: list[dict], skip_dedup: bool = False):
    if not chunks:
        return
    name = _safe_name(collection_name) if not collection_name.startswith("_") else collection_name
    _ensure_dedup_schema()   # T-INGEST-DEDUP: 幂等建列/索引/回填(首个 upsert 一次性)
    now = time.time()

    # T-INGEST-DEDUP: 算每条 incoming 的 sha256, 批量查同 collection 已存在的 hash → 跳过重复不插入。
    # R2 语义裁定(cross-review P1-3, orchestrator decision, 有意为之): 精确同文即跳过, **不做 metadata merge** ——
    #   这正是 MEM-C5 设计 R4 "每组保留一份(信息全的 md 侧)" 的入库侧对偶: 重复卡的会话级 metadata
    #   对语义检索无额外价值, merge 只徒增写放大。跳过=保留库内已有那一份, 语义等价 C5"留一删余"。
    #   (详见 docs/plans/reviews/mem_c5_dedup_list_20260714.md 保留侧规则 R4。) skip 计数照记(下方日志)。
    #
    # T-MEMINDEX-STEP2 (2026-08-06): 加 skip_dedup 形参 — 调用方已完成 delete_session_chunks +
    # get_existing_embeddings 跨 session 复用决策时, 显式跳过内置 dedup (跨 session 同文本块
    # 各自保留一份, 因 export 按 session_id 分组, 每个 sid 都要有自己的代表块)。
    # default False 不影响其他调用方 (indexer.py / migrate_to_sqlite.py 等)。
    for c in chunks:
        c["_dedup_sha"] = _text_sha256(c.get("text", ""))
    _incoming = list({c["_dedup_sha"] for c in chunks})
    _existing_hashes: set[str] = set()
    if _incoming and not skip_dedup:
        _cx = _get_conn()
        try:
            for _i in range(0, len(_incoming), 900):   # SQLite 变量上限, 分批 IN 查询
                _b = _incoming[_i:_i + 900]
                _q = ("SELECT text_sha256 FROM chunks WHERE collection=? AND text_sha256 IN "
                      f"({','.join('?' * len(_b))})")
                for (_h,) in _cx.execute(_q, (name, *_b)):
                    _existing_hashes.add(_h)
        finally:
            _cx.close()
        # T-MEMDB-COLD-SPLIT: 冷行 hash 在冷库 — 并入判重集 (防跨库重复入库)
        if _cold_split_active() and len(_existing_hashes) < len(_incoming):
            _remaining = [h for h in _incoming if h not in _existing_hashes]
            try:
                _cc = _get_cold_conn()
                try:
                    for _i in range(0, len(_remaining), 900):
                        _b = _remaining[_i:_i + 900]
                        _q = ("SELECT text_sha256 FROM chunks WHERE collection=? AND text_sha256 IN "
                              f"({','.join('?' * len(_b))})")
                        for (_h,) in _cc.execute(_q, (name, *_b)):
                            _existing_hashes.add(_h)
                finally:
                    _cc.close()
            except Exception as _e:
                _note_cold_degraded("upsert_dedup_check", _e)

    rows = []
    rejected = 0
    skipped_dup = 0
    _seen_in_batch: set[str] = set()
    for c in chunks:
        h = c["_dedup_sha"]
        if not skip_dedup and (h in _existing_hashes or h in _seen_in_batch):   # 同库已存在 或 本批内重复 → 跳过
            skipped_dup += 1
            continue
        meta = c.get("metadata", {})
        source = meta.get("source", "")
        # RETAIN-LAYER-CARRY: 写入侧补 layer, 防 E1 回填(117044行→session)被 re-index 冲掉。
        # 与 _resolve_layer_for_vec 同口径: metadata.layer 优先, 缺则 classify_layer(source)。
        if not meta.get("layer"):
            meta = dict(meta, layer=classify_layer(source))
        emb = c.get("embedding", [])
        # Refuse empty embeddings — historically these accumulated as 0-byte
        # BLOBs that broke search loading. Caller should retry or skip.
        # 5/18: emb 现在可能是 numpy ndarray (来自 get_existing_chunks), 不能用
        # `if not emb` (numpy 抛 "truth value of array is ambiguous"). 改 len()/None 检查.
        if emb is None or len(emb) == 0:
            rejected += 1
            continue
        _seen_in_batch.add(h)   # 确认将插入才记入本批已见(空 embedding 被 reject 的不记, 免误跳后续同 hash)
        emb_blob = np.array(emb, dtype=np.float32).tobytes()
        rows.append((
            c["id"],
            name,
            c["text"],
            emb_blob,
            json.dumps(meta, ensure_ascii=False),
            source,
            now,
            h,
        ))

    if rejected:
        logger.warning(
            f"upsert_chunks: rejected {rejected} chunk(s) with empty embeddings "
            f"in collection '{name}' (caller should retry embed)"
        )
    if skipped_dup:   # T-INGEST-DEDUP: 每批一行日志记跳过数
        logger.info(f"dedup: 跳过 {skipped_dup} 条同库重复 chunk (collection={name})")
    if not rows:
        return

    # R1427 (8/16 第4项 错峰/限流): 单一写咽喉处分批 + 跨调用最小间隔。
    # - 分批: 巨批 (单文件千级 chunks) 切成 ≤_UPSERT_BATCH_MAX_ROWS 的批, 每批独立
    #   事务 → WAL 单次增量/checkpoint 粒度有界; 原子性从文件级收敛为批级
    #   (upsert 幂等 + 崩溃后下轮重灌, 无丢行窗)。
    # - 限流: 距本进程上次 upsert commit < _UPSERT_MIN_INTERVAL_S 则等齐 — 持
    #   _write_lock 睡眠 = 同进程写者被节流 (预期); 检索不取此锁, 不受影响;
    #   跨进程写者 (cron 索引器/导出器) 各自节流, 聚合写速从无界背靠背降为有界。
    # - 回退: MEMSVC_UPSERT_BATCH_MAX_ROWS=0 不分批 / MEMSVC_UPSERT_MIN_INTERVAL_MS=0 不等。
    with _write_lock:
        global _upsert_last_commit_monotonic
        _batch_max = _UPSERT_BATCH_MAX_ROWS if _UPSERT_BATCH_MAX_ROWS > 0 else max(len(rows), 1)
        _batches = [rows[i:i + _batch_max] for i in range(0, len(rows), _batch_max)]
        _paced_single = False   # 复审N2: 单批但发生跨调用等待时也要可观测 ([paced] 标)
        for _batch in _batches:
            if _upsert_last_commit_monotonic and _UPSERT_MIN_INTERVAL_S > 0:
                _delta = _UPSERT_MIN_INTERVAL_S - (time.monotonic() - _upsert_last_commit_monotonic)
                if _delta > 0:
                    time.sleep(_delta)
                    _paced_single = True
            _upsert_one_batch(_batch)
            _upsert_last_commit_monotonic = time.monotonic()
    _invalidate_cache(name)
    logger.info(
        f"Upserted {len(rows)} chunks into {name} (skipped {skipped_dup} dup)"
        + (f" [{len(_batches)} batches paced]" if len(_batches) > 1 else "")
        + (" [paced]" if _paced_single and len(_batches) == 1 else "")
    )


def _upsert_one_batch(rows: list[tuple]):
        """R1427: 单批写入 = 原 upsert_chunks 锁内体原样搬入 (rows = 批切片)。

        调用方必须已持 _write_lock; 批级原子 (upsert 幂等, 中断后下轮重灌)。
        (体保持原 8 空格缩进原样搬入, 减少搬移失真面)
        """
        conn = _get_conn()
        # T-MEMDB-COLD-SPLIT: 分库 active 时 cold 层行不写主库 (单一归属: 冷行真相在
        # 冷库)。写序 = 冷库先 commit, 主库后清同 id 旧拷贝 — 中途崩溃最坏态是
        # "主库残留旧拷贝(照常可读)", 不存在丢行窗 (零删除铁律)。
        # ⚠️ 归属判定只看 数据状态 (split state + tiered 布局), 不看 _vec_initialized —
        # 索引器进程可能未跑过任何 search (vec 未初始化), 若耦合会把冷行静默落主库。
        cold_active = _tiered_layout() and _cold_split_active()
        if cold_active:
            tiers_pre = [tier_for_chunk(r[4], r[5], r[6]) for r in rows]
            main_rows = [r for r, tr in zip(rows, tiers_pre) if tr != "cold"]
            cold_rows = [r for r, tr in zip(rows, tiers_pre) if tr == "cold"]
        else:
            main_rows = rows
            cold_rows = []
        if main_rows:
            conn.executemany(
                "INSERT OR REPLACE INTO chunks (id, collection, text, embedding, metadata, source, updated_at, text_sha256) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                main_rows,
            )
        # MS-C: vec0 双写 (同事务保一致). vec0 无 REPLACE → 先 DELETE 旧再 INSERT 新
        # (副官点名的 REPLACE 坑). rows 元组: (id, coll, text, emb_blob, meta_json, source, now, sha)
        # W3: tiered 布局按 chunk 源时间戳分层写入对应 tier 表 + chunk_tier 注册
        # (re-index 旧内容按真实源年龄落温/冷, 不占热层); legacy 布局行为不变。
        if _VEC_AVAILABLE and not _vec_initialized:
            _init_vec0()   # 兜底: 模块尾应已初始化; 防测试/异常复位后整块静默跳过
        if _VEC_AVAILABLE and _vec_initialized:
            try:
                tiered = _tiered_layout()
                if tiered:
                    all_ids = [(r[0],) for r in rows]
                    for t in (_TIER_HOT_TABLE, _TIER_WARM_TABLE, _TIER_COLD_TABLE):
                        conn.executemany(
                            f"DELETE FROM {t} WHERE chunk_id=?",
                            all_ids,
                        )
                    tiers = [tier_for_chunk(r[4], r[5], r[6]) for r in main_rows]
                    by_tier: dict[str, list[tuple]] = {"hot": [], "warm": [], "cold": []}
                    _main_reg: list[tuple] = []
                    _flip_strays = 0
                    for r, tr in zip(main_rows, tiers):
                        if cold_active and tr == "cold":
                            # 边界翻层 (tiers_pre 判非冷, 重算判冷 — 跨过年龄阈值):
                            # 罕见; 分库态此行已由冷库侧接管, 跳过主库写入待自愈
                            _flip_strays += 1
                            continue
                        by_tier[tr].append(
                            (r[0], r[3], r[1], _resolve_layer_for_vec(r[4], r[5]))
                        )
                        _main_reg.append((r[0], tr, r[6]))
                    if _flip_strays:
                        logger.warning(
                            f"[cold-split] {_flip_strays} 行在写入途中翻层 (年龄边界), "
                            f"已跳过其主库 vec 写入, 待对账/下轮自愈")
                    for tr, vec_rows in by_tier.items():
                        if vec_rows:
                            conn.executemany(
                                f"INSERT INTO {_TIER_TABLE_BY_NAME[tr]}"
                                "(chunk_id, embedding, collection, layer) VALUES (?,?,?,?)",
                                vec_rows,
                            )
                    conn.executemany(
                        f"INSERT OR REPLACE INTO {_TIER_REGISTRY}"
                        "(chunk_id, tier, reason, assigned_at) VALUES (?,?,'upsert',?)",
                        _main_reg,
                    )
                    if cold_rows:
                        # 主库清同 id 旧拷贝 (upsert 替换语义; 冷库新行已先行落地)
                        conn.executemany(
                            "DELETE FROM chunks WHERE id=?",
                            [(r[0],) for r in cold_rows],
                        )
                        conn.executemany(
                            f"DELETE FROM {_TIER_REGISTRY} WHERE chunk_id=?",
                            [(r[0],) for r in cold_rows],
                        )
                else:
                    conn.executemany(
                        "DELETE FROM vec_chunks WHERE chunk_id=?",
                        [(r[0],) for r in rows],
                    )
                    vec_rows = [
                        (r[0], r[3], r[1], _resolve_layer_for_vec(r[4], r[5]))
                        for r in rows
                    ]
                    conn.executemany(
                        "INSERT INTO vec_chunks(chunk_id, embedding, collection, layer) "
                        "VALUES (?,?,?,?)",
                        vec_rows,
                    )
            except Exception:
                logger.warning("[vec0] 双写失败 (vec 表可能未建) — 将触发对账告警", exc_info=True)
        if cold_active and cold_rows:
            # 冷库先落地 (零丢失序: 冷库 commit 成功后, 主库才提交"清同 id 旧拷贝")。
            # chunks+registry 恒写 (真相层归属); vec 行仅索引就绪时写 (缺=对账暴露)。
            cconn = _get_cold_conn()
            try:
                if _VEC_AVAILABLE and _vec_initialized:
                    cconn.executemany(
                        "DELETE FROM vec_chunks_cold WHERE chunk_id=?",
                        [(r[0],) for r in cold_rows],
                    )
                cconn.executemany(
                    "INSERT OR REPLACE INTO chunks (id, collection, text, embedding, metadata, source, updated_at, text_sha256) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    cold_rows,
                )
                if _VEC_AVAILABLE and _vec_initialized:
                    cconn.executemany(
                        "INSERT INTO vec_chunks_cold(chunk_id, embedding, collection, layer) "
                        "VALUES (?,?,?,?)",
                        [(r[0], r[3], r[1], _resolve_layer_for_vec(r[4], r[5])) for r in cold_rows],
                    )
                cconn.executemany(
                    f"INSERT OR REPLACE INTO {_TIER_REGISTRY}"
                    "(chunk_id, tier, reason, assigned_at) VALUES (?,?,'upsert',?)",
                    [(r[0], "cold", r[6]) for r in cold_rows],
                )
                cconn.commit()
                _invalidate_cold_counts()
            except Exception as e:
                # 冷库写失败 = 该批记忆面临丢失 → fail-loud (indexer 下轮重试), 禁吞
                conn.rollback()
                conn.close()
                raise RuntimeError(
                    f"[cold-split] 冷库写入失败 ({len(cold_rows)} 行) — 本批整体回滚, "
                    f"请检查 {COLD_DB_PATH}: {e}"
                ) from e
            finally:
                cconn.close()
        conn.commit()
        conn.close()


def delete_by_source(collection_name: str, file_path: str):
    name = _safe_name(collection_name)
    with _write_lock:
        conn = _get_conn()
        # MS-C: 先取 id 再删 (vec0 按 chunk_id 删, 无 source 列)
        ids_to_clean: list[str] = []
        if _VEC_AVAILABLE and _vec_initialized:
            ids_to_clean = [
                r[0] for r in conn.execute(
                    "SELECT id FROM chunks WHERE collection=? AND source=?",
                    (name, file_path),
                ).fetchall()
            ]
        conn.execute(
            "DELETE FROM chunks WHERE collection=? AND source=?",
            (name, file_path),
        )
        if ids_to_clean:
            _batch_delete_vec(conn, ids_to_clean)
        conn.commit()
        conn.close()
        # T-MEMDB-COLD-SPLIT: 冷库同源行同删 (旧文件的冷块归冷库)
        if _cold_split_active():
            try:
                cconn = _get_cold_conn()
                try:
                    cids = [r[0] for r in cconn.execute(
                        "SELECT id FROM chunks WHERE collection=? AND source=?",
                        (name, file_path),
                    ).fetchall()]
                    if cids:
                        _cold_delete_ids(cids, conn=cconn)
                finally:
                    cconn.close()
            except Exception as e:
                _note_cold_degraded("delete_by_source", e)
    _invalidate_cache(name)
    logger.info(f"Deleted chunks for {file_path} from {name}")


# T-MEMINDEX-STEP2 (2026-08-06, maintainer-approved plan B): 会话级删旧重插 helpers
# 消灭 dedup-pre 制造的混合版本孤儿块 (export_to_openclaw.py:70/73 取 chunks[0]
# 的 version 判陈旧会被旧 V1 孤儿击穿 → OpenClaw MD 永久不更新)。
# 配套: hash 相同的块跨 session 复用库内已有 embedding (不调 ollama)。
# 详见 docs/plans/T-MEMINDEX-STEP2-20260806.md + memory-service/A-MEMINDEX-DESIGN-20260805.md
def delete_session_chunks(
    collection_name: str, session_id: str, except_ids: set[str] | None = None
) -> int:
    """删该 session 全部 chunk (任意 version, 含孤儿)。

    用于 version 变动重插后清孤儿。except_ids 非空时, 保留这些 id (新插入的),
    只删该 session 其余 chunk (旧版本残留的孤儿)。

    范式对齐 delete_by_source:606 + delete_chunks_by_ids:1349。
    按 metadata.session_id 精确删 (vec_chunks 无 metadata 列, 需先 SELECT id 再双删)。

    Returns: 删除条数 (chunks 表; vec_chunks 同步删)。
    """
    name = _safe_name(collection_name) if not collection_name.startswith("_") else collection_name
    keep = set(except_ids or [])
    with _write_lock:
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT id FROM chunks WHERE collection = ? "
                "AND json_extract(metadata, '$.session_id') = ?",
                (name, session_id),
            ).fetchall()
            ids = [r[0] for r in rows]
            # D4 sanity: 所有 id 应是 chunk_id 前缀 ocdb_{sid[:16]}_
            # (防极端撞前缀误删他 session; 撞概率几乎 0 但加防御无害)
            expected_prefix = f"ocdb_{session_id[:16]}_"
            bad = [i for i in ids if not i.startswith(expected_prefix)]
            if bad:
                logger.warning(
                    f"delete_session_chunks: {len(bad)} id 不匹配前缀 {expected_prefix} "
                    f"(sid={session_id[:16]}), 跳过这些不删"
                )
                ids = [i for i in ids if i.startswith(expected_prefix)]
            to_delete = [i for i in ids if i not in keep]
            if not to_delete:
                conn.close()
                return 0
            placeholders = ",".join("?" * len(to_delete))
            conn.execute(f"DELETE FROM chunks WHERE id IN ({placeholders})", to_delete)
            # MS-C: vec0 同步删 (chunk_id 是 vec_chunks 主键, 直接 IN 删)
            if _VEC_AVAILABLE and _vec_initialized:
                try:
                    _batch_delete_vec(conn, to_delete)
                except Exception:
                    # flash 第2轮代码审 LOW: 改 warning (原 debug), 便于生产排查 vec_chunks 残留孤儿
                    logger.warning(
                        "[vec0] delete_session_chunks 同步删 vec_chunks 失败 "
                        f"(sid={session_id[:16]}, n={len(to_delete)}) — chunks 已删但 vec_chunks 可能残留",
                        exc_info=True,
                    )
            conn.commit()
            # T-MEMDB-COLD-SPLIT: 冷库同 session 行同删 (同前缀护栏口径)
            if _cold_split_active():
                try:
                    cconn = _get_cold_conn()
                    try:
                        crows = cconn.execute(
                            "SELECT id FROM chunks WHERE collection = ? "
                            "AND json_extract(metadata, '$.session_id') = ?",
                            (name, session_id),
                        ).fetchall()
                        cids = [r[0] for r in crows if r[0].startswith(expected_prefix)
                                and r[0] not in keep]
                        if cids:
                            _cold_delete_ids(cids, conn=cconn)
                    finally:
                        cconn.close()
                except Exception as e:
                    _note_cold_degraded("delete_session_chunks", e)
        finally:
            conn.close()
    _invalidate_cache(name)
    return len(to_delete)


def get_existing_embeddings(
    collection_name: str, text_hashes: list[str]
) -> dict[str, bytes]:
    """查同 collection 同 hash 已有 chunk 的 embedding blob (跨 session 复用)。

    T-MEMINDEX-STEP2 maintainer directive配套: "hash 相同的块直接复用库里已有的向量(不调 ollama)"。

    前提 (flash 二审 D2 认可): 当前 collection 的 embedding 维度必须一致 (bge-m3:latest
    = 1024 dim = 4096 bytes)。本函数加 dim 校验: length(embedding) != 4096 的行跳过,
    历史混入其他维度时不复用, 安全降级到 embed_batch。
    若将来 embedding 模型升级, 必须先全量重 embed 再开复用, 或在 metadata 加 embed_model
    字段做版本校验。

    Returns: {text_sha256: embedding_blob} (仅含 dim 一致的已存在 hash)。
    """
    if not text_hashes:
        return {}
    name = _safe_name(collection_name) if not collection_name.startswith("_") else collection_name
    _ensure_dedup_schema()  # 确保 text_sha256 列/索引就绪
    EXPECTED_DIM_BYTES = 1024 * 4   # bge-m3 = 1024 floats × 4 bytes (flash 二审 D2 认可)
    result: dict[str, bytes] = {}
    conn = _get_conn()
    try:
        for i in range(0, len(text_hashes), 900):  # SQLite 变量上限 999, 留余量
            batch = text_hashes[i:i + 900]
            placeholders = ",".join("?" * len(batch))
            rows = conn.execute(
                f"SELECT text_sha256, embedding FROM chunks "
                f"WHERE collection = ? AND text_sha256 IN ({placeholders}) "
                f"AND embedding IS NOT NULL AND length(embedding) = ?",
                (name, *batch, EXPECTED_DIM_BYTES),
            ).fetchall()
            for sha, emb in rows:
                if sha not in result:
                    # 同 hash 多份取首份 (语义等价, 重复 embedding 是冗余)
                    result[sha] = emb
    finally:
        conn.close()
    # T-MEMDB-COLD-SPLIT: 冷行的同 hash 向量在冷库 — 补捞 (复用不降级)
    if _cold_split_active() and len(result) < len(set(text_hashes)):
        remaining = [h for h in dict.fromkeys(text_hashes) if h not in result]
        try:
            cconn = _get_cold_conn()
            try:
                for i in range(0, len(remaining), 900):
                    batch = remaining[i:i + 900]
                    placeholders = ",".join("?" * len(batch))
                    crows = cconn.execute(
                        f"SELECT text_sha256, embedding FROM chunks "
                        f"WHERE collection = ? AND text_sha256 IN ({placeholders}) "
                        f"AND embedding IS NOT NULL AND length(embedding) = ?",
                        (name, *batch, EXPECTED_DIM_BYTES),
                    ).fetchall()
                    for sha, emb in crows:
                        if sha not in result:
                            result[sha] = emb
            finally:
                cconn.close()
        except Exception as e:
            _note_cold_degraded("get_existing_embeddings", e)
    return result


def _cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    dot = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(dot / (norm_a * norm_b))


def _time_decay(file_mtime: float, half_life: float, now: float) -> float:
    if file_mtime <= 0:
        return 0.5
    days_ago = (now - file_mtime) / 86400.0
    if days_ago < 0:
        days_ago = 0
    return 0.5 ** (days_ago / half_life)


def _mmr_rerank(
    candidates: list[dict],
    query_embedding: list[float],
    lambda_: float = 0.7,
    top_n: int = 5,
) -> list[dict]:
    if not candidates or len(candidates) <= top_n:
        return candidates

    qemb = np.array(query_embedding, dtype=np.float32)
    qemb = qemb / (np.linalg.norm(qemb) + 1e-10)
    selected: list[dict] = []
    remaining = list(candidates)

    for _ in range(min(top_n, len(candidates))):
        best_mmr = -float("inf")
        best_idx = 0
        for i, cand in enumerate(remaining):
            relevance = 1.0 - cand["score"]
            if not selected:
                diversity = 0.0
            else:
                cemb = np.array(cand.get("_emb", []), dtype=np.float32)
                if cemb.size == 0:
                    diversity = 0.0
                else:
                    diversity = max(
                        _cosine_sim(cemb, np.array(s.get("_emb", []), dtype=np.float32))
                        for s in selected
                    )
            mmr = lambda_ * relevance - (1 - lambda_) * diversity
            if mmr > best_mmr:
                best_mmr = mmr
                best_idx = i
        selected.append(remaining.pop(best_idx))

    return selected


_GOLD_TOPLEVEL_FILES = frozenset({
    "AGENTS.md", "BOOTSTRAP.md", "DREAMS.md", "KNOWLEDGE.md", "LESSONS.md",
    "MEMORY.md", "METHODOLOGY.md", "SOUL.md", "STANDING-ORDERS.md",
    "TOOLS.md", "USER.md", "WORKFLOW.md", "tools.md",
})


def classify_layer(source: str) -> str:
    """根据 source 路径推断 layer ∈ {gold, task, session} (与 C1 回填规则同源).

    gold  = 精编层: memory/{details,inbox}/ + memory/ 顶层 .md + 顶层精编文件名
            + decisions/ (ADR 架构决策记录) + docs/plans/reviews/ (三方会审存档)
    task  = agent_team/ 任务文件
    session = 其余 (会话切块, 含 memory/.dreams/ memory/sessions/)

    indexer 写入侧与 db.search layered 模式共用此函数, 保证"写入打标=读取分流"
    单一真相源, 避免 C1 回填规则与写入侧漂移 (设计 §5 MEM-C1/C2).

    T-MEMIDX-INDEX-DECISIONS (2026-08-08): decisions/ 和 docs/plans/reviews/ 归 gold
    — 它们跟 k_*.md / STANDING-ORDERS 是同一类东西: 正式决策/审查记录.
    """
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


def _resolve_layer_for_vec(metadata_json: str, source: str) -> str:
    """upsert 双写时取 layer: metadata.layer 优先, 缺则 classify_layer(source).
    与 _search_vec / backfill 脚本同口径, 保证写入打标 = 读取分流."""
    if metadata_json:
        try:
            ml = json.loads(metadata_json).get("layer")
            if ml:
                return ml
        except Exception:
            pass
    return classify_layer(source)


def _match_where(meta: dict, source_col: str, where: dict) -> bool:
    """元数据过滤 (复活 where 死代码, 设计 §1 洞A / §5 MEM-C2).

    支持:
      {"source": {"$like": "%xxx%"}}   # SQL LIKE 包含匹配
      {"layer": "gold"}                 # 等值
      {"layer": {"$eq": "gold"}}        # 等值 (显式)
    """
    for key, cond in where.items():
        val = meta.get(key)
        if val is None and key == "source":
            val = source_col or ""
        if isinstance(cond, dict) and "$like" in cond:
            pat = cond["$like"]
            haystack = str(val if val is not None else "")
            if "%" in pat:
                subs = [seg for seg in pat.split("%") if seg]
                if any(seg not in haystack for seg in subs):
                    return False
            elif haystack != pat:
                return False
        elif isinstance(cond, dict):
            if cond.get("$eq", object()) != val:
                return False
        else:
            if val != cond:
                return False
    return True


def _layered_select(
    items: list[dict],
    n_results: int,
    gold_admission_distance: float,
) -> list[dict]:
    """两段式召回 (设计 §2 P0-1 + R1/R2/L3).

    1. 取金层(layer=gold)且 distance <= gold_admission_distance 的合格块, 按分排序优先
    2. 不足 n_results 用 session/task 补齐 (按分)
    3. R1 空转回退: 金层合格块=0 (全弱) → 退回纯距离排序, 防金层弱匹配挤掉会话强答案

    注: 在全库合并后调用 (单 collection 内金层会被 session 距离淹没, 设计 §0).
    """
    gold = [
        it for it in items
        if it.get("_layer") == "gold" and it["distance"] <= gold_admission_distance
    ]
    if not gold:
        for it in items:
            it.pop("_layer", None)
        items.sort(key=lambda x: x["score"])
        return items
    gold.sort(key=lambda x: x["score"])
    others = [it for it in items if it.get("_layer") != "gold"]
    others.sort(key=lambda x: x["score"])
    selected = gold[:n_results]
    if len(selected) < n_results:
        selected.extend(others[: n_results - len(selected)])
    for it in selected:
        it.pop("_layer", None)
    return selected


def layered_select(
    items: list[dict],
    n_results: int,
    gold_admission_distance: float = 0.50,
) -> list[dict]:
    """全库合并后的两段式选择 (public, server.py 调用).

    items 元数据里须有 metadata.layer (C1 回填/indexer 写入). 无 layer 视为 session.
    """
    for it in items:
        layer = it.get("_layer") or it.get("metadata", {}).get("layer") or "session"
        it["_layer"] = layer
    return _layered_select(items, n_results, gold_admission_distance)


def _join_and_score(
    collection_name: str,
    dist_map: dict[str, float],
    where: dict | None,
    min_distance: float | None,
    use_feedback: bool,
    row_map: dict | None = None,
) -> list[dict]:
    """候选池 → 打分 items (未排序). _search_vec 与 _cross_single_scan 共用 (单一真相源).

    row_map: 外部已批量预取的 chunks 行 {id: (id,text,metadata,source,embedding)};
    不传则本函数自行回连 (R1245: 不加冗余 collection 谓词, id 全局 PK).
    """
    all_ids = sorted(dist_map.keys(), key=lambda c: dist_map[c])
    if row_map is None:
        conn = _get_conn()
        placeholders = ",".join("?" * len(all_ids))
        rows = conn.execute(
            f"SELECT id, text, metadata, source, embedding FROM chunks "
            f"WHERE id IN ({placeholders})",
            all_ids,
        ).fetchall()
        conn.close()
        row_map = {r[0]: r for r in rows}
        # T-MEMDB-COLD-SPLIT: 分库后冷行真相在冷库 — 未命中的 id 补捞 (降级不静默)
        if _cold_split_active() and len(row_map) < len(all_ids):
            try:
                row_map.update(_cold_lookup_rows(
                    all_ids, row_map, "id, text, metadata, source, embedding"))
            except Exception as e:
                _note_cold_degraded("join_and_score", e)

    items: list[dict] = []
    now = time.time()
    half_life = SEARCH.time_decay_half_life
    for chunk_id in all_ids:
        dist = dist_map[chunk_id]
        if min_distance is not None and dist >= min_distance:
            continue
        row = row_map.get(chunk_id)
        if not row:
            continue
        meta = json.loads(row[2]) if row[2] else {}
        if where and not _match_where(meta, row[3] or "", where):
            continue
        file_mtime = float(meta.get("file_mtime", 0))
        decay = _time_decay(file_mtime, half_life, now)
        score = dist / max(decay, 0.01)
        if use_feedback:
            factor = get_feedback_factor(chunk_id)
            if factor != 1.0:
                score = score / factor
        emb_blob = row[4]
        # numpy view 保留 (MMR 用); MMR 的 _cosine_sim 内自带 norm, 不需预归一化.
        emb = np.frombuffer(emb_blob, dtype=np.float32).copy() if emb_blob else np.zeros(1024, dtype=np.float32)
        items.append({
            "id": chunk_id,
            "collection": collection_name,
            "text": row[1],
            "metadata": meta,
            "distance": round(dist, 6),
            "time_decay": round(decay, 4),
            "score": round(score, 6),
            "_emb": emb,
            "_layer": meta.get("layer") or classify_layer(row[3] or ""),
        })
    return items


def _finish_items(
    items: list[dict],
    query_embedding: list[float],
    n_results: int,
    use_mmr: bool,
    mode: str | None,
) -> list[dict]:
    """排序/MMR/top-n 后处理 (与 legacy search() 完全同构, 设计铁律: 逐行语义不变)."""
    if mode == "layered":
        for item in items:
            item.pop("_emb", None)
        return items

    items.sort(key=lambda x: x["score"])

    if use_mmr and len(items) > n_results:
        items = _mmr_rerank(items, query_embedding, SEARCH.mmr_lambda, n_results)
    else:
        items = items[:n_results]

    for rank, item in enumerate(items):
        item.pop("_emb", None)
        item.pop("_layer", None)
        if use_mmr:
            item["mmr_rank"] = rank

    return items


# T-FIX-T-ALERT-memory-service-degraded-2026-08-30-19-00-32: sqlite-vec 0.1.9
# KNN 查询 k 硬上限 (超限抛 "k value in knn query too large"), where 路径池值钳制用。
_VEC0_KNN_MAX_K = 4096


def _search_vec(
    collection_name: str,
    query_embedding: list[float],
    n_results: int = 5,
    where: dict | None = None,
    min_distance: float | None = None,
    use_mmr: bool = True,
    use_feedback: bool = True,
    mode: str | None = None,
) -> list[dict]:
    """vec0 KNN 搜索路径 (MS-C). 语义与 legacy search() 等价, 候选池来自 vec0 而非整矩阵物化.

    两段查询 (mode='layered'): 金层一段 (WHERE layer='gold') + 余层一段, 合并去重.
    距离 = vec0 cosine distance (= 1 - cos_sim, 与 legacy `1 - embs @ q` 同语义).
    候选 chunk_ids 回连 chunks 表取 text/metadata/embedding, 后续 score/MMR 与 legacy 同.
    """
    q = np.array(query_embedding, dtype=np.float32)
    q = q / (np.linalg.norm(q) + 1e-10)
    qblob = q.tobytes()

    candidate_multiplier = 5 if use_mmr else 3
    fetch_count = n_results * candidate_multiplier

    # W3: 活动表集扫描 (tiered 三表并集 = 单层全量语义; legacy = vec_chunks 原行为).
    # tier 互斥 (一 chunk 只在一表), dist_map 直接合并无冲突。
    scan_tables = _active_vec_tables()
    # T-MEMDB-COLD-SPLIT: cold 表分库路由 (冷库故障 → 该段空结果 + 降级记账,
    # 不倒主检索 — 与跨库路径同语义; gold 段同理, gold 越龄置热通常在 hot 表).
    _cold_route = _cold_split_active()

    def _seg_fetch(t: str, sql: str, params: tuple) -> list:
        # T-FIX-...-19-00-32: 每次调用独立连接 (段扫描并行化的前提; WAL 模式并发读
        # 安全, 线程模型复用 search_cross_collection 既有模式)。冷表路由语义原样。
        if t == _TIER_COLD_TABLE and _cold_route:
            try:
                cconn = _get_cold_conn()
                try:
                    return _knn_fetch(cconn, sql, params)
                finally:
                    cconn.close()
            except Exception as e:
                _note_cold_degraded(f"search_vec[{t}]", e)
                return []
        conn = _get_conn()
        try:
            return _knn_fetch(conn, sql, params)
        finally:
            conn.close()

    # T-FIX-...-19-00-32 (Phase A H1a/H1b): where 路径禁 COUNT 预查 + 池值钳制。
    # 原: pool_size = Σ COUNT(*) WHERE collection=? (vec0 辅助列无索引 = 每表全表扫
    # ~2-4s, 双表白付 ~6s) 且值=全集合行数, 超过 vec0 KNN k 硬上限 4096 →
    # OperationalError → search() 护栏 → 大集合 where 查询恒 degraded 空结果
    # (launchd_stderr.log 8/29 起多条 "k value too large, provided 26254~167565" 实锤)。
    # 语义: legacy 全池过滤 → vec0 下"最近 _VEC0_KNN_MAX_K 内过滤"; 对 source LIKE
    # 类元数据过滤 (n_results 典型 5-20) 召回损失可忽略, 且是 k 上限下最优可实现。
    if where:
        pool_size = _VEC0_KNN_MAX_K
    else:
        pool_size = fetch_count

    if pool_size == 0:
        return []

    # 段 1: 余层 (全集合) top-pool_size — 逐表扫, 距离合并
    # 段 2 (仅 layered): 金层 top-fetch_count (gold∈hot 为 rebuild 不变量,
    # 但读路径不依赖它 — 同样扫全表集, 免手工维护不变量)
    #
    # T-FIX-...-19-00-32 (Phase A H1c, 主因): 各段并发执行。原串行 2-4 次全表遍历
    # (vec0 无 partition key, collection=? 只省距离计算省不掉遍历), 冷页单次 5-8s,
    # 叠加即慢查日志的 db_search 9-35s; 并行后墙钟 = max(单段)。结果按 (kind, 表序)
    # 收集再合并, 与串行版逐字节等价 (tier 表互斥, dist_map 合并无冲突; 金段合并
    # 逻辑原样)。异常语义: hot/warm 异常经 future.result() 原样上抛 (护栏不变)。
    seg_by_table: dict[str, list[tuple]] = {}
    gold_by_table: dict[str, list[tuple]] = {}
    futures: dict = {}
    with ThreadPoolExecutor(
        max_workers=max(1, len(scan_tables) * (2 if mode == "layered" else 1))
    ) as ex:
        for t in scan_tables:
            futures[("main", t)] = ex.submit(
                _seg_fetch,
                t,
                f"SELECT chunk_id, distance FROM {t} "
                f"WHERE collection=? AND embedding MATCH ? "
                f"ORDER BY distance LIMIT ?",
                (collection_name, qblob, pool_size),
            )
        if mode == "layered":
            for t in scan_tables:
                futures[("gold", t)] = ex.submit(
                    _seg_fetch,
                    t,
                    f"SELECT chunk_id, distance FROM {t} "
                    f"WHERE collection=? AND layer='gold' AND embedding MATCH ? "
                    f"ORDER BY distance LIMIT ?",
                    (collection_name, qblob, fetch_count),
                )
        for key, fut in futures.items():
            rows = fut.result()
            (seg_by_table if key[0] == "main" else gold_by_table)[key[1]] = rows

    seg_rows: list[tuple] = []
    for t in scan_tables:
        seg_rows.extend(seg_by_table.get(t, []))
    gold_rows: list[tuple] = []
    if mode == "layered":
        for t in scan_tables:
            gold_rows.extend(gold_by_table.get(t, []))

    if not seg_rows and not gold_rows:
        return []

    # 合并候选: chunk_id → best(cosine) distance
    dist_map: dict[str, float] = {}
    for cid, dist in seg_rows:
        dist_map[cid] = dist
    for cid, dist in gold_rows:
        if cid not in dist_map or dist < dist_map[cid]:
            dist_map[cid] = dist

    if not dist_map:
        return []

    # 打分 + 后处理 (与 _cross_single_scan 共用同一实现, 单一真相源)
    items = _join_and_score(collection_name, dist_map, where, min_distance, use_feedback)
    return _finish_items(items, query_embedding, n_results, use_mmr, mode)


def _cross_single_scan(
    query_embedding: list[float],
    large_cols: list[str],
    n_results: int,
    use_mmr: bool,
    min_distance: float | None,
    use_feedback: bool,
    tables: list[str] | None = None,
) -> list[dict]:
    """单次全库 vec0 扫 + 逐集合同款打分/MMR (T-MEMSVC-P95, 2026-08-16).

    替代逐集合各查一次 vec0: vec0 无 partition key, 每次MATCH都全表遍历(~460ms地板),
    5 个大集合付 5 遍; 一次全库扫(~933ms@L25)就把所有集合的候选算完。

    与逐集合 _search_vec 的语义等价三支点:
    1) 每集合候选池 = 该集合距离 top-(n_results×multiplier) — 全局扫按距离序分组截取;
    2) 打分/MMR 走同一 _join_and_score/_finish_items (单一真相源, 不复制公式);
    3) 池饥饿(某集合在全局 top-R 内凑不满池) → 该集合回退逐集合扫, 语义永不因截断漂移.
       R 默认 1600: 20 条真实查询实测每集合第 25 候选的全局序号 max=907
       (data/research/p95_step2_probe.json, 2026-08-16), 留 1.76× 余量.

    W3: tables 参数化 — tiered 布局下传三表并集 (killswitch=off 的单层语义);
    tier 互斥 → 跨表 dist 同名冲突仅取更小值, 合并语义无损。
    """
    q = np.array(query_embedding, dtype=np.float32)
    q = q / (np.linalg.norm(q) + 1e-10)
    qblob = q.tobytes()

    candidate_multiplier = 5 if use_mmr else 3
    pool_per_coll = n_results * candidate_multiplier
    r_limit = max(_VEC_CROSS_LIMIT, pool_per_coll * 16)
    scan_tables = tables if tables is not None else _active_vec_tables()

    dist_map: dict[str, float] = {}
    coll_map: dict[str, str] = {}
    scanned_rows = 0
    conn = _get_conn()
    _cold_route = _cold_split_active()
    try:
        for t in scan_tables:
            # T-MEMDB-COLD-SPLIT: cold 表分库路由 (冷库故障=该段空+记账, 不倒主检索)
            if t == _TIER_COLD_TABLE and _cold_route:
                try:
                    cconn = _get_cold_conn()
                    try:
                        seg = _knn_fetch(
                            cconn,
                            f"SELECT chunk_id, distance, collection FROM {t} "
                            f"WHERE embedding MATCH ? ORDER BY distance LIMIT ?",
                            (qblob, r_limit),
                        )
                    finally:
                        cconn.close()
                except Exception as e:
                    _note_cold_degraded(f"cross_single_scan[{t}]", e)
                    seg = []
            else:
                seg = _knn_fetch(
                    conn,
                    f"SELECT chunk_id, distance, collection FROM {t} "
                    f"WHERE embedding MATCH ? ORDER BY distance LIMIT ?",
                    (qblob, r_limit),
                )
            scanned_rows += len(seg)
            for cid, dist, coll in seg:
                prev = dist_map.get(cid)
                if prev is None or dist < prev:
                    dist_map[cid] = dist
                    coll_map[cid] = coll
    finally:
        conn.close()

    # 按集合分组, 各取距离 top-pool_per_coll (距离升序入池)
    pools: dict[str, dict[str, float]] = {c: {} for c in large_cols}
    for cid, dist in sorted(dist_map.items(), key=lambda kv: kv[1]):
        p = pools.get(coll_map[cid])
        if p is not None and len(p) < pool_per_coll:
            p[cid] = dist
    starved = [c for c in large_cols if len(pools[c]) < pool_per_coll]

    # 批量 join-back: 一次 IN 查全部非饥饿集合的池 ids (R1245: 不加冗余谓词)
    all_ids = [cid for c in large_cols if c not in starved for cid in pools[c]]
    row_map: dict = {}
    if all_ids:
        conn = _get_conn()
        placeholders = ",".join("?" * len(all_ids))
        rows = conn.execute(
            f"SELECT id, text, metadata, source, embedding FROM chunks "
            f"WHERE id IN ({placeholders})",
            all_ids,
        ).fetchall()
        conn.close()
        row_map = {r[0]: r for r in rows}
        # T-MEMDB-COLD-SPLIT: 冷行补捞 (同 _join_and_score 口径)
        if _cold_split_active() and len(row_map) < len(all_ids):
            try:
                row_map.update(_cold_lookup_rows(
                    all_ids, row_map, "id, text, metadata, source, embedding"))
            except Exception as e:
                _note_cold_degraded("cross_single_scan_join", e)

    items: list[dict] = []
    for c in large_cols:
        if c in starved:
            logger.warning(
                f"[vec-cross] {c} 池饥饿({len(pools[c])}/{pool_per_coll}, R={r_limit}) → 回退逐集合扫"
            )
            items.extend(
                search(c, "", query_embedding, n_results=n_results,
                       min_distance=min_distance, use_mmr=use_mmr,
                       use_feedback=use_feedback)
            )
            continue
        col_items = _join_and_score(c, pools[c], None, min_distance, use_feedback, row_map=row_map)
        items.extend(_finish_items(col_items, query_embedding, n_results, use_mmr, None))
    return items


# ── MEMEXEC-W1 / T-MEMSVC-P95 步骤3 (2026-08-17): 进程内 1-bit 粗召回 (方案A, orchestrator-approved) ──
# 架构: chunks.embedding sign 量化 (float32[1024] → uint8[128]) 常驻内存 (~101MB@800k行)
#   + 查询时汉明距离 top-K 粗召回 (~40-130ms, 分块限 transient) → rowid 点查回连 float 精排
#   → 逐集合 top-25 池 → 复用 _join_and_score/_finish_items (打分/MMR 链零改动)。
# 只替换 _cross_single_scan 的"vec0 全库扫"那一步 (1.5s→~0.1s), 其余语义支点全继承:
#   池饥饿回退 / VecFallbackError 护栏放行 / killswitch (MEMSVC_BIT_RECALL=off 秒回退 float 单扫)。
#     rowid 平行数组 (int64, 6.3MB) 而非 chunk_id 字符串 (~70MB): RSS 预算 150MB 的硬约束;
#   INSERT OR REPLACE 换 rowid → 旧行 fetch miss 自然跳过 (仅浪费候选槽, 无语义污染),
#   新行由 watermark 增量刷新拾起 (默认 30s, 陈旧窗硬条件实测见 test_bit_recall.py)。
#   已知边角 (记录不修): "删除最大 rowid 行后插入" 会复用 rowid ≤ watermark → 刷新
#   盲区, 至容量重建才收录 (SQLite rowid 分配语义; 生产写入模式不触发该形态)。

# 出厂默认 K=16000 / band=960 (w1_band_sweep 20真实查询实测): 池 recall 96.4% /
# top-5 96% (双硬闸 ≥95%), db 层 p50 121ms, fetch p50 4800 行。K 上探到 16000 的
# 代价仅汉明段不变 (候选分组多一倍, 仍是同一数组上的 argpartition);
# internal reference cap 4000 实测池 recall 92.96%<95% → 按实测曲面取值, 偏离已在回执注明。
_BIT_RECALL_K = int(os.environ.get("MEMSVC_BIT_RECALL_LIMIT", "16000"))
# 两段 trim 带宽: 汉明 top-K 后按集合取 bit 序 top-band 才 fetch 精排 — Python 串行段
# 从 ~90ms(K 行全取) 砍到 ~25ms, 3 并发 GIL 劣化 3.15×→2.1× (e2e 由 embed 并行重叠
# 兜到 ~1.3×)。band 对池 recall 的代价实测: 60→64% ✗ / 960→96.4% ✓ / 2000→98.2%
# (加宽换召回, 减窄换并发; 960 为双闸达标的最省 fetch 点)。
_BIT_BAND = int(os.environ.get("MEMSVC_BIT_RECALL_BAND", "960"))
# T-MEMSVC-PARTITION-IMPL-0927 P5: 单集合 tiered 路径的集合内 band (只精排一个集合,
# 可比跨集合 960 宽; 取值依据见 docs/PARTITION-0927-NOTES.md 召回对比)。
_BIT_SINGLE_BAND = int(os.environ.get("MEMSVC_BIT_SINGLE_BAND", "2000"))
_BIT_REFRESH_SECS = float(os.environ.get("MEMSVC_BIT_RECALL_REFRESH", "30"))
# 等 bit 构建完成的上限 (v2: 3.0→6.0): 冷查判据下, 风暴窗内 float 回退(~2s+)远劣于
# 多等 1-2s 构建 (实测构建 2.4-5s, indexer 已让道 6s 启动)。
_BIT_WAIT_SECS = float(os.environ.get("MEMSVC_BIT_RECALL_WAIT", "6.0"))
_BIT_CAPACITY_HEADROOM = int(os.environ.get("MEMSVC_BIT_RECALL_HEADROOM", "50000"))
_BIT_EMBED_BYTES = 4096   # bge-m3 float32[1024] 裸长; 不符即坏行跳过
_BIT_PACK_BYTES = 128     # np.packbits(1024 bool) → 128 uint8
_BIT_BUILD_BATCH = 2048   # 全量构建分批 (限 transient ~8MB/批; RSS 高水位预算驱动)
# R1335 (W1.5 防御): 增量刷新读批上限 — 原 1<<30 单次无上限 fetch 是 90s stall 的
# 嫌疑点之一 (memexec_w1_2h_observation_20260817 §二); 分批后 blob 即批消化不跨批囤积。
_BIT_REFRESH_BATCH = int(os.environ.get("MEMSVC_BIT_RECALL_REFRESH_BATCH", "2048"))
# backlog item 1393 / T-MEMSVC-W15§E: 单轮 _refresh 积压告警阈 — 全史最大单窗 17,360 行(4/25 灌库),
# 脆弱窗实发 511 行≈2MB; 50k 全史未达, 触达即写入端异常信号(灌库/重放/回扫), 记日志供事后归因。
_BIT_BACKLOG_WARN_ROWS = int(os.environ.get("MEMSVC_BIT_RECALL_BACKLOG_WARN", "50000"))
_BIT_FETCH_BATCH = 900    # rowid IN 点查批大小 (与既有 dedup 批口径一致)
_BIT_HAM_BLOCK = 131072   # 汉明分块行数 (限 transient ~32MB)

_POP_LUT = np.array([bin(i).count("1") for i in range(256)], dtype=np.uint8)
_HAS_BITWISE_COUNT = hasattr(np, "bitwise_count")  # numpy≥2.0, 无 gather 更快


def _bit_recall_enabled() -> bool:
    return os.environ.get("MEMSVC_BIT_RECALL", "on").lower() not in ("off", "0", "false")


class _BitRecallIndex:
    """1-bit 粗召回索引单例 (常驻, 后台构建/刷新, 查询只读快照)。

    并发协议: 预分配定容数组永不 realloc; 增量刷新只在 [_n, _n+k) 区间原地写,
    写完才发布新 _n (单赋值, GIL 下顺序保证) → 读者锁内取 (bits, rowids, n) 引用快照,
    锁外计算, 永远只见完整行。容量耗尽 → 全量重建 (新数组, 锁内换引用, 旧快照读者
    持旧数组自然失效安全)。stale rowid (被删/被替换) 由查询侧 fetch miss 吸收。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._ready_ev = threading.Event()
        self._state = "idle"  # idle|loading|ready|failed
        self._bits: np.ndarray | None = None
        self._rowids: np.ndarray | None = None
        self._colls: np.ndarray | None = None   # uint8 集合编码 (v2: 两段 trim 免 phase-1 fetch)
        self._coll_code: dict[str, int] = {}    # collection 名 → 编码 (构建期分配)
        self._coll_names: list[str] = []        # 编码 → 名
        self._n = 0
        self._capacity = 0
        self._watermark = 0
        self._built_at = 0.0
        self._build_secs = 0.0
        self._refresh_count = 0
        self._refresh_rows_total = 0
        self._rebuild_count = 0
        # T-MEMSVC-PARTITION-IMPL-0927 P2: 降温钟 (hot→warm) 搬来的行 rowid 在
        # watermark 之下, 增量刷新 (rowid > wm) 永远看不到 → 由降温钟提交后显式
        # append_rows 推入; 构建中/容量不足时先挂 _pending, 构建完去重后补入。
        self._pending: list[tuple[int, str, np.ndarray]] = []
        self._appended_total = 0
        self._force_rebuild = False

    def _code_of(self, coll_name: str) -> int:
        # 只被构建/刷新线程调用 (单写者); 256 上限足够, 溢出归并到 255 (仅影响 band 精度)
        code = self._coll_code.get(coll_name)
        if code is None:
            if len(self._coll_names) >= 255:
                code = 255
            else:
                code = len(self._coll_names)
                self._coll_names.append(coll_name)
            self._coll_code[coll_name] = code
        return code

    def status(self) -> dict:
        with self._lock:
            return {
                "state": self._state,
                "rows": self._n,
                "watermark": self._watermark,
                "build_secs": round(self._build_secs, 2),
                "built_at": self._built_at,
                "refresh_count": self._refresh_count,
                "refresh_rows_total": self._refresh_rows_total,
                "rebuild_count": self._rebuild_count,
                "capacity": self._capacity,
                "enabled": _bit_recall_enabled(),
                "appended_total": self._appended_total,
                "pending": len(self._pending),
            }

    def snapshot(self):
        with self._lock:
            return self._bits, self._rowids, self._colls, self._n

    def coll_code_map(self) -> dict[str, int]:
        with self._lock:
            return dict(self._coll_code)

    def is_ready(self) -> bool:
        return self._state == "ready"

    # ---- 后台线程 (幂等启动) ----
    def start_background(self) -> None:
        with self._lock:
            if self._state != "idle":
                return
            self._state = "loading"
        t = threading.Thread(target=self._run, name="bit-recall", daemon=True)
        t.start()

    def _run(self) -> None:
        try:
            self._build()
        except Exception:
            # 构建失败 = bit 路径不可用, 查询走 float 单扫 (killswitch 同效), 不影响服务
            with self._lock:
                self._state = "failed"
                self._pending = []
            logger.exception("[bit-recall] 全量构建失败 → 停用 bit 路径 (查询走 float 单扫)")
            return
        with self._lock:
            if not self._drain_pending_locked():
                self._force_rebuild = True
            self._state = "ready"
        self._ready_ev.set()
        logger.info(
            f"[bit-recall] ready: {self._n} 行 × {_BIT_PACK_BYTES}B "
            f"(构建 {self._build_secs:.1f}s, watermark={self._watermark})"
        )
        while True:
            time.sleep(_BIT_REFRESH_SECS)
            try:
                self._refresh()
            except Exception:
                logger.exception("[bit-recall] 增量刷新失败 (保留旧索引, 下轮重试)")

    def _read_batch(self, conn, after_rowid, limit):
        # W3: tiered 布局只收 warm 行 (设计 §一: 温层以 bit 形态参与默认扫描;
        # 热层走 vec0 float 精确扫, 不占 bit 预算; 冷层物理移出不进 bit)。
        # 非 warm 行被 JOIN 滤掉但 watermark 照常推进 (wm=最后命中行 rowid),
        # 后续非 warm 行由下一轮重走 — 2048 批内代价可忽略。
        if _tiered_layout_safe(conn):
            return conn.execute(
                f"SELECT c.rowid, c.collection, c.embedding FROM chunks c "
                f"JOIN {_TIER_REGISTRY} t ON t.chunk_id = c.id AND t.tier='warm' "
                f"WHERE c.rowid > ? ORDER BY c.rowid LIMIT ?",
                (after_rowid, limit),
            ).fetchall()
        return conn.execute(
            "SELECT rowid, collection, embedding FROM chunks "
            "WHERE rowid > ? ORDER BY rowid LIMIT ?",
            (after_rowid, limit),
        ).fetchall()

    def _build(self) -> None:
        t0 = time.perf_counter()
        conn = _get_conn()
        try:
            if _tiered_layout_safe(conn):
                # W3: 容量按 warm 行数预算 (485k 量级), 不再按全库多留 50% 死区
                total = conn.execute(
                    f"SELECT COUNT(*) FROM {_TIER_REGISTRY} WHERE tier='warm'"
                ).fetchone()[0]
            else:
                total = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
            capacity = total + max(_BIT_CAPACITY_HEADROOM, total // 20)
            bits = np.zeros((capacity, _BIT_PACK_BYTES), dtype=np.uint8)
            rowids = np.zeros(capacity, dtype=np.int64)
            colls = np.zeros(capacity, dtype=np.uint8)
            with self._lock:
                self._coll_code, self._coll_names = {}, []
            n, wm = 0, 0
            while True:
                rows = self._read_batch(conn, wm, _BIT_BUILD_BATCH)
                if not rows:
                    break
                for rid, coll, blob in rows:
                    if blob is None or len(blob) != _BIT_EMBED_BYTES:
                        continue  # 坏行跳过 (历史空 embedding 防御, 与 upsert reject 同口径)
                    emb = np.frombuffer(blob, dtype=np.float32)
                    bits[n] = np.packbits(emb > 0)
                    rowids[n] = rid
                    colls[n] = self._code_of(coll)
                    n += 1
                wm = rows[-1][0]
            with self._lock:
                self._bits, self._rowids, self._colls = bits, rowids, colls
                self._n, self._capacity, self._watermark = n, capacity, wm
                self._built_at = time.time()
                self._build_secs = time.perf_counter() - t0
        finally:
            conn.close()

    def _refresh(self) -> None:
        """增量: watermark 之后新行原地 append; 容量耗尽 → 全量重建 (罕见, 记数)。

        R1335 (W1.5 防御, memexec_w1_2h_observation §二):
        ① 单次 fetch 从无上限 (1<<30) 改分批 (_BIT_REFRESH_BATCH 批上限);
        ② packbits 在锁外预计算, 锁内只做数组写+元数据更新 (原锁内逐行 packbits
           会拖住 snapshot()/status() 读者 — 90s stall 嫌疑点之二)。
        语义保持: 坏行跳过 / watermark=最后读到的 rowid / 容量判据含坏行的全部读数。
        """
        with self._lock:
            wm = self._watermark
            # P2 (降温钟 append): 先补挂起行; 容量不足 → 本轮直接走全量重建
            force = self._force_rebuild or not self._drain_pending_locked()
        if force:
            self._full_rebuild_inline("降温钟 append 容量不足/强制")
            return
        conn = _get_conn()
        packed = []   # (rowid, collection_name, 128B packed bits) — 锁外预计算产物
        total_read = 0
        try:
            while True:
                batch = self._read_batch(conn, wm, _BIT_REFRESH_BATCH)
                if not batch:
                    break
                total_read += len(batch)
                for rid, coll, blob in batch:
                    if blob is None or len(blob) != _BIT_EMBED_BYTES:
                        continue  # 坏行跳过 (与 _build 同口径)
                    emb = np.frombuffer(blob, dtype=np.float32)
                    packed.append((rid, coll, np.packbits(emb > 0)))
                wm = batch[-1][0]
                if len(batch) < _BIT_REFRESH_BATCH:
                    break
        finally:
            conn.close()
        if total_read == 0:
            return
        if total_read > _BIT_BACKLOG_WARN_ROWS:
            logger.warning(
                f"[bit-recall] 积压超阈: 单轮读 {total_read} 行 (阈值 {_BIT_BACKLOG_WARN_ROWS}, "
                f"累计读入 ~{total_read * _BIT_EMBED_BYTES >> 20}MB, 批大小 {_BIT_REFRESH_BATCH} 分批消化) "
                f"— 写入端突发(灌库/重放/回扫)?"
            )
        with self._lock:
            capacity, n = self._capacity, self._n
        if n + total_read > capacity:
            self._full_rebuild_inline(f"容量耗尽 ({n}+{total_read}>{capacity})")
            return
        k = 0
        with self._lock:
            for rid, coll, bits128 in packed:
                self._bits[self._n + k] = bits128
                self._rowids[self._n + k] = rid
                self._colls[self._n + k] = self._code_of(coll)
                k += 1
            self._n += k
            self._watermark = wm
            self._refresh_count += 1
            self._refresh_rows_total += k

    def _full_rebuild_inline(self, why: str) -> None:
        """刷新线程内全量重建 (容量耗尽 / 挂起行放不下); 重建后挂起行去重补入。"""
        self._rebuild_count += 1
        logger.info(f"[bit-recall] {why} → 全量重建")
        with self._lock:
            self._state = "loading"
            self._ready_ev.clear()
            self._force_rebuild = False
        try:
            self._build()
        finally:
            with self._lock:
                if not self._drain_pending_locked():
                    self._force_rebuild = True   # 仍放不下 (构建期间又涌入) → 下轮再建
                self._state = "ready"
            self._ready_ev.set()

    def _drain_pending_locked(self) -> bool:
        """(调用方持 self._lock) 挂起行按 rowid 去重 (构建可能已读到) 后写入定容数组。

        返回 False = 容量不足 (挂起行保留, 由调用方触发全量重建 — 行已提交, 重建必含)。
        """
        if not self._pending:
            return True
        if self._bits is None:
            return False
        n = self._n
        prids = np.fromiter((p[0] for p in self._pending), dtype=np.int64,
                            count=len(self._pending))
        keep = ~np.isin(prids, self._rowids[:n])
        fresh = [p for p, k in zip(self._pending, keep) if k]
        if n + len(fresh) > self._capacity:
            return False
        for j, (rid, coll, bits128) in enumerate(fresh):
            self._bits[n + j] = bits128
            self._rowids[n + j] = rid
            self._colls[n + j] = self._code_of(coll)
        self._n = n + len(fresh)
        self._appended_total += len(fresh)
        self._pending = []
        return True

    def append_rows(self, rows: list[tuple[int, str, bytes]]) -> int:
        """P2 降温钟调用: 刚降入温层的行 (rowid, collection, embedding) 推入 bit 索引。

        就绪 → 立即去重写入 (容量不足则挂起+标强制重建); 构建中 → 挂起, 构建完补;
        idle/failed (本进程未启用 bit, 如脚本/索引器进程) → 忽略: 下次全量构建自然收录。
        返回挂入/写入的行数。
        """
        packed = []
        for rid, coll, blob in rows:
            if blob is None or len(blob) != _BIT_EMBED_BYTES:
                continue  # 坏行跳过 (与 _build 同口径)
            packed.append((int(rid), coll,
                           np.packbits(np.frombuffer(blob, dtype=np.float32) > 0)))
        if not packed:
            return 0
        with self._lock:
            if self._state not in ("loading", "ready"):
                return 0
            self._pending.extend(packed)
            if self._state == "ready" and not self._drain_pending_locked():
                self._force_rebuild = True
        return len(packed)


_bit_index = _BitRecallIndex()


def bit_recall_warmup_start() -> None:
    """server 启动期显式预热入口 (脚本 import db 不触发); 幂等。"""
    if _bit_recall_enabled():
        _bit_index.start_background()


def _bit_wait_ready(wait_secs: float) -> bool:
    """查询侧就绪门: ready 即真; loading 则等 (冷查判据靠它); idle 懒启动; failed 假。"""
    if _bit_index.is_ready():
        return True
    if not _bit_recall_enabled():
        return False
    _bit_index.start_background()
    return _bit_index._ready_ev.wait(timeout=wait_secs)


def _bit_hamming_topk(bits, n, qbits, k, colls=None, code=None):
    """分块汉明 + 两级 top-k (每块 top-k → 合并再 top-k), transient 限 ~32MB。

    返回 (行号数组, 对应汉明值数组) — v2: 两段 trim 需要候选的汉明值做集合内 band 排序。
    T-MEMSVC-PARTITION-IMPL-0927 P5: colls+code 给定时只在该集合行内取 top-k (其余行
    汉明值置 0xFFFF 垫底; 调用方按集合编码再滤, 垫底行不会混入)。
    """
    k = min(k, n)
    if k <= 0:
        return np.array([], dtype=np.int64), np.array([], dtype=np.uint16)
    cand_h, cand_i = [], []
    for s in range(0, n, _BIT_HAM_BLOCK):
        e = min(s + _BIT_HAM_BLOCK, n)
        blk = bits[s:e] ^ qbits
        if _HAS_BITWISE_COUNT:
            h = np.bitwise_count(blk).sum(axis=1, dtype=np.uint16)
        else:
            h = _POP_LUT[blk].sum(axis=1, dtype=np.uint16)
        if code is not None:
            h = np.where(colls[s:e] == code, h, np.uint16(0xFFFF))
        m = e - s
        part = np.argpartition(h, min(k, m) - 1)[: min(k, m)] if k < m else np.arange(m)
        cand_h.append(h[part])
        cand_i.append(part.astype(np.int64) + s)
    H = np.concatenate(cand_h)
    I = np.concatenate(cand_i)
    if len(H) > k:
        take = np.argpartition(H, k - 1)[:k]
        return I[take], H[take]
    return I, H


def _bit_pool_core(
    query_embedding: list[float],
    large_cols: list[str],
    pool_per_coll: int,
    restrict_single: bool = False,
) -> tuple[dict[str, dict[str, float]], dict[str, tuple], list[str], dict]:
    """bit 粗召回核心 (MEMEXEC-W1 方案A v2, W3 抽出共用): 汉明 top-K → 两段 trim
    band → rowid 点查 float 精排 → 逐集合候选池 + phase-2 全列 row_map。

    与 float 侧 "vec0 距离序逐集合 top-25" 同构 (band 代价实测见 w1_band_sweep);
    池饥饿集合由返回值交给调用方组接回退 (tiered 与 legacy 回退目标不同:
    tiered → hot+warm 表精确补扫; legacy → 逐集合全量 search)。
    精排距离 = 1 - cos (显式归一化, 与 vec0 cosine distance 同义)。
    """
    t0 = time.perf_counter()
    q = np.array(query_embedding, dtype=np.float32)
    q = q / (np.linalg.norm(q) + 1e-10)
    qbits = np.packbits(q > 0)

    bits, rowids, colls, n = _bit_index.snapshot()
    if n == 0 or bits is None:
        raise RuntimeError("bit index empty snapshot")
    code_map = _bit_index.coll_code_map()
    if restrict_single and len(large_cols) == 1:
        # P5 单集合: 汉明只在该集合行内排 (全局 top-K 再按集合滤会让小份额集合被
        # 别的集合挤出候选) → 直接取集合内 top-band, band 可比跨集合宽 (只精排一个集合)
        band = max(_BIT_SINGLE_BAND, pool_per_coll)
        K = band
        code = code_map.get(large_cols[0])
        if code is None:   # 该集合无温层行 → 空候选 (调用方按饥饿补扫)
            sel, sel_ham = np.array([], dtype=np.int64), np.array([], dtype=np.uint16)
        else:
            sel, sel_ham = _bit_hamming_topk(bits, n, qbits, K, colls=colls, code=code)
    else:
        K = max(_BIT_RECALL_K, pool_per_coll * len(large_cols))
        band = max(_BIT_BAND, pool_per_coll)
        sel, sel_ham = _bit_hamming_topk(bits, n, qbits, K)
    t_ham = time.perf_counter() - t0

    # 两段 trim: 候选按集合编码分组 (常驻 uint8 数组, 免 phase-1 fetch),
    # 每集合取 bit (汉明) 序 top-band
    sel_codes = colls[sel]
    band_pos: list[np.ndarray] = []   # sel 内位置 (各集合 band)
    band_coll: list[int] = []         # 对应集合编码
    starved: list[str] = []
    for c in large_cols:
        code = code_map.get(c)
        mask = sel_codes == code if code is not None else np.zeros(len(sel), dtype=bool)
        cnt = int(mask.sum())
        if cnt < pool_per_coll:
            starved.append(c)
            continue
        if cnt > band:
            h = sel_ham[mask]
            keep = np.argpartition(h, band - 1)[:band]
            pos = np.flatnonzero(mask)[keep]
        else:
            pos = np.flatnonzero(mask)
        band_pos.append(pos)
        band_coll.extend([code] * len(pos))
    code_to_coll = {v: k for k, v in code_map.items()}
    t_band = time.perf_counter() - t0

    # 两段 trim v2.1 (并发优化): phase-1 只取 (rowid, embedding) 轻两列 — 7列元组
    # (含 text/metadata 大 bytes) 的 Python 物化是 GIL 串行主成本。
    # 对齐协议: band rowid 先整体排序再分批 IN — 各批内 rowid 升序且批间接续 →
    # fetched_rids 全局升序, searchsorted 才合法 (v2.1 首版跨批乱序 → embedding 错配,
    # 池 recall 崩到 19%, 已修); 距离经逆置换散回 band 序。
    band_sel = np.concatenate(band_pos) if band_pos else np.array([], dtype=np.int64)
    band_rowids = rowids[sel[band_sel]]
    perm = np.argsort(band_rowids, kind="stable")          # sorted序 → band序 (散回用)
    rid_sorted = band_rowids[perm]
    blobs: list[bytes] = []
    fetched_rids: list[int] = []
    conn = _get_conn()
    try:
        rid_py = rid_sorted.tolist()
        for i in range(0, len(rid_py), _BIT_FETCH_BATCH):
            batch = rid_py[i:i + _BIT_FETCH_BATCH]
            rows = conn.execute(
                f"SELECT rowid, embedding FROM chunks "
                f"WHERE rowid IN ({','.join('?' * len(batch))})",
                batch,
            ).fetchall()
            for r in rows:
                if r[1] is not None and len(r[1]) == _BIT_EMBED_BYTES:
                    fetched_rids.append(r[0])
                    blobs.append(r[1])
    finally:
        conn.close()
    t_fetch = time.perf_counter() - t0

    # float 精排 (v2.2 RSS 优化: 分块 E@q/norms — 免 b"".join 全量拼接与 En 物化,
    # 每查 transient 峰值 ~60MB→~8MB; macOS 小对象 malloc 不还 OS, 峰值即 RSS 高水位)
    dists = np.full(len(band_rowids), np.inf, dtype=np.float32)
    if fetched_rids:
        # 防御性排序: 不假设 IN 查询返回序 (plan 相关), 排序后 searchsorted 才成立
        fr = np.asarray(fetched_rids, dtype=np.int64)
        srt = np.argsort(fr, kind="stable")
        fr_sorted = fr[srt]
        pos = np.searchsorted(fr_sorted, rid_sorted)
        ok = (pos < len(fr)) & (fr_sorted[np.minimum(pos, len(fr) - 1)] == rid_sorted)
        ok_idx = np.flatnonzero(ok)
        d_sorted = np.empty(len(fr), dtype=np.float32)
        B = 2048
        for s in range(0, len(blobs), B):
            sel_b = srt[s:s + B].tolist()
            if len(sel_b) == 1:
                Eb = np.frombuffer(blobs[sel_b[0]], dtype=np.float32)
            else:
                Eb = np.frombuffer(
                    b"".join(map(blobs.__getitem__, sel_b)), dtype=np.float32)
            Eb = Eb.reshape(-1, 1024)
            nb = np.linalg.norm(Eb, axis=1)
            d_sorted[s:s + len(sel_b)] = 1.0 - (Eb @ q) / np.maximum(nb, 1e-10)
            del Eb
        dists[perm[ok_idx]] = d_sorted[pos[ok_idx]]
    t_rerank = time.perf_counter() - t0

    # 池: band 序按精确距离取逐集合 top-pool_per_coll (全池满即早退出, ~125 次迭代)
    pools: dict[str, dict[str, float]] = {c: {} for c in large_cols}
    band_rowid_list = band_rowids.tolist()
    order = np.argsort(dists, kind="stable")
    need = {c: pool_per_coll for c in large_cols}
    remaining = sum(need.values())
    for idx in order:
        if not np.isfinite(dists[idx]):
            break
        br = int(idx)
        c = code_to_coll[band_coll[br]]
        if need.get(c):
            pools[c][br] = float(dists[idx])
            need[c] -= 1
            remaining -= 1
            if remaining <= 0:
                break
    t_pool = time.perf_counter() - t0

    # phase-2: 池 ids (≤集合数×pool) 回连全 5 列 (text/metadata/source/embedding) 供打分/MMR
    pool_rows_rids = sorted({band_rowid_list[br] for c in large_cols for br in pools[c]})
    row_map: dict[str, tuple] = {}
    rid_to_id: dict[int, str] = {}
    if pool_rows_rids:
        conn = _get_conn()
        try:
            ph = ",".join("?" * len(pool_rows_rids))
            rows2 = conn.execute(
                f"SELECT rowid, id, text, metadata, source, embedding FROM chunks "
                f"WHERE rowid IN ({ph})", pool_rows_rids,
            ).fetchall()
        finally:
            conn.close()
        for r in rows2:
            if r[5] is not None and len(r[5]) == _BIT_EMBED_BYTES:
                rid_to_id[r[0]] = r[1]
                row_map[r[1]] = (r[1], r[2], r[3], r[4], r[5])
    # 池键 rowid → chunk id (stale rowid 在 phase-1 已滤, 此处必命中)
    pools_by_id: dict[str, dict[str, float]] = {}
    for c in large_cols:
        pools_by_id[c] = {rid_to_id[band_rowid_list[br]]: d
                          for br, d in pools[c].items()
                          if band_rowid_list[br] in rid_to_id}

    meta = {
        "timings": {
            "ham_s": t_ham, "band_s": t_band, "fetch_s": t_fetch,
            "rerank_s": t_rerank, "pool_s": t_pool,
            "total_s": time.perf_counter() - t0,
            "n": n, "K": K, "band": band, "fetched": len(fetched_rids),
            "starved": list(starved),
        }
    }
    return pools_by_id, row_map, starved, meta


def _bit_cross_scan(
    query_embedding: list[float],
    large_cols: list[str],
    n_results: int,
    use_mmr: bool,
    min_distance: float | None,
    use_feedback: bool,
    debug_pools: dict | None = None,
) -> list[dict]:
    """bit 粗召回 + 两段 trim + float 精排 + 逐集合同款打分/MMR (MEMEXEC-W1 方案A v2)。

    与 _cross_single_scan 的语义对应: 全局候选源 vec0 全库扫 → 汉明 top-K (K 默认
    16000, 曲线实测); 池构成 = 逐集合 bit 序 top-band 内按精确 float cosine
    取 top-(n_results×multiplier) — 与 float 侧 "vec0 距离序逐集合 top-25" 同构,
    band 代价实测见 w1_band_sweep (池 recall 持平全量精排); 饥饿回退/护栏语义原样。

    W3: 池生成段抽到 _bit_pool_core (tiered 温层段共用); 本函数 = legacy 布局的
    W1 完整路径 (bit 覆盖全库), 行为/签名/debug 契约不变。
    """
    candidate_multiplier = 5 if use_mmr else 3
    pool_per_coll = n_results * candidate_multiplier
    pools_by_id, row_map, starved, core_meta = _bit_pool_core(
        query_embedding, large_cols, pool_per_coll)

    if debug_pools is not None:
        # 验证钩子 (测试/probe 专用): 暴露逐集合池 + 分段计时, 生产调用不传。
        debug_pools["pools"] = {c: dict(pools_by_id[c]) for c in large_cols}
        debug_pools["timings"] = core_meta["timings"]

    items: list[dict] = []
    for c in large_cols:
        if c in starved:
            logger.warning(
                f"[bit-recall] {c} 池饥饿(0/{pool_per_coll}, K={core_meta['timings']['K']}) → 回退逐集合扫"
            )
            items.extend(
                search(c, "", query_embedding, n_results=n_results,
                       min_distance=min_distance, use_mmr=use_mmr,
                       use_feedback=use_feedback)
            )
            continue
        col_items = _join_and_score(c, pools_by_id[c], None, min_distance, use_feedback, row_map=row_map)
        items.extend(_finish_items(col_items, query_embedding, n_results, use_mmr, None))

    if logger.isEnabledFor(logging.DEBUG):
        t = core_meta["timings"]
        logger.debug(
            f"[bit-recall] n={t['n']} K={t['K']} band={t['band']} ham={t['ham_s']*1000:.0f}ms "
            f"fetch={t['fetch_s']*1000:.0f}ms total={t['total_s']*1000:.0f}ms"
        )
    return items


# ── MEMEXEC-W3: tiered 默认路径 (热 float 精确 + 温 bit + 弱结果冷扫 + 翻热阀) ──

_tier_counts_cache: tuple[float, dict[str, int]] | None = None
# T-MEMSVC-IO-FIX-0930 #2: 行数 TTL 60→600 + 温层行数免 COUNT。依据: 本函数产物
# 只喂 [tier-scan] 日志行与 scan_bytes 上界记账 (非计费口径, 函数自身 docstring
# 既定"允许分钟级陈旧"); hot/warm 行数日漂移 ~0.1% (T-MEMSVC-SLOW-0930 实测
# warm 1,635,761→1,637,188/+0.09% 每日), 10min 陈旧远在记账容差内。而旧 60s TTL
# 每 60s 对 168 万行 warm 表 COUNT(*) — I/O 停滞期实测卡 14.7s (同调查 dump ②),
# 是尾部慢直接机制之一。温层改取 bit 索引 status rows: W3 构造不变量 bit 索引
# = warm 行集 (降温钟 append_rows 同步推入), O(1) 锁读; bit 不可用/非 tiered
# 布局才回退 COUNT (值可能差 bit 增量刷新窗内的少量行, 记账容差内)。
try:
    _TIER_COUNTS_TTL = float(os.environ.get("MEMSVC_TIER_COUNTS_TTL_S", "600"))
except ValueError:
    _TIER_COUNTS_TTL = 600.0


def _tier_table_counts() -> dict[str, int]:
    """三张 tier 表行数 (600s TTL — 扫描字节记账用, 非计费口径, 允许分钟级陈旧)."""
    global _tier_counts_cache
    now = time.time()
    if _tier_counts_cache is not None and now - _tier_counts_cache[0] < _TIER_COUNTS_TTL:
        return _tier_counts_cache[1]
    counts = {}
    try:
        conn = _get_conn()
        _t0 = time.perf_counter()
        counts[_TIER_HOT_TABLE] = conn.execute(
            f"SELECT COUNT(*) FROM {_TIER_HOT_TABLE}").fetchone()[0]
        if _tiered_layout() and _bit_index.is_ready():
            counts[_TIER_WARM_TABLE] = int(_bit_index.status()["rows"])
        else:
            counts[_TIER_WARM_TABLE] = conn.execute(
                f"SELECT COUNT(*) FROM {_TIER_WARM_TABLE}").fetchone()[0]
        _seg_add("cnt_ms", time.perf_counter() - _t0)
        conn.close()
        # T-MEMDB-COLD-SPLIT: cold 表分库 active 后行数在冷库 (扫描字节记账口径不变)
        if _cold_split_active():
            cconn = _get_cold_conn()
            _t1 = time.perf_counter()
            counts[_TIER_COLD_TABLE] = cconn.execute(
                f"SELECT COUNT(*) FROM {_TIER_COLD_TABLE}").fetchone()[0]
            _seg_add("cnt_ms", time.perf_counter() - _t1)
            cconn.close()
        else:
            conn = _get_conn()
            _t2 = time.perf_counter()
            counts[_TIER_COLD_TABLE] = conn.execute(
                f"SELECT COUNT(*) FROM {_TIER_COLD_TABLE}").fetchone()[0]
            _seg_add("cnt_ms", time.perf_counter() - _t2)
            conn.close()
    except Exception as e:
        logger.warning(f"[tier] 表行数统计失败 (记账降级为 0): {e}")
    _tier_counts_cache = (now, counts)
    return counts


def _vec_table_pools(
    table: str,
    colls: list[str],
    pool_per_coll: int,
    qblob: bytes,
    r_limit: int,
) -> dict[str, dict[str, float]]:
    """单张 vec0 表全局扫 → 逐集合 top-pool 候选池 (距离精确, 热层/冷层共用).

    T-MEMDB-COLD-SPLIT: cold 表在分库 active 时扫冷库 (主库该表为回滚锚/过渡残留).
    """
    conn = _get_cold_conn() if (table == _TIER_COLD_TABLE and _cold_split_active()) else _get_conn()
    try:
        seg = _knn_fetch(
            conn,
            f"SELECT chunk_id, distance, collection FROM {table} "
            f"WHERE embedding MATCH ? ORDER BY distance LIMIT ?",
            (qblob, r_limit),
        )
    finally:
        conn.close()
    pools: dict[str, dict[str, float]] = {c: {} for c in colls}
    for cid, dist, coll in seg:  # seg 按距离升序
        p = pools.get(coll)
        if p is not None and len(p) < pool_per_coll:
            p[cid] = dist
    return pools


# ── T-MEMSVC-PARTITION-IMPL-0927 P3: 逐集合 partition 扫 (替代全局扫+截断) ──────
# ADR-029 后 hot/warm 已带 collection partition key, 但 tiered 跨集合路径仍是"无
# collection 约束的全局 MATCH LIMIT r_limit"(不剪枝, 且全局截断会让小集合池饥饿 →
# 再逐集合补扫)。改为逐集合 `collection=? AND MATCH` 分区扫: 每集合拿到精确 top-pool
# (语义 ⊇ 旧全局截断), 线程并行, 叶子段过 P4 KNN 闸。表未带分区键 (如冷库未重建 /
# 回滚后) → 回退旧全局扫, 绝不在非分区表上逐集合全表扫 N 遍。
# killswitch: MEMSVC_PARTITION_SCAN=off → 全部回旧全局扫。
_PARTITION_SCAN_WORKERS = int(_env_num_r1427("MEMSVC_PARTITION_SCAN_WORKERS", 4.0))
_partition_form_cache: dict[tuple[str, str], tuple[float, bool]] = {}


def _partition_scan_enabled() -> bool:
    return os.environ.get("MEMSVC_PARTITION_SCAN", "on").lower() not in ("off", "0", "false")


def _table_partitioned(table: str) -> bool:
    """tier 表 DDL 是否带 collection partition key (60s TTL; 冷表按分库路由读冷库 DDL).

    探测失败按"非分区"处理 (走旧全局扫 = 今日行为, 保守方向)。
    """
    cold = table == _TIER_COLD_TABLE and _cold_split_active()
    key = ("cold" if cold else "main", table)
    now = time.time()
    hit = _partition_form_cache.get(key)
    if hit is not None and now - hit[0] < 60:
        return hit[1]
    val = False
    try:
        conn = _get_cold_conn() if cold else _get_conn()
        try:
            r = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
        finally:
            conn.close()
        val = bool(r and r[0] and "partition key" in r[0].lower())
    except Exception as e:  # 具名 catch: 吞的是 DDL 探测自身失败, 保守按非分区
        logger.warning(f"[partition] {table} 分区形态探测失败 (按非分区走全局扫): {e}")
    _partition_form_cache[key] = (now, val)
    return val


def _vec_partition_pools(
    table: str,
    colls: list[str],
    pool_per_coll: int,
    qblob: bytes,
) -> dict[str, dict[str, float]]:
    """逐集合分区 KNN → {collection: {chunk_id: distance}} (每集合精确 top-pool)."""
    if not colls:
        return {}
    cold = table == _TIER_COLD_TABLE and _cold_split_active()
    sql = (f"SELECT chunk_id, distance FROM {table} "
           f"WHERE collection=? AND embedding MATCH ? ORDER BY distance LIMIT ?")

    def _one(c: str) -> tuple[str, dict[str, float]]:
        conn = _get_cold_conn() if cold else _get_conn()
        try:
            rows = _knn_fetch(conn, sql, (c, qblob, pool_per_coll))
        finally:
            conn.close()
        return c, {cid: d for cid, d in rows}

    if len(colls) == 1:
        return dict([_one(colls[0])])
    with ThreadPoolExecutor(max_workers=max(1, min(len(colls), _PARTITION_SCAN_WORKERS))) as ex:
        return dict(ex.map(_one, colls))


def _tier_pools(
    table: str,
    colls: list[str],
    pool_per_coll: int,
    qblob: bytes,
    r_limit: int,
) -> dict[str, dict[str, float]]:
    """P3 统一入口: 分区表 → 逐集合分区扫; 非分区/killswitch → 旧全局扫 (_vec_table_pools)."""
    if _partition_scan_enabled() and _table_partitioned(table):
        return _vec_partition_pools(table, colls, pool_per_coll, qblob)
    return _vec_table_pools(table, colls, pool_per_coll, qblob, r_limit)


def _tier_fill_pool_hw(
    collection: str,
    pool_per_coll: int,
    qblob: bytes,
) -> dict[str, float]:
    """饥饿集合补扫: 该集合 hot+warm 表精确池 (tier 语义内回退, 不放冷 —
    与 legacy 的逐集合全量回退不同, 冷层只经弱结果阀到达, 设计 §二)."""
    merged: dict[str, float] = {}
    for t in (_TIER_HOT_TABLE, _TIER_WARM_TABLE):
        conn = _get_conn()
        try:
            rows = _knn_fetch(
                conn,
                f"SELECT chunk_id, distance FROM {t} "
                f"WHERE collection=? AND embedding MATCH ? ORDER BY distance LIMIT ?",
                (collection, qblob, pool_per_coll),
            )
        finally:
            conn.close()
        for cid, dist in rows:
            if cid not in merged or dist < merged[cid]:
                merged[cid] = dist
    return dict(sorted(merged.items(), key=lambda kv: kv[1])[:pool_per_coll])


def _tier_result_weak(items: list[dict]) -> tuple[bool, float, float]:
    """弱结果判定 (设计 §二 Q2: top-1 > 0.38 噪音线 或 top-5 均值 > 0.42; 空结果=弱)."""
    dists = sorted(it.get("distance", 1.0) for it in items)
    if not dists:
        return True, float("inf"), float("inf")
    top1 = dists[0]
    top5m = sum(dists[:5]) / min(5, len(dists))
    return (top1 > _TIER_WEAK_TOP1 or top5m > _TIER_WEAK_TOP5_MEAN), top1, top5m


def _tier_promotion_ledger_append(records: list[dict]) -> None:
    """翻热台账 (fail-silent: IO 异常只记日志, 不影响搜索响应)."""
    try:
        _TIER_PROMOTION_LEDGER.parent.mkdir(parents=True, exist_ok=True)
        with open(_TIER_PROMOTION_LEDGER, "a", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    except OSError as e:  # 磁盘满/权限 — 台账失败不阻断检索
        logger.warning(f"[tier] 翻热台账写入失败 ({len(records)} 条): {e}")


def _promote_cold_to_hot(chunk_ids: list[str], query_prefix: str) -> list[str]:
    """翻热单向阀: cold 表 → hot 表 + chunk_tier (reason='promoted') + 台账.

    幂等 (INSERT OR REPLACE / DELETE); 与 indexer upsert 同把 _write_lock 串行。
    翻热后该 chunk 立即进默认路径 (hot 表), TTL 由后续降温钟 (W4) 管理。
    T-MEMDB-COLD-SPLIT: 分库 active 时冷行真相在冷库 — 翻热 = 三件套搬家
    (vec 行冷库→主库 hot 表, chunks 行冷库→主库, 注册行冷库删/主库 hot)。
    冷库故障 → 跳过本次翻热 + 降级记账 (翻热是 best-effort, 不放倒检索)。
    """
    if not chunk_ids:
        return []
    moved: list[str] = []
    colls: set[str] = set()
    now = time.time()
    cold_active = _cold_split_active()
    with _write_lock:
        conn = _get_conn()
        cconn = _get_cold_conn() if cold_active else None
        try:
            for i in range(0, len(chunk_ids), 900):
                batch = chunk_ids[i:i + 900]
                ph = ",".join("?" * len(batch))
                if cold_active:
                    rows = cconn.execute(
                        f"SELECT chunk_id, embedding, collection, layer FROM {_TIER_COLD_TABLE} "
                        f"WHERE chunk_id IN ({ph})", batch,
                    ).fetchall()
                    if not rows:
                        continue
                    # chunks 真相行搬家 (冷库 → 主库): 翻热后行回主库归属
                    crows = cconn.execute(
                        f"SELECT id, collection, text, embedding, metadata, source, "
                        f"updated_at, text_sha256 FROM chunks WHERE id IN ({ph})",
                        batch,
                    ).fetchall()
                    conn.executemany(
                        "INSERT OR REPLACE INTO chunks (id, collection, text, embedding, "
                        "metadata, source, updated_at, text_sha256) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        crows,
                    )
                    conn.executemany(
                        f"INSERT OR REPLACE INTO {_TIER_HOT_TABLE}"
                        "(chunk_id, embedding, collection, layer) VALUES (?,?,?,?)",
                        rows,
                    )
                    # 主库残留的 stale cold 行同删 (该 id 现属 hot; 不清=跨层 dup 假警)
                    conn.executemany(
                        f"DELETE FROM {_TIER_COLD_TABLE} WHERE chunk_id=?",
                        [(r[0],) for r in rows],
                    )
                    conn.executemany(
                        f"INSERT OR REPLACE INTO {_TIER_REGISTRY}"
                        "(chunk_id, tier, reason, assigned_at) VALUES (?,'hot','promoted',?)",
                        [(r[0], now) for r in rows],
                    )
                    cconn.executemany(
                        f"DELETE FROM {_TIER_COLD_TABLE} WHERE chunk_id=?",
                        [(r[0],) for r in rows],
                    )
                    cconn.executemany(
                        "DELETE FROM chunks WHERE id=?",
                        [(r[0],) for r in rows],
                    )
                    cconn.executemany(
                        f"DELETE FROM {_TIER_REGISTRY} WHERE chunk_id=?",
                        [(r[0],) for r in rows],
                    )
                    moved.extend(r[0] for r in rows)
                    colls.update(r[2] for r in rows)
                else:
                    rows = conn.execute(
                        f"SELECT chunk_id, embedding, collection, layer FROM {_TIER_COLD_TABLE} "
                        f"WHERE chunk_id IN ({ph})", batch,
                    ).fetchall()
                    if not rows:
                        continue
                    conn.executemany(
                        f"INSERT OR REPLACE INTO {_TIER_HOT_TABLE}"
                        "(chunk_id, embedding, collection, layer) VALUES (?,?,?,?)",
                        rows,
                    )
                    conn.executemany(
                        f"DELETE FROM {_TIER_COLD_TABLE} WHERE chunk_id=?",
                        [(r[0],) for r in rows],
                    )
                    conn.executemany(
                        f"INSERT OR REPLACE INTO {_TIER_REGISTRY}"
                        "(chunk_id, tier, reason, assigned_at) VALUES (?,'hot','promoted',?)",
                        [(r[0], now) for r in rows],
                    )
                    moved.extend(r[0] for r in rows)
                    colls.update(r[2] for r in rows)
            if moved:
                conn.commit()
                if cconn is not None:
                    cconn.commit()
        except Exception as e:
            if cold_active:
                _note_cold_degraded("promote_cold_to_hot", e)
                return []   # 未 commit = 主库侧自动回滚; 翻热 best-effort 放弃本轮
            raise
        finally:
            conn.close()
            if cconn is not None:
                cconn.close()
    if moved:
        for c in colls:
            _invalidate_cache(c)
        _invalidate_cold_counts()
        _tier_promotion_ledger_append([
            {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "chunk_id": cid,
             "query": query_prefix[:120]}
            for cid in moved
        ])
        logger.info(f"[tier] 翻热 {len(moved)} 条 cold→hot (top-{_TIER_PROMOTE_TOPN} 命中)")
    return moved


# ── T-MEMSVC-PARTITION-IMPL-0927 P3: 翻热移出请求线程 ────────────────────────────
# 原: 检索请求线程内持 _write_lock 跨库搬家 (0925 同类扫描 §8: 响应被写锁/写 IO 拖住)。
# 改: 单线程后台执行器串行搬家; _promote_cold_to_hot 本身幂等 + best-effort 语义不变
# (冷行已不在 → 跳过)。响应元数据 promoted = 入队条数。同一 chunk 在途去重。
# killswitch: MEMSVC_TIER_PROMOTE_ASYNC=off → 请求线程内同步搬 (旧行为)。
_promote_executor: ThreadPoolExecutor | None = None
_promote_lock = threading.Lock()
_promote_inflight: set[str] = set()
_promote_pending = 0


def _promote_async_enabled() -> bool:
    return os.environ.get("MEMSVC_TIER_PROMOTE_ASYNC", "on").lower() not in ("off", "0", "false")


def _promote_submit(chunk_ids: list[str], query_prefix: str) -> int:
    """翻热入口 (检索路径调用): 异步入队, 返回入队条数; killswitch=off 时同步返回搬运数。"""
    global _promote_executor, _promote_pending
    if not chunk_ids:
        return 0
    if not _promote_async_enabled():
        return len(_promote_cold_to_hot(chunk_ids, query_prefix))
    with _promote_lock:
        ids = [c for c in dict.fromkeys(chunk_ids) if c not in _promote_inflight]
        if not ids:
            return 0
        _promote_inflight.update(ids)
        _promote_pending += 1
        if _promote_executor is None:
            _promote_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tier-promote")
        ex = _promote_executor

    def _run() -> None:
        global _promote_pending
        try:
            _promote_cold_to_hot(ids, query_prefix)
        except Exception:  # 命名: 吞的是后台搬家失败 — best-effort, 下次命中再翻 (原同步路径 raise 进请求)
            logger.exception("[tier] 后台翻热失败 (best-effort, 下次命中再翻)")
        finally:
            with _promote_lock:
                _promote_inflight.difference_update(ids)
                _promote_pending -= 1

    ex.submit(_run)
    return len(ids)


def promote_wait_idle(timeout: float = 30.0) -> bool:
    """等后台翻热队列清空 (测试/运维探针用); 超时返回 False。"""
    deadline = time.time() + timeout
    while True:
        with _promote_lock:
            if _promote_pending == 0:
                return True
        if time.time() >= deadline:
            return False
        time.sleep(0.02)


def _tiered_cross_search(
    query_embedding: list[float],
    large_cols: list[str],
    small_cols: list[str],
    n_results: int,
    use_mmr: bool,
    min_distance: float | None,
    use_feedback: bool,
    tier_meta: dict | None = None,
    single: bool = False,
) -> list[dict]:
    """W3 tiered 默认路径编排: 热 float 精确 + 温 bit + 小集合 hw 矩阵
    → 弱结果自动加扫冷层并并池 → 翻热阀 → [tier-scan] 扫描字节日志。

    T-MEMSVC-PARTITION-IMPL-0927: P3 热段/温降级段/冷段改 _tier_pools (分区表逐集合
    分区扫, 非分区表回退旧全局扫); 翻热改 _promote_submit 异步入队。single=True =
    P5 单集合 tiered 路径 (large_cols=[c], small_cols=[]; bit 汉明限定在该集合行内)。

    语义支点 (与 W1 单层路径对齐):
    - 逐集合池 = 该集合 hot+warm(±cold) 距离 top-(n_results×multiplier), 打分/MMR
      走同一 _join_and_score/_finish_items;
    - bit 失败 → 温层降级 warm 表精确扫 (当次, 不吃护栏语义);
    - vec0 (hot 扫) 失败 → VecFallbackError 护栏 (与 _cross_single_scan 同款);
    - 冷层只经弱结果阀到达; 冷层命中进最终 top-N → 翻热。
    """
    t0 = time.perf_counter()
    q = np.array(query_embedding, dtype=np.float32)
    q = q / (np.linalg.norm(q) + 1e-10)
    qblob = q.tobytes()

    candidate_multiplier = 5 if use_mmr else 3
    pool_per_coll = n_results * candidate_multiplier
    r_limit = max(_VEC_CROSS_LIMIT, pool_per_coll * 16)

    meta = tier_meta if tier_meta is not None else {}

    # ── 段1: 热层 float 精确扫 (P3: 逐集合分区扫; 表非分区 → 旧全局扫) ──
    _t_hot = time.perf_counter()
    hot_pools = _tier_pools(_TIER_HOT_TABLE, large_cols, pool_per_coll, qblob, r_limit)
    _seg_add("vec_ms", time.perf_counter() - _t_hot)

    # ── 段2: 温层 bit 粗召回 (bit 索引 = warm 行) → 失败降级 warm 表精确扫 ──
    warm_pools: dict[str, dict[str, float]] = {c: {} for c in large_cols}
    bit_failed = False
    if _bit_recall_enabled() and _bit_wait_ready(_BIT_WAIT_SECS):
        try:
            wp, row_map_bit, _bit_starved, _bm = _bit_pool_core(
                query_embedding, large_cols, pool_per_coll, restrict_single=single)
            warm_pools = wp
            # T-MEMSVC-IO-FIX-0930 #4: bit 段拆 fetch (phase-1 rowid 随机回捞,
            # 20.9GB 跨页读主源) 与计算 (汉明/band/精排/池)
            _bt = _bm["timings"]
            _fetch_seg = max(0.0, _bt["fetch_s"] - _bt["band_s"])
            _seg_add("fetch_ms", _fetch_seg)
            _seg_add("bit_ms", max(0.0, _bt["total_s"] - _fetch_seg))
        except VecFallbackError:
            raise
        except Exception:
            bit_failed = True
            logger.exception("[tier] 温层 bit 段失败 → 当次降级 warm 表精确扫")
    else:
        bit_failed = True  # bit 关/未就绪 → 精确扫兜底 (语义等价)
    if bit_failed:
        _t_warm = time.perf_counter()
        warm_pools = _tier_pools(_TIER_WARM_TABLE, large_cols, pool_per_coll, qblob, r_limit)
        _seg_add("vec_ms", time.perf_counter() - _t_warm)

    # ── 合并 hot+warm 池; 饥饿集合 tier 语义内补扫 ──
    # 池语义 = 单层路径逐集合 top-pool_per_coll (距离序): 合并后必须截回同尺寸 —
    # 否则候选 25→50, MMR 的多样性权衡在 2× 弹药下会把近重复的好结果挤出去
    # (实测 G11: 3 个基线目标 d≈0.20 只剩 1 个, 被 d≈0.33 的多样项顶掉)。
    pools: dict[str, dict[str, float]] = {}
    starved: list[str] = []
    _t_merge = time.perf_counter()
    for c in large_cols:
        m: dict[str, float] = dict(hot_pools.get(c) or {})
        for cid, d in (warm_pools.get(c) or {}).items():
            if cid not in m or d < m[cid]:
                m[cid] = d
        if len(m) > pool_per_coll:
            m = dict(sorted(m.items(), key=lambda kv: kv[1])[:pool_per_coll])
        if len(m) < pool_per_coll:
            filled = _tier_fill_pool_hw(c, pool_per_coll, qblob)
            if len(filled) > len(m):
                m = filled
            if len(m) < pool_per_coll:
                starved.append(c)  # hot+warm 真不足 (多为全冷集合) — 弱结果阀兜底
        pools[c] = m

    def _finish_all() -> list[dict]:
        items: list[dict] = []
        for c in large_cols:
            col_items = _join_and_score(c, pools[c], None, min_distance, use_feedback)
            items.extend(_finish_items(col_items, query_embedding, n_results, use_mmr, None))
        return items

    items = _finish_all()
    _seg_add("merge_ms", time.perf_counter() - _t_merge)

    # ── 段3: 小集合 legacy 矩阵路径 (tier 过滤: 只装 hot+warm 行) ──
    small_items: list[dict] = []
    _t_small = time.perf_counter()
    for col_name in small_cols:
        try:
            col_items = search(col_name, "", query_embedding,
                               n_results=n_results, min_distance=min_distance,
                               use_mmr=use_mmr, use_feedback=use_feedback,
                               tier_filter_hw=True)
            small_items.extend(col_items)
        except VecFallbackError:
            raise
        except Exception:
            logger.exception(f"[tier] 小集合 {col_name} 搜索失败")
    _seg_add("merge_ms", time.perf_counter() - _t_small)

    # ── 弱结果自动加扫冷层 (设计 §二 Q2) ──
    weak, top1, top5m = _tier_result_weak(items + small_items)
    explored_cold = False
    cold_ids_seen: set[str] = set()
    small_cold_items: list[dict] = []
    if weak:
        explored_cold = True
        try:
            _t_cold = time.perf_counter()
            cold_pools = _tier_pools(
                _TIER_COLD_TABLE, large_cols + small_cols, pool_per_coll, qblob, r_limit)
            _seg_add("vec_ms", time.perf_counter() - _t_cold)
        except Exception as e:
            # T-MEMDB-COLD-SPLIT: 冷库不可读 → 主结果照常, 显式降级 (禁静默少返回)
            _note_cold_degraded("tiered_cold_pools", e)
            meta["cold_db_degraded"] = True
            cold_pools = {c: {} for c in large_cols + small_cols}
        touched = False
        for c, cp in cold_pools.items():
            if not cp:
                continue
            touched = True
            cold_ids_seen.update(cp.keys())
            tgt = pools.get(c)
            if tgt is None:           # 小集合: 冷池单独 finish (矩阵路径只出 hot+warm)
                col_items = _join_and_score(c, cp, None, min_distance, use_feedback)
                small_cold_items.extend(
                    _finish_items(col_items, query_embedding, n_results, use_mmr, None))
            else:                     # 大集合: 并池后同样截回 pool_per_coll (池尺寸不变量)
                for cid, d in cp.items():
                    if cid not in tgt or d < tgt[cid]:
                        tgt[cid] = d
                if len(tgt) > pool_per_coll:
                    pools[c] = dict(sorted(tgt.items(), key=lambda kv: kv[1])[:pool_per_coll])
        if touched:
            _t_coldm = time.perf_counter()
            items = _finish_all()
            _seg_add("merge_ms", time.perf_counter() - _t_coldm)
    items = items + small_items + small_cold_items

    # ── 翻热单向阀: 冷层命中进最终 top-N (P3: 后台异步搬, 计数=入队条数) ──
    n_promoted = 0
    if cold_ids_seen:
        final_top = sorted(items, key=lambda it: it.get("distance", 1.0))[:_TIER_PROMOTE_TOPN]
        to_promote = [it["id"] for it in final_top if it["id"] in cold_ids_seen]
        if to_promote:
            n_promoted = _promote_submit(to_promote, str(meta.get("query", "")))

    # ── 扫描字节记账 + 日志 (机检"默认路径 ≤0.7GB 日志实证"锚) ──
    counts = _tier_table_counts()
    hot_rows = counts.get(_TIER_HOT_TABLE, 0)
    warm_rows = counts.get(_TIER_WARM_TABLE, 0)
    cold_rows = counts.get(_TIER_COLD_TABLE, 0)
    bit_n = _bit_index.status()["rows"] if not bit_failed else 0
    small_hw_rows = 0
    for c in small_cols:
        entry = _embeddings_cache.get(f"{c}::hw")
        if entry is not None and entry[0] is not None and entry[0].size:
            small_hw_rows += entry[0].shape[0]
    scan_bytes = (
        hot_rows * 4096                      # 热层 float 精确扫 (表全遍历)
        + (bit_n * 128 if not bit_failed else warm_rows * 4096)  # 温层 bit (或精确扫降级)
        + (cold_rows * 4096 if explored_cold else 0)             # 弱结果冷扫
        + small_hw_rows * 4096               # 小集合 hw 矩阵乘
    )
    meta.update({
        "tier": "tiered", "explored_cold": explored_cold,
        "cold_hits": len(cold_ids_seen), "promoted": n_promoted,
        "scan_bytes": scan_bytes, "hot_rows": hot_rows, "warm_rows": warm_rows,
        "cold_rows": cold_rows, "warm_via": "bit" if not bit_failed else "vec",
        "took_ms": round((time.perf_counter() - t0) * 1000, 1),
    })
    # scan_bytes 口径 = 表全量上界 (分区扫时实扫 = 所涉集合的分区, ≤ 上界; 单集合路径
    # 只扫该集合分区, 上界无意义 → 日志改标 single=<集合>, 不报字节)
    _mode = f"single={large_cols[0]} " if single and large_cols else ""
    logger.info(
        f"[tier-scan] {_mode}hot={'part' if single else hot_rows} "
        f"warm={'bit:' + str(bit_n) if not bit_failed else 'vec:' + str(warm_rows)} "
        f"cold={'on' if explored_cold else 'off'} small_hw={small_hw_rows} "
        + ("" if single else f"total={scan_bytes / 1e9:.3f}GB ")
        + f"weak=({top1:.3f},{top5m:.3f}) promoted={n_promoted} ms={meta['took_ms']}"
    )
    return items


def search_cross_collection(
    query_embedding: list[float],
    n_results: int = 5,
    where: dict | None = None,
    min_distance: float | None = None,
    use_mmr: bool = True,
    use_feedback: bool = True,
    mode: str | None = None,
    tier_meta: dict | None = None,
) -> list[dict]:
    """全库搜索入口 (T-MEMSVC-IO-FIX-0930 #4 外壳: 分段计时收集进 tier_meta).

    检索结束后 ``tier_meta["seg_ms"]`` = {bit fetch / COUNT / EXISTS / 向量 /
    合并 / total} 各段 ms (server 并入 [slow-query] 归因; 无 tier_meta 时只计时
    不外露)。嵌套 search() 调用共享同线程上下文, 不重复起算。
    """
    _owned = _seg_begin()
    _t0 = time.perf_counter()
    try:
        return _search_cross_impl(
            query_embedding, n_results=n_results, where=where,
            min_distance=min_distance, use_mmr=use_mmr,
            use_feedback=use_feedback, mode=mode, tier_meta=tier_meta,
        )
    finally:
        if _owned:  # 嵌套层墙钟已含在外层 total 内, 不重复累加
            _seg_add("total_ms", time.perf_counter() - _t0)
        seg = _seg_collect(_owned)
        if seg is not None and tier_meta is not None:
            tier_meta["seg_ms"] = seg


def _search_cross_impl(
    query_embedding: list[float],
    n_results: int = 5,
    where: dict | None = None,
    min_distance: float | None = None,
    use_mmr: bool = True,
    use_feedback: bool = True,
    mode: str | None = None,
    tier_meta: dict | None = None,
) -> list[dict]:
    """全库搜索 (server.py 无 project 时调用).

    MS-C hybrid: 大集合走 vec0, 小集合走 legacy numpy (2.5ms each).
    T-MEMSVC-P95 (2026-08-16): 默认模式大集合改单次全库 vec0 扫 (_cross_single_scan,
    ~1.5s vs 旧逐集合并行扫 ~2.3-2.9s), 语义等价 (同一打分/MMR helpers + 池饥饿回退);
    layered/where/MEMSVC_VEC_CROSS=off 走原逐集合并行路径.
    MEMEXEC-W3: tiered 布局 + 默认模式 → _tiered_cross_search (热 float + 温 bit +
    弱结果冷扫 + 翻热阀); tier_meta 出参供 server 增量响应元数据 (契约零改).
    """
    from concurrent.futures import ThreadPoolExecutor

    all_cols = list_collections()
    if not all_cols:
        return []

    # 分大/小集合
    large_cols: list[str] = []
    small_cols: list[str] = []
    for c in all_cols:
        cnt = collection_count(c)
        if cnt == 0:
            continue
        if cnt >= _VEC_HYBRID_THRESHOLD:
            large_cols.append(c)
        else:
            small_cols.append(c)

    items: list[dict] = []

    # W3: tiered 默认路径 (热 float 精确 + 温 bit + 弱结果冷扫 + 翻热)。
    # 前置: tiering 开 + tiered 布局 + 默认模式 (无 where/mode) + vec 可用;
    # 任一不满足 → 下方既有路径 (legacy 布局 = W1/W2 行为原样)。
    if (
        _tiering_enabled()
        and _tiered_layout()
        and large_cols
        and where is None
        and mode is None
        and _check_vec_usable()
    ):
        return _tiered_cross_search(
            query_embedding, large_cols, small_cols, n_results, use_mmr,
            min_distance, use_feedback, tier_meta,
        )

    # T-MEMSVC-P95 (2026-08-16): 默认模式(无 where)下单次全库扫替代逐集合 vec0 扫.
    # vec0 无 partition key, collection=? 只省距离计算省不掉 ~460ms 全表遍历地板,
    # 5 大集合 = 5 遍冗余遍历 (根因实测见 data/research/memsvc_p95_root_20260816.md).
    # layered/where 仍走原逐集合路径 (layered 需逐集合金段查询; where 需全集合池;
    # 真实流量占比 0.3% 见 hits ledger). MEMSVC_VEC_CROSS=off 可整体回退旧路径.
    use_single_scan = (
        _vec_cross_enabled()
        and large_cols
        and where is None
        and mode is None
        and _check_vec_usable()
    )

    # 大集合: 并行 vec0 查询 (各线程独立连接, WAL 模式支持并发读)
    def _search_one(col_name):
        try:
            return search(
                col_name, "", query_embedding,
                n_results=n_results, where=where, min_distance=min_distance,
                use_mmr=use_mmr, use_feedback=use_feedback, mode=mode,
            )
        except VecFallbackError:
            # CC终审REJECT-A必修: 护栏异常必须放行到 server 层 (线程池 worker 不得吞),
            # 否则全库搜 degraded 恒 False, 检索失败被读成"没有相关记忆"
            raise
        except Exception as e:
            logger.exception(f"vec search failed for {col_name}")
            return []

    if use_single_scan:
        # MEMEXEC-W1 (2026-08-17): bit 粗召回优先 (方案A)。失败仅当次回退 float 单扫,
        # 不吃掉 VecFallbackError 护栏语义; killswitch MEMSVC_BIT_RECALL=off → 跳过本块。
        # W3: tiered 布局下 bit 只覆盖 warm 行 — 到达本分支即 killswitch (tiering off)
        # 场景, 必须走三表并集精确扫 (单层语义), 跳过 bit。
        bit_done = False
        if _bit_recall_enabled() and not _tiered_layout() and _bit_wait_ready(_BIT_WAIT_SECS):
            try:
                _t_bit = time.perf_counter()
                items.extend(_bit_cross_scan(
                    query_embedding, large_cols, n_results, use_mmr,
                    min_distance, use_feedback,
                ))
                _seg_add("bit_ms", time.perf_counter() - _t_bit)  # bit 路径 fetch 计入 bit 段
                bit_done = True
            except VecFallbackError:
                raise
            except Exception:
                logger.exception("[bit-recall] bit 路径失败 → 当次回退 float 单扫")
        if not bit_done:
            try:
                _t_vec = time.perf_counter()
                items.extend(_cross_single_scan(
                    query_embedding, large_cols, n_results, use_mmr,
                    min_distance, use_feedback,
                ))
                _seg_add("vec_ms", time.perf_counter() - _t_vec)
            except VecFallbackError:
                # 池饥饿回退的逐集合扫触发护栏 → 原样放行 (语义同旧路径)
                raise
            except Exception:
                # 全库扫故障 = 大集合 vec0 故障, 同款护栏语义 (禁物化降级, per-request 回执)
                _count_vec_fallback()
                logger.exception("[vec-cross] 单次全库扫失败 → VecFallbackError 护栏")
                raise VecFallbackError(large_cols[0])
    elif large_cols:
        max_workers = min(len(large_cols), 4)
        _t_vec = time.perf_counter()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for col_items in ex.map(_search_one, large_cols):
                items.extend(col_items)
        _seg_add("vec_ms", time.perf_counter() - _t_vec)

    # 小集合: 串行 legacy (快, 2.5ms each)
    _t_small = time.perf_counter()
    for col_name in small_cols:
        try:
            col_items = search(
                col_name, "", query_embedding,
                n_results=n_results, where=where, min_distance=min_distance,
                use_mmr=use_mmr, use_feedback=use_feedback, mode=mode,
            )
            items.extend(col_items)
        except VecFallbackError:
            # CC终审残留3 (2026-08-15): 集合数增长会改变大小归类, 阈值一调即同款盲区 — 护栏异常必须放行
            raise
        except Exception:
            logger.exception(f"Search failed for collection {col_name}")
    _seg_add("merge_ms", time.perf_counter() - _t_small)

    return items


def _search_vec_cross(
    query_embedding: list[float],
    n_results: int = 5,
    where: dict | None = None,
    min_distance: float | None = None,
    use_mmr: bool = True,
    use_feedback: bool = True,
    mode: str | None = None,
) -> list[dict]:
    """[已废弃 2026-08-16, 零调用点死代码] 单次全库扫的早期尝试.

    与现路径语义不一致 (无逐集合 top-25 池/无 MMR/无 top-n 截断, global_limit=375
    不保证每集合池完整): top-5 重合仅 0-4/5. 正确实现见 _cross_single_scan
    (T-MEMSVC-P95). 保留供 diff 追溯, 勿接回.
    """
    q = np.array(query_embedding, dtype=np.float32)
    q = q / (np.linalg.norm(q) + 1e-10)
    qblob = q.tobytes()

    candidate_multiplier = 5 if use_mmr else 3
    fetch_per_coll = n_results * candidate_multiplier
    try:
        n_colls = max(len(list_collections()), 1)
    except Exception:
        n_colls = 10
    # 全局池: 每集合至少留 fetch_per_coll 候选; where 过滤放大 (回退: 极大池)
    global_limit = fetch_per_coll * n_colls if not where else 5000

    conn = _get_conn()
    try:
        # 段 1: 全库 top-global_limit
        seg = conn.execute(
            "SELECT chunk_id, distance, collection FROM vec_chunks "
            "WHERE embedding MATCH ? ORDER BY distance LIMIT ?",
            (qblob, global_limit),
        ).fetchall()
        # 段 2 (layered): 全库 gold
        gold: list[tuple] = []
        if mode == "layered":
            gold = conn.execute(
                "SELECT chunk_id, distance, collection FROM vec_chunks "
                "WHERE layer='gold' AND embedding MATCH ? ORDER BY distance LIMIT ?",
                (qblob, fetch_per_coll),
            ).fetchall()
    finally:
        conn.close()

    dist_map: dict[str, float] = {}
    coll_map: dict[str, str] = {}
    for cid, dist, coll in seg:
        dist_map[cid] = dist
        coll_map[cid] = coll
    for cid, dist, coll in gold:
        if cid not in dist_map or dist < dist_map[cid]:
            dist_map[cid] = dist
            coll_map[cid] = coll

    if not dist_map:
        return []
    all_ids = sorted(dist_map.keys(), key=lambda c: dist_map[c])

    conn = _get_conn()
    try:
        placeholders = ",".join("?" * len(all_ids))
        rows = conn.execute(
            f"SELECT id, text, metadata, source, embedding, collection FROM chunks "
            f"WHERE id IN ({placeholders})",
            all_ids,
        ).fetchall()
    finally:
        conn.close()

    row_map = {r[0]: r for r in rows}
    items: list[dict] = []
    now = time.time()
    half_life = SEARCH.time_decay_half_life
    for chunk_id in all_ids:
        dist = dist_map[chunk_id]
        if min_distance is not None and dist >= min_distance:
            continue
        row = row_map.get(chunk_id)
        if not row:
            continue
        coll = coll_map.get(chunk_id, row[5])
        meta = json.loads(row[2]) if row[2] else {}
        if where and not _match_where(meta, row[3] or "", where):
            continue
        file_mtime = float(meta.get("file_mtime", 0))
        decay = _time_decay(file_mtime, half_life, now)
        score = dist / max(decay, 0.01)
        if use_feedback:
            factor = get_feedback_factor(chunk_id)
            if factor != 1.0:
                score = score / factor
        emb_blob = row[4]
        emb = np.frombuffer(emb_blob, dtype=np.float32).copy() if emb_blob else np.zeros(1024, dtype=np.float32)
        items.append({
            "id": chunk_id,
            "collection": coll,
            "text": row[1],
            "metadata": meta,
            "distance": round(dist, 6),
            "time_decay": round(decay, 4),
            "score": round(score, 6),
            "_emb": emb,
            "_layer": meta.get("layer") or classify_layer(row[3] or ""),
        })

    if mode == "layered":
        for item in items:
            item.pop("_emb", None)
        return items

    items.sort(key=lambda x: x["score"])
    for item in items:
        item.pop("_emb", None)
    return items


# MS-C hybrid 阈值: 大集合 (>此值) 走 vec0 (零堆), 小集合走 legacy numpy (快, 内存小).
# 实测: 4 大集合各 ~100K vecs 矩阵 ~430MB × 3 拷贝 = ~1.25GB each → 内存泄漏源.
# 6 小集合共 ~40K vecs = ~160MB, legacy 缓存安全. vec0 对 100K vecs ~280ms, numpy 2.5ms.
# hybrid: 全库搜 ~1.1s(4 vec) + ~15ms(6 numpy) ≈ 1.1s; 总矩阵缓存 ~160MB.
_VEC_HYBRID_THRESHOLD = int(os.environ.get("MEMSVC_VEC_THRESHOLD", "25000"))
_vec_fallback_count = 0

# T-MEMSVC-P95 (2026-08-16): 大集合跨库搜索单次全库扫开关 + 全局候选上限 R.
# off=回退逐集合旧路径 (秒级回滚, 无需改代码); R 依据见 _cross_single_scan docstring.
_VEC_CROSS_LIMIT = int(os.environ.get("MEMSVC_VEC_CROSS_LIMIT", "1600"))


def _vec_cross_enabled() -> bool:
    return os.environ.get("MEMSVC_VEC_CROSS", "on").lower() not in ("off", "0", "false")


# ── MEMEXEC-W3 (2026-08-18): 三层分表 (hot/warm/cold) + 路由 ──────────────────
# 设计: docs/W3-TIERING-NOTES.md + memsvc_tiering_design_20260816.md (补3 口径)。
# 物理层: vec_chunks_hot/warm/cold 三张 vec0 派生表 + chunk_tier 注册表 (chunk_id 键,
#   防 INSERT OR REPLACE 的 rowid 漂移) + tier_meta 运行参数。真相层 chunks 与旧
#   vec_chunks 零改动 (后者 = 回滚锚, checkout 旧代码 + restart 即回)。
# 默认路径 (无 where/mode): 热=hot 表 float 精确扫; 温=warm 行 bit 粗召回+精排;
#   冷=物理移出, 弱结果 (top-1>0.38 ∨ top-5均值>0.42 ∨ 空) 自动加扫 cold 表并并池;
#   冷层命中进最终 top-5 → 翻热单向阀 (cold 表→hot 表 + chunk_tier + 台账)。
# killswitch: MEMSVC_TIERING=off → 三表并集精确扫 (单层语义), 路由/弱扫/翻热全关。
# 边界: H_DAYS 由 tier_rebuild 按数据 derive (float 预算 ≤0.6GB 是真约束, 天数是
#   派生 UI — 设计书"边界不变量"原文), 写入 tier_meta.hot_days_effective 供 upsert 同源。
_TIER_HOT_TABLE = "vec_chunks_hot"
_TIER_WARM_TABLE = "vec_chunks_warm"
_TIER_COLD_TABLE = "vec_chunks_cold"
_TIER_REGISTRY = "chunk_tier"
_TIER_META_TABLE = "tier_meta"
_TIER_TABLE_BY_NAME = {"hot": _TIER_HOT_TABLE, "warm": _TIER_WARM_TABLE, "cold": _TIER_COLD_TABLE}
# 弱结果加扫阈值 (设计 §二 Q2 原文: top-1 distance>0.38 噪音线 或 top-5 均值>0.42)
_TIER_WEAK_TOP1 = float(os.environ.get("MEMSVC_TIER_WEAK_TOP1", "0.38"))
_TIER_WEAK_TOP5_MEAN = float(os.environ.get("MEMSVC_TIER_WEAK_TOP5MEAN", "0.42"))
_TIER_PROMOTE_TOPN = int(os.environ.get("MEMSVC_TIER_PROMOTE_TOPN", "5"))  # 翻热: 进最终 top-N
# 层边界默认值 (rebuild 可收缩 hot; upsert 经 tier_meta.hot_days_effective 同源取值)
_TIER_HOT_DAYS_DEFAULT = float(os.environ.get("MEMSVC_TIER_HOT_DAYS", "7"))
_TIER_WARM_DAYS = float(os.environ.get("MEMSVC_TIER_WARM_DAYS", "30"))
_TIER_PROMOTION_LEDGER = SERVICE_DIR / "data" / "tier_promotions.jsonl"

# ── T-MEMDB-COLD-SPLIT-EXEC-0830 (2026-08-30, maintainer-approved): cold 层物理分库 ──────────
# 归属: 主库 = hot+warm 行 (chunks/vec_hot/vec_warm/chunk_tier[hot,warm]);
#       冷库 memory_cold.db = cold 行 (chunks/vec_chunks_cold/chunk_tier[cold])。
# 状态机 (主库 cold_split_meta 表, scripts/cold_split.py 管理):
#   无表/无行/off = 未分库(原行为); copying = 迁移中(读仍全主库, 对账跳过冷库方);
#   active = 冷库权威 (检索冷层走冷库, cold 层写入只落冷库+清主库同 id 旧拷贝)。
# 降级语义 (任务书自测3, maintainer directive"禁静默少返回"): 冷库不可读 → 主路径 hot/warm 照常
#   返回 + logger.error + 计数 (health/检索响应显式暴露 cold_db_degraded) —
#   不 raise: 冷层是弱结果补扫层 (W3 设计§二), 它的故障不应放倒主检索。
# killswitch: MEMSVC_COLD_SPLIT=off → 一切按未分库 (秒回退, 无需改库)。
COLD_DB_PATH = Path(os.environ.get("MEMSVC_COLD_DB", SERVICE_DIR / "memory_cold.db"))
_COLD_SPLIT_TABLE = "cold_split_meta"
_cold_state_cache: tuple[float, str] | None = None  # (ts, state)
_cold_counts_cache: tuple[float, dict[str, int]] | None = None  # (ts, counts)
_cold_degraded_count = 0
# T-FIX-T-ALERT-memory-service-degraded (2026-08-30): 主库侧 collection_count
# 逐集合 TTL 缓存 (60s, 镜像 52db99a 冷库侧 _cold_counts_cache 模式)。
# 失效纪律: 写路径统一走 _invalidate_cache(name) (R403 既有钩子) 同步 pop 本键
# — chunks INSERT/DELETE / tier 翻层 (_promote_cold_to_hot) / delete_chunks_by_ids
# 全部经它, 不另设失效点。外部进程直写库 (cold_split.py 迁移 / index_zcode_embed /
# index_claude_code_embed 整集合删 / 手工 sqlite3) 收不到进程内信号, 陈旧窗最坏
# ~60s (主库部分); 冷库部分叠加 _cold_collection_counts 自身 60s TTL, 最坏 ~120s
# (opus5 复审口径订正)。影响面: ==0 早退 (空→非空窗内误空, 下轮自愈) 与 vec 阈值
# 判断, 不影响检索正确性 (与 52db99a 同款取舍)。
_main_counts_cache: dict[str, tuple[float, int]] = {}  # safe_name -> (ts, count)


def _cold_split_state() -> str:
    """读主库分库状态 ('copying'/'active'/''), 30s TTL; 表不存在 = 未分库 (非错误).

    T-MEMSVC-IO-FIX-0930 #1: era 翻转钩子 — 状态变化 (active↔copying↔未分库)
    即时清 _main_cold_rows_cache: cold_split 回滚/迁移会把 cold 行成批写回/移出
    主库, 状态翻转是 cold 行真相整体重置的可靠信号 (比等 TTL 快一个数量级,
    且两个方向都保守: 清了只会多探一次, 不会用旧值)。
    """
    global _cold_state_cache, _main_cold_rows_cache
    if os.environ.get("MEMSVC_COLD_SPLIT", "on").lower() in ("off", "0", "false"):
        return ""
    now = time.time()
    if _cold_state_cache is not None and now - _cold_state_cache[0] < 30:
        return _cold_state_cache[1]
    prev = _cold_state_cache[1] if _cold_state_cache is not None else None
    state = ""
    try:
        conn = _get_conn()
        r = conn.execute(
            f"SELECT value FROM {_COLD_SPLIT_TABLE} WHERE key='state'"
        ).fetchone()
        conn.close()
        if r:
            state = str(r[0])
    except sqlite3.OperationalError as e:  # 表未建 = 从未分库 (命名: 明示吞的是"无表")
        pass
    except Exception as e:
        logger.warning(f"[cold-split] 状态读取失败 (视为未分库): {e}")
        state = ""
    if prev is not None and prev != state:
        _main_cold_rows_cache = None  # era 翻转: cold 行真相重置, 下次计数前重探
    _cold_state_cache = (now, state)
    return state


def _cold_split_active() -> bool:
    """分库是否生效 — 只看**数据状态** (state=active), 不耦合冷库文件可达性。

    ⚠️ T-03 变异教训: 若此处 AND 文件存在检查, 冷库文件丢失会静默回退"读主库
    stale 拷贝"模式 = 迁移后新冷块静默不可见 (任务书禁的静默少返回)。文件不可达
    的失败由 _get_cold_conn (mode=rw 不建空文件) 抛出 → 各调用点降级记账。
    """
    return _cold_split_state() == "active"


def _get_cold_conn() -> sqlite3.Connection:
    # mode=rw: 冷库文件缺失时连接失败 (而非静默新建空库掩盖丢库)
    conn = sqlite3.connect(f"file:{COLD_DB_PATH}?mode=rw", uri=True,
                           check_same_thread=False, timeout=5)
    conn.execute("PRAGMA busy_timeout=5000")
    # R1427: 冷库同享 WAL 调参 (实测本就 wal 模式; 同 _get_conn 口径)
    conn.execute(f"PRAGMA synchronous={_DB_SYNCHRONOUS}")
    conn.execute(f"PRAGMA journal_size_limit={_JOURNAL_SIZE_LIMIT}")
    conn.execute(f"PRAGMA wal_autocheckpoint={_WAL_AUTOCHECKPOINT}")
    if _VEC_AVAILABLE:
        try:
            conn.enable_load_extension(True)
            _sqlite_vec.load(conn)
        except Exception:
            logger.debug("[cold] sqlite_vec 载入失败 (cold conn)", exc_info=True)
    return conn


def _note_cold_degraded(where: str, exc: Exception) -> None:
    """冷库降级记账: error 日志 + 计数 (health/响应暴露) — 禁静默少返回."""
    global _cold_degraded_count
    _cold_degraded_count += 1
    logger.error(f"[cold-split] 冷库不可用@{where} (累计第 {_cold_degraded_count} 次): {exc}")


def cold_db_health() -> dict:
    """冷库健康块 (/api/health 暴露; 监控/验收用)."""
    h: dict = {"path": str(COLD_DB_PATH), "degraded_count": _cold_degraded_count}
    state = _cold_split_state()
    h["state"] = state or "off"
    if state != "active":
        return h
    if not COLD_DB_PATH.exists():
        h.update({"split_active": False, "status": "error", "reason": "state=active 但冷库文件缺失"})
        return h
    h["split_active"] = True
    try:
        conn = _get_cold_conn()
        n = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        conn.close()
        h.update({"status": "ok", "chunks": n})
    except Exception as e:
        h.update({"status": "error", "reason": str(e)})
    return h


def _invalidate_cold_counts() -> None:
    """冷库行数缓存失效 (冷写/删/翻热后调用 — 计数口径跟数据走)."""
    global _cold_counts_cache
    _cold_counts_cache = None


# ── T-MEMSVC-FIX-E2 (2026-09-02, E1 §4 C1a+C3): 主库 cold 注册行探测缓存 ──
# 背景 (E1 T-ROPE4-MEMSVC-E1B 实锤): 分库 active 后主库 chunk_tier cold 注册行已清零
# (当前 hot=38万/warm=66万/cold=0), 但 collection_count / _main_collection_counts /
# count_collection_chunks 三处反连接仍逐行执行相关子查询 → 单个大集合 5465ms vs
# plain COUNT 6ms (900 倍), 是慢查 p50=11.1s 中开销缺口 p50=7.6s 的主成分。
# 短路门: 主库零 cold 注册行 ⇒ 反连接排除集结构性恒空 ⇒ plain COUNT 同值。
# 失效联锁 (三道): ① _invalidate_cache 写路径统一尾钩同步清本缓存 — 进程内任何
#   写 (upsert/delete/promote) 都触发重探, 保守方向, 杜绝"未来新增 cold 写点漏挂
#   挂钩"的结构性缺口 (比只挂 upsert cold 分支更机制级); ② TTL 兜外部进程直写
#   (tier_rebuild / cold_split 回滚 / 手工 sqlite3 无进程内信号) — 档位随库形态
#   分档 (T-MEMSVC-IO-FIX-0930: 有 tier 索引 60s 对齐既有 _main_counts_cache
#   陈旧窗包络; 无索引 1800s, 依据见下方块头; 外部写者的计数陈旧本来就是被接受
#   的窗口, cold 排除集陈旧不引入新超纲方向); ③ 探测失败不落缓存且当次
#   按"有行"走反连接 — 慢但对, 禁假阴性 (假阴性会漏排除过渡期残留 cold 行 → 双算)。
#   注: 计数只用于 ==0 早退门/vec 阈值判/stats 展示, 不影响检索返回行集 — 双算
#   方向 (外部塞 cold 行 + 缓存 False) 最坏一个 TTL 窗计数偏大, ==0 门只会晚开
#   不会误开。era 翻转 (cold_split 状态变化) 由 _cold_split_state 钩子即时清缓存。
_main_cold_rows_cache: tuple[float, bool] | None = None  # (ts, has_cold_rows)
try:
    _MAIN_COLD_PROBE_TTL = float(os.environ.get("MEMSVC_MAIN_COLD_PROBE_TTL_S", "60"))
except ValueError:  # env 误设非数字/空串不致模块崩 (镜像 server._STATS_TTL_S 模式)
    _MAIN_COLD_PROBE_TTL = 60.0
# T-MEMSVC-IO-FIX-0930 #1: 探测成本索引感知 — 无 tier 索引时 EXISTS 证明"无 cold 行"
# 须全扫 168 万行 (I/O 停滞期实测卡 14.7s, T-MEMSVC-SLOW-0930 ⑨①), 每 60s 一次
# 就是尾部慢的直接机制。分两档:
#   有索引档 60s: DDL 方案 (docs/plans/chunk_tier_index_vacuum_20260930.md) 落地
#     CREATE INDEX idx_chunk_tier_tier 后探测 µs 级, 维持原频率语义零变化;
#   无索引档 1800s: 外部写者兜底窗 60s→30min。方向安全 — 缓存 False + 外部塞
#     cold 行最坏 = 计数偏大一个窗 (==0 门只晚开不会误开, 不影响检索返回行集,
#     E1 §4 既有口径); 会写主库 cold 行的运维 (cold_split 回滚 / tier_rebuild /
#     手工 sqlite3) 本就是停机窗操作, 且状态翻转走 _cold_split_state 的 era
#     钩子即时清本缓存。代码不自动建索引 (DDL 走 maintainer-approved window), 两种库形态都兼容。
try:
    _MAIN_COLD_PROBE_TTL_NO_INDEX = float(
        os.environ.get("MEMSVC_MAIN_COLD_PROBE_TTL_NOIDX_S", "1800"))
except ValueError:
    _MAIN_COLD_PROBE_TTL_NO_INDEX = 1800.0

# chunk_tier 显式索引探测 (sqlite_master 点查, 非 tier 表扫描), 300s TTL —
# 建索引是 deliberate 运维动作, 分钟级感知足够。
_chunk_tier_index_cache: tuple[float, bool] | None = None
_IDX_FORM_PROBE_TTL = 300.0


def _explicit_index_first_col(conn: sqlite3.Connection, table: str,
                              column: str) -> bool:
    """table 是否存在"首列 = column"的显式索引 (自动索引 sql 为 NULL 被排除).

    首列才算: 等值/EXISTS 只吃前导列 (chunks(source) / chunk_tier(tier))。
    """
    rows = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? "
        "AND sql IS NOT NULL", (table,)).fetchall()
    for (sql,) in rows:
        i = sql.rfind("(")
        if i < 0:
            continue
        cols = sql[i + 1:].rstrip(")").split(",")
        if cols and cols[0].strip().strip('"`[] ').lower() == column:
            return True
    return False


def _chunk_tier_tier_indexed() -> bool:
    global _chunk_tier_index_cache
    now = time.time()
    if (_chunk_tier_index_cache is not None
            and now - _chunk_tier_index_cache[0] < _IDX_FORM_PROBE_TTL):
        return _chunk_tier_index_cache[1]
    val = False
    try:
        conn = _get_conn()
        try:
            val = _explicit_index_first_col(conn, _TIER_REGISTRY, "tier")
        finally:
            conn.close()
    except Exception as e:  # 具名 catch: 吞的是目录探测自身失败, 按未建走
        logger.warning(f"[tier] chunk_tier tier 索引探测失败 (按未建处理): {e}")
    _chunk_tier_index_cache = (now, val)
    return val


def _main_cold_probe_ttl() -> float:
    """探测 TTL 有效档 (索引感知): 已建 60s / 未建 1800s, 见块头注释."""
    return _MAIN_COLD_PROBE_TTL if _chunk_tier_tier_indexed() else _MAIN_COLD_PROBE_TTL_NO_INDEX


def prewarm_main_cold_probe() -> None:
    """启动期后台探测一次 (T-MEMSVC-IO-FIX-0930 #1: 无索引库首个全扫离开搜索
    线程 — 每日 5 次计划重启后的首查不再付 10s+ 冷探测)。幂等, 立即返回。
    """
    threading.Thread(target=_main_has_cold_registry_rows,
                     name="cold-probe-prewarm", daemon=True).start()


def _main_has_cold_registry_rows(conn: sqlite3.Connection | None = None) -> bool:
    """主库 chunk_tier 是否存在 tier='cold' 行 (True=走反连接 / False=可短路).

    探测成本: 未建 tier 索引时 EXISTS 证明"无行"须全扫 (168 万行, I/O 停滞期
    14.7s) — 靠索引感知 TTL 分档 + 启动 prewarm 摊薄 (见块头注释), 不进检索
    热路径的每查成本。conn 可由调用方传入复用 (免二次建连); 不传则自开自关。
    """
    global _main_cold_rows_cache
    now = time.time()
    ttl = _main_cold_probe_ttl()
    if _main_cold_rows_cache is not None and now - _main_cold_rows_cache[0] < ttl:
        return _main_cold_rows_cache[1]
    own = conn is None
    try:
        if own:
            conn = _get_conn()
        _t0 = time.perf_counter()
        has = bool(conn.execute(
            f"SELECT EXISTS(SELECT 1 FROM {_TIER_REGISTRY} WHERE tier='cold')"
        ).fetchone()[0])
        _seg_add("exists_ms", time.perf_counter() - _t0)
    except Exception as e:
        # 具名 catch: 吞的是"探测查询自身失败"(锁/IO/表缺失),
        # 不吞调用方逻辑 — 保守按有行处理且不落缓存, 探测恢复后下轮自愈。
        logger.warning(f"[cold-split] 主库 cold 注册行探测失败 (按有行走反连接): {e}")
        return True
    finally:
        if own and conn is not None:
            conn.close()
    _main_cold_rows_cache = (now, has)
    return has


def _cold_lookup_rows(ids: list[str], exclude: dict, cols: str) -> dict:
    """冷库补捞 chunks 行 (主库 join 未命中的 id → 冷库; 分库后冷行真相在冷库).

    cols 由调用方给 (与主库查询同列序, 如 'id, text, metadata, source, embedding').
    """
    missing = [i for i in ids if i not in exclude]
    if not missing:
        return {}
    out: dict = {}
    conn = _get_cold_conn()
    try:
        for k in range(0, len(missing), 900):
            b = missing[k:k + 900]
            ph = ",".join("?" * len(b))
            for r in conn.execute(
                f"SELECT {cols} FROM chunks WHERE id IN ({ph})", b
            ).fetchall():
                out[r[0]] = r
    finally:
        conn.close()
    return out


def _cold_delete_ids(chunk_ids: list[str], conn: sqlite3.Connection | None = None) -> int:
    """冷库侧全删 (chunks + vec_chunks_cold + chunk_tier), 返回删 chunks 行数.

    conn: 调用方已开的冷库连接 (复用, 不关闭); None = 自开自关.
    """
    if not chunk_ids:
        return 0
    n = 0
    cconn = conn or _get_cold_conn()
    try:
        for k in range(0, len(chunk_ids), 900):
            b = chunk_ids[k:k + 900]
            ph = ",".join("?" * len(b))
            n += cconn.execute(f"DELETE FROM chunks WHERE id IN ({ph})", b).rowcount or 0
            cconn.execute(f"DELETE FROM vec_chunks_cold WHERE chunk_id IN ({ph})", b)
            cconn.execute(f"DELETE FROM chunk_tier WHERE chunk_id IN ({ph})", b)
        cconn.commit()
        _invalidate_cold_counts()
    finally:
        if conn is None:
            cconn.close()
    return n

_tier_layout_cache: tuple[float, list[str]] | None = None  # (ts, active_tables)
_tier_hot_days_cache: tuple[float, float] | None = None    # (ts, hot_days_effective)


def _tiering_enabled() -> bool:
    return os.environ.get("MEMSVC_TIERING", "on").lower() not in ("off", "0", "false")


def _tiered_single_enabled() -> bool:
    """P5 (T-MEMSVC-PARTITION-IMPL-0927): 单集合默认模式走 tiered 编排的开关."""
    return os.environ.get("MEMSVC_TIERED_SINGLE", "on").lower() not in ("off", "0", "false")


def _active_vec_tables() -> list[str]:
    """当前 vec0 扫描表集合: 三张分表齐 → tiered; 否则 [vec_chunks] (W1/W2 单层行为).

    60s TTL: tier_rebuild 在活库建表后, 运行中服务最长 60s 内跟进布局翻转
    (部署序列是 rebuild→restart, TTL 只是中途窗兜底; 期间读写仍走旧表,
    rebuild 尾部 --sync-stragglers 负责收敛中途写入)。
    """
    global _tier_layout_cache
    now = time.time()
    if _tier_layout_cache is not None and now - _tier_layout_cache[0] < 60:
        return _tier_layout_cache[1]
    try:
        conn = _get_conn()
        names = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?)",
                (_TIER_HOT_TABLE, _TIER_WARM_TABLE, _TIER_COLD_TABLE),
            )
        }
        conn.close()
        tables = (
            [_TIER_HOT_TABLE, _TIER_WARM_TABLE, _TIER_COLD_TABLE]
            if len(names) == 3 else ["vec_chunks"]
        )
    except Exception:
        tables = ["vec_chunks"]
    _tier_layout_cache = (now, tables)
    return tables


def _tiered_layout() -> bool:
    return _active_vec_tables()[0] == _TIER_HOT_TABLE


def _tiered_layout_safe(conn: sqlite3.Connection) -> bool:
    """Test/isolated DBs may intentionally replace _get_conn without tier tables.

    `_tiered_layout()` describes the configured production layout; this defensive
    variant falls back to the legacy scan when the optional registry is absent.
    """
    try:
        return _tiered_layout() and bool(conn.execute(
            f"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (_TIER_REGISTRY,),
        ).fetchone())
    except Exception:
        return False


def _effective_hot_days() -> float:
    """upsert 分层用的热层天数 (单一真相源 = tier_meta.hot_days_effective).

    tier_rebuild 写入 (按数据 derive); 表缺失/读失败回退 env 默认 (部署序列里
    rebuild 前的 upsert 用 7 天默认 — 该窗口行数极小, rebuild 全量重分派覆盖)。
    """
    global _tier_hot_days_cache
    now = time.time()
    if _tier_hot_days_cache is not None and now - _tier_hot_days_cache[0] < 300:
        return _tier_hot_days_cache[1]
    days = _TIER_HOT_DAYS_DEFAULT
    try:
        conn = _get_conn()
        r = conn.execute(
            f"SELECT value FROM {_TIER_META_TABLE} WHERE key='hot_days_effective'"
        ).fetchone()
        conn.close()
        if r:
            days = float(r[0])
    except Exception:
        pass
    _tier_hot_days_cache = (now, days)
    return days


def tier_for_chunk(metadata_json: str, source: str, updated_at: float, now: float | None = None,
                   hot_days: float | None = None) -> str:
    """upsert 写入侧分层: 按 chunk 自身源时间戳 (file_mtime) 判年龄。

    与 tier_rebuild 同口径: gold 越龄置热 (re-index 旧决策文件不坠冷层);
    re-index 的旧会话内容按真实源年龄落温/冷 (治 E1 "updated_at 显年轻" 毒性)。
    hot_days: 缺省取 _effective_hot_days() (本进程库的 tier_meta); 降温批量在新文件上
    跑时由调用方显式传入该文件的 tier_meta 值 (T-MEMSVC-PARTITION-IMPL-0927)。
    """
    now = now if now is not None else time.time()
    hot_days = _effective_hot_days() if hot_days is None else hot_days
    fmt = 0.0
    if metadata_json:
        try:
            fmt = float(json.loads(metadata_json).get("file_mtime") or 0)
        except Exception:
            fmt = 0.0
    if fmt <= 0:
        fmt = updated_at
    age = now - fmt
    if age <= hot_days * 86400:
        return "hot"
    if age <= _TIER_WARM_DAYS * 86400:
        return "warm"
    if _resolve_layer_for_vec(metadata_json, source) == "gold":
        return "hot"
    return "cold"


# ── T-MEMSVC-PARTITION-IMPL-0927 P2: W4 降温钟 (hot → warm) ──────────────────────
# 病灶 (T-MEMSVC-LATENCY-0925 §4): tier 只在写入时按源年龄判一次, W4 降温钟未实现 →
# 热层只进不出 (8/22 建层 8.3 万 → 9/27 71.8 万; 设计预算 tier_meta.max_hot_rows=15 万),
# 热层 float 全扫是跨集合检索主成本。降温规则 = tier_for_chunk 同口径 (源年龄超
# hot_days_effective 即出热), 保护 reason (probe/gold/hit) 不降 (W3 NOTES §十.2),
# 翻热行 (promoted) 满 hot_days 再降 (assigned_at 即翻热时刻)。
# 候选充分条件: assigned_at < now-hot_days (源时间 ≤ 入层时间 ⇒ 源龄必超 hot_days),
# 免逐行解析全部热行; 判后仍属热的 (gold 越龄置热) 改 reason='gold' 入保护, 不再重扫。
# 期1 边界: 只降到温 (同库搬), 源龄 >30 天的也落温不入冷 — 入冷=跨库三件套搬家且放大
# 弱结果冷扫成本 (冷扫是全冷分区), 另议。温层可见性: 降入行的 chunks.rowid 在 bit 索引
# watermark 之下, 增量刷新看不到 → 提交后 _bit_index.append_rows 显式推入。
_TIER_COOLDOWN_PROTECT = ("probe", "gold", "hit")


def _tier_cooldown_enabled() -> bool:
    return os.environ.get("MEMSVC_TIER_COOLDOWN", "on").lower() not in ("off", "0", "false")


def tier_cooldown_plan(conn: sqlite3.Connection, now: float | None = None,
                       limit: int | None = None, hot_days: float | None = None) -> dict:
    """扫描热层注册行, 规划降温 (只读)。返回 {demote, mark_gold, touch, scanned, hot_days}.

    demote=应降温的 chunk_id; mark_gold=gold 越龄置热 (reason→gold 入保护);
    touch=按源年龄仍属热 (源时间晚于入层, 罕见) → 刷 assigned_at 推迟复查。
    conn 由调用方给 (服务进程 / rebuild 脚本在新文件上共用同一规则)。
    """
    now = now if now is not None else time.time()
    hot_days = _effective_hot_days() if hot_days is None else float(hot_days)
    cut = now - hot_days * 86400
    ph = ",".join("?" * len(_TIER_COOLDOWN_PROTECT))
    sql = (f"SELECT r.chunk_id, c.metadata, c.source, c.updated_at "
           f"FROM {_TIER_REGISTRY} r JOIN chunks c ON c.id = r.chunk_id "
           f"WHERE r.tier = 'hot' AND COALESCE(r.reason, '') NOT IN ({ph}) "
           f"AND COALESCE(r.assigned_at, 0) < ?")
    params: list = [*_TIER_COOLDOWN_PROTECT, cut]
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))
    demote: list[str] = []
    mark_gold: list[str] = []
    touch: list[str] = []
    scanned = 0
    for cid, mj, src, upd in conn.execute(sql, params):
        scanned += 1
        if tier_for_chunk(mj, src, upd, now=now, hot_days=hot_days) != "hot":
            demote.append(cid)
        elif _resolve_layer_for_vec(mj, src) == "gold":
            mark_gold.append(cid)
        else:
            touch.append(cid)
    return {"demote": demote, "mark_gold": mark_gold, "touch": touch,
            "scanned": scanned, "hot_days": hot_days}


def _tier_cooldown_apply_batch(conn: sqlite3.Connection, ids: list[str], now: float) -> list[tuple]:
    """(调用方持 _write_lock 且已 BEGIN IMMEDIATE) 一批 hot→warm; 返回已搬行
    [(rowid, collection, embedding)] 供 bit 索引 append。事务内复核 tier 仍为 hot
    (候选规划后被 upsert/删除改过的行自然跳过)。embedding/layer 取 chunks 真相。"""
    if not ids:
        return []
    ph = ",".join("?" * len(ids))
    rows = conn.execute(
        f"SELECT c.id, c.rowid, c.embedding, c.collection, c.metadata, c.source "
        f"FROM chunks c JOIN {_TIER_REGISTRY} r ON r.chunk_id = c.id AND r.tier = 'hot' "
        f"WHERE c.id IN ({ph})", ids).fetchall()
    good = [r for r in rows if r[2] is not None and len(r[2]) == _BIT_EMBED_BYTES]
    bad = [r[0] for r in rows if not (r[2] is not None and len(r[2]) == _BIT_EMBED_BYTES)]
    if bad:  # 坏向量行留热 (不物化坏向量), 刷 assigned_at 免每拍重扫
        conn.executemany(f"UPDATE {_TIER_REGISTRY} SET assigned_at=? WHERE chunk_id=?",
                         [(now, b) for b in bad])
    if not good:
        return []
    gph = ",".join("?" * len(good))
    gids = [r[0] for r in good]
    conn.execute(f"DELETE FROM {_TIER_HOT_TABLE} WHERE chunk_id IN ({gph})", gids)
    conn.execute(f"DELETE FROM {_TIER_WARM_TABLE} WHERE chunk_id IN ({gph})", gids)
    conn.executemany(
        f"INSERT INTO {_TIER_WARM_TABLE}(chunk_id, embedding, collection, layer) VALUES (?,?,?,?)",
        [(r[0], r[2], r[3], _resolve_layer_for_vec(r[4], r[5])) for r in good])
    conn.executemany(
        f"UPDATE {_TIER_REGISTRY} SET tier='warm', reason='cooldown', assigned_at=? "
        f"WHERE chunk_id=? AND tier='hot'", [(now, g) for g in gids])
    return [(r[1], r[3], r[2]) for r in good]


def tier_cooldown_run(max_rows: int = 2000, batch: int = 500, now: float | None = None) -> dict:
    """降温钟一拍 (服务进程后台线程调用): 规划 → 分批 (≤batch 行/事务, 持 _write_lock)
    hot→warm → bit 索引 append。只在 tiered 布局下动作; 返回计数 (日志/测试用)。"""
    out = {"demoted": 0, "marked_gold": 0, "touched": 0, "scanned": 0, "skipped": ""}
    if not (_tiering_enabled() and _tiered_layout()):
        out["skipped"] = "not_tiered"
        return out
    now = now if now is not None else time.time()
    conn = _get_conn()
    try:
        plan = tier_cooldown_plan(conn, now=now, limit=max_rows)
    finally:
        conn.close()
    out["scanned"] = plan["scanned"]
    moved_total: list[tuple] = []
    todo = plan["demote"]
    for i in range(0, len(todo), max(1, batch)):
        chunk = todo[i:i + batch]
        with _write_lock:
            conn = _get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    moved = _tier_cooldown_apply_batch(conn, chunk, now)
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
            finally:
                conn.close()
        moved_total.extend(moved)
        if moved:
            _bit_index.append_rows(moved)
    marks = [(g,) for g in plan["mark_gold"]]
    touches = [(now, t) for t in plan["touch"]]
    if marks or touches:
        with _write_lock:
            conn = _get_conn()
            try:
                if marks:
                    conn.executemany(
                        f"UPDATE {_TIER_REGISTRY} SET reason='gold' WHERE chunk_id=? AND tier='hot'",
                        marks)
                if touches:
                    conn.executemany(
                        f"UPDATE {_TIER_REGISTRY} SET assigned_at=? WHERE chunk_id=? AND tier='hot'",
                        touches)
                conn.commit()
            finally:
                conn.close()
    global _tier_counts_cache
    if moved_total:
        _tier_counts_cache = None
    out.update(demoted=len(moved_total), marked_gold=len(marks), touched=len(touches))
    return out


class VecFallbackError(Exception):
    """GLM执行审HOLD修 (2026-08-15): 大集合 vec0 故障护栏 — per-request 异常回执.

    用异常替代裸 return []: 并发下全局计数差值会串扰 (A触发污染B的degraded),
    异常按请求传播到调用方, 且调用方可区分'真没有' vs '护栏拦截'.
    """

    def __init__(self, collection: str):
        super().__init__(f"vec0 不可用, 大集合 {collection} 禁止物化降级")
        self.collection = collection


def _count_vec_fallback() -> None:
    """P1-1: vec0 降级次数累计 (供监控/诊断; 单进程内)."""
    global _vec_fallback_count
    _vec_fallback_count += 1


# ── T-MEMSVC-IO-FIX-0930 #3: 纯路径 query 走 source 精确/后缀 (免 embed 免向量) ──
# 依据 T-MEMSVC-SLOW-0930 ⑧: 当日 7 条 >8s 慢查全是 agent 会话拿任务文件路径原文
# 当检索词 (5 条为路径原文) — 语义价值低, 却付整条向量链 (embed + bit fetch +
# 弱结果冷扫)。路径检索的真实意图 = "找这(类)文件的记忆块", source 列精确/后缀
# 匹配即语义正解, 且走既有 idx_chunks_source 索引 (主库已有; 冷库随 DDL 方案窗
# 补建后自动并入)。killswitch: MEMSVC_PATH_ROUTE=off 或 /tmp 旗标文件 (R1)。
# R1 判据收紧: 尾组件必须带"点+字母开头扩展名" — 排除纯数字尾段 (如版本号
# 1.2.3 / 纯数字代码) 与无扩展名的端点/目录形态 (/api/
# search、…/inbox/), 这些匹配不到 source 且语义价值本就在向量侧。
_PATH_EXT_RE = re.compile(r"\.[A-Za-z][A-Za-z0-9]{0,9}$")
_PATH_LINE_NO_RE = re.compile(r":\d+(-\d+)?$")
_PATH_QUERY_MAXLEN = 512
_PATH_SUFFIX_CAP = 400      # 后缀候选 rowid 上限 (覆盖索引限量扫, 防大库全捞)
_PATH_ROUTE_OFF_FLAG = "/tmp/memsvc_path_route_off"  # R1: touch 即关, 免 launchd 改 env 重启
_path_route_count = 0       # 已服务路径检索计数 (观测用)
_chunks_source_index_cache: tuple[float, bool] | None = None


def _path_route_enabled() -> bool:
    """路径路由开关: env MEMSVC_PATH_ROUTE=off, 或 /tmp 旗标文件存在 (R1)。

    旗标做法同 SERVICE_DIR/.no_warmup, 但放 /tmp — touch/rm 即时生效且不脏
    仓库目录 (复审: env 开关要改 launchd 环境变量+重启 = 需 maintainer approval, 不是
    随手能拉的闸)。每查一次 exists() (~µs 级), 检索频次下开销可忽略。"""
    if os.path.exists(_PATH_ROUTE_OFF_FLAG):
        return False
    return os.environ.get("MEMSVC_PATH_ROUTE", "on").lower() not in ("off", "0", "false")


def _normalize_path_query(q: str) -> tuple[str, list[str]]:
    """路径 query 规范化 (R1) → (主候选, 精确点查补充候选)。

    1. 去 :行号 尾缀 (…/T-A.md:42 → …/T-A.md — 库内 source 无行号);
    2. 去 ./ ../ ~/ 前缀 (库内 source 全为绝对路径, 相对形式转纯相对走尾缀
       匹配; ~ 不做 expanduser — 家目录展开值与库内绝对路径尾部等价, 尾缀
       同样命中);
    3. 无前导 / 且以 Users/ 开头 (真实流量 = 绝对路径掉了首斜杠, 复审 §一.3)
       → 追加 /Users/… 精确点查候选 (索引点查 O(log n), 免整条尾缀索引扫)。
    """
    s = (q or "").strip()
    s = _PATH_LINE_NO_RE.sub("", s)
    while True:
        if s.startswith("./"):
            s = s[2:]
        elif s.startswith("../"):
            s = s[3:]
        elif s.startswith("~/"):
            s = s[2:]
        else:
            break
    extra: list[str] = []
    if not s.startswith("/") and s.startswith("Users/"):
        extra.append("/" + s)
    return s, extra


def is_path_query(q: str) -> bool:
    """纯路径字符串判据 (判据即契约, 全部满足才路由; R1 收紧):

    1. strip 后非空且 ≤512 字符;
    2. 不含任何空白字符 (混路径+语义词的 query 仍走向量);
    3. 规范化 (去 :行号 / ./ ../ ~/ 前缀) 后, 尾组件带"点+字母开头扩展名"
       (1-10 位字母数字、首字符字母, 如 .md/.jsonl/.py) — 排除纯数字尾段
       (纯数字尾段如 1.2.3/02513) 与无扩展名的端点/目录 (/api/search);
    4. 路径形态: 以 / ~/ ./ ../ 开头 (绝对/相对路径), 或规范化后含 '/'。
    排除: 裸文件名 (无斜杠)、"记忆/规则"类无扩展名短语、含空格语义 query、
    超长串、目录路径 (无扩展名)。
    """
    s = (q or "").strip()
    if not s or len(s) > _PATH_QUERY_MAXLEN:
        return False
    if any(ch.isspace() for ch in s):
        return False
    norm, _ = _normalize_path_query(s)
    if _PATH_EXT_RE.search(norm) is None:
        return False
    return s.startswith(("/", "~/", "./", "../")) or "/" in norm


def _like_escape(s: str) -> str:
    """LIKE 通配符转义 (路径常含 _ — 不转义会伪命中)."""
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _chunks_source_indexed() -> bool:
    """主库 chunks 是否有首列=source 的索引 (sqlite_master 点查, 300s TTL)."""
    global _chunks_source_index_cache
    now = time.time()
    if (_chunks_source_index_cache is not None
            and now - _chunks_source_index_cache[0] < _IDX_FORM_PROBE_TTL):
        return _chunks_source_index_cache[1]
    val = False
    try:
        conn = _get_conn()
        try:
            val = _explicit_index_first_col(conn, "chunks", "source")
        finally:
            conn.close()
    except Exception as e:  # 命名: 吞的是目录探测自身失败, 按"不支持"走原路径
        logger.warning(f"[path-route] chunks source 索引探测失败 (按无索引处理): {e}")
    _chunks_source_index_cache = (now, val)
    return val


def _path_item(row: tuple, dist: float) -> dict:
    meta = json.loads(row[3]) if row[3] else {}
    return {
        "id": row[0], "collection": row[1], "text": row[2], "metadata": meta,
        "source": row[4], "distance": dist, "score": dist, "time_decay": 1.0,
    }


def _path_exact(conn: sqlite3.Connection, pathq: str, coll: str | None,
                n: int) -> list[tuple]:
    """source 精确等值 (索引点查); 行 = (id, collection, text, metadata, source,
    updated_at) 按 updated_at 降序 LIMIT n。INDEXED BY 强制走 source 索引 —
    无索引的库 (如生产冷库现状) 抛 OperationalError 由调用方处理, 绝不退化成
    裸表扫 (3-21GB 宽行)。"""
    if coll:
        return conn.execute(
            "SELECT id, collection, text, metadata, source, updated_at "
            "FROM chunks INDEXED BY idx_chunks_source "
            "WHERE source = ? AND collection = ? "
            "ORDER BY updated_at DESC LIMIT ?", (pathq, coll, n)).fetchall()
    return conn.execute(
        "SELECT id, collection, text, metadata, source, updated_at "
        "FROM chunks INDEXED BY idx_chunks_source WHERE source = ? "
        "ORDER BY updated_at DESC LIMIT ?", (pathq, n)).fetchall()


def _path_suffix(conn: sqlite3.Connection, pathq: str, coll: str | None,
                 n: int, seen_ids: set) -> list[tuple]:
    """source 后缀匹配 (相对路径 query vs 绝对 source): 覆盖索引限量扫 rowid
    (INDEXED BY 强制 — 无索引的库抛 OperationalError 由调用方处理, 绝不退化成
    裸表扫) → 批量回捞行 → collection 过滤 → updated_at 降序取 n。"""
    esc = "%" + _like_escape(pathq)
    rids = [r[0] for r in conn.execute(
        f"SELECT rowid FROM chunks INDEXED BY idx_chunks_source "
        f"WHERE source LIKE ? ESCAPE '\\' LIMIT {_PATH_SUFFIX_CAP}", (esc,))]
    if not rids:
        return []
    rows: list[tuple] = []
    for k in range(0, len(rids), 900):
        b = rids[k:k + 900]
        ph = ",".join("?" * len(b))
        # rowid IN 走表 PK 点查 (不加 INDEXED BY — 强制 source 索引会退化为全索引扫)
        rows.extend(conn.execute(
            f"SELECT id, collection, text, metadata, source, updated_at "
            f"FROM chunks WHERE rowid IN ({ph})", b))
    out = [r for r in rows if r[0] not in seen_ids and (coll is None or r[1] == coll)]
    out.sort(key=lambda r: r[5] or 0, reverse=True)
    return out[:n]


def search_by_source(pathq: str, collection: str | None = None,
                     n_results: int = 5) -> list[dict] | None:
    """路径检索: 精确 source 命中 + 后缀兜底 (主库索引点查/覆盖扫, 免向量).

    返回 None = 回退向量路径, 两种情形同语义:
    ① 库形态不支持 (主库无首列 source 索引) — 本函数绝不在此形态下裸扫大表;
    ② 精确+尾缀 (主库+冷库) 全 0 命中 (R1) — 复审回放 19% 路径查询库内无此
      文件, 返回空列表会顶掉旧路径的向量近邻, 回退向量这 19% 付旧耗时、其余
      照样提速。
    规范化 (R1): 去 :行号 / ./ ../ ~/ 前缀; 无前导 / 的 Users/… 补 / 走精确
    点查; 绝对路径形态 (含补斜杠变体) 命中即止, 不再跑整条尾缀索引扫 (绝对
    路径已唯一定位, 尾缀同尾文件是不同文件)。min_distance/where/mmr 对路径
    检索无意义, 不适用; 结果排序 = 精确(0.0) > 后缀(0.1), 同级按 updated_at
    降序。冷库: 有同款索引才并入 (缺索引静默跳过不算 degraded — "索引已建/
    未建兼容"; 生产冷库现状无, DDL 方案窗补建后自动并入)。
    """
    global _path_route_count, _chunks_source_index_cache
    if not _chunks_source_indexed():
        return None
    try:
        n = max(1, min(int(n_results or 5), 50))
    except (TypeError, ValueError):
        n = 5
    coll = _safe_name(collection) if collection else None
    norm, extra_exact = _normalize_path_query(pathq)
    abs_form = norm.startswith("/") or bool(extra_exact)
    exact: list[tuple] = []
    suffix: list[tuple] = []
    conn = _get_conn()
    try:
        _t0 = time.perf_counter()
        # 精确点查: 主候选 + 补斜杠变体 (Users/…), 任一命中即止
        for cand in [norm] + extra_exact:
            exact = _path_exact(conn, cand, coll, n)
            if exact:
                break
        # 尾缀兜底: 仅相对形态 (绝对路径已唯一定位, 免整条索引扫 — R1)
        if not exact and not abs_form and _PATH_EXT_RE.search(norm):
            suffix = _path_suffix(conn, norm, coll, n, set())
        _seg_add("path_ms", time.perf_counter() - _t0)
    except sqlite3.OperationalError as e:
        if "no such index" in str(e):
            # 命名: 探测缓存 (300s TTL) 与真实形态的窄竞态窗 — 索引被并发 DDL 删掉。
            # 回退向量路径 (与"不支持"同语义), 不裸扫。
            logger.warning(f"[path-route] 主库 source 索引探测/实态竞态, 回退向量: {e}")
            _chunks_source_index_cache = None
            return None
        raise
    finally:
        conn.close()
    # 冷库并集 (分库 active 且冷库有同款索引; 缺索引静默跳过不算 degraded —
    # "索引已建/未建兼容"; 真冷库故障照 _note_cold_degraded 记账, 主结果照常)
    if _cold_split_active():
        try:
            cconn = _get_cold_conn()
            try:
                cexact: list[tuple] = []
                for cand in [norm] + extra_exact:
                    cexact = _path_exact(cconn, cand, coll, n)
                    if cexact:
                        break
                csuffix: list[tuple] = []
                if not cexact and not abs_form and _PATH_EXT_RE.search(norm):
                    csuffix = _path_suffix(cconn, norm, coll, n, set())
            finally:
                cconn.close()
            exact += cexact
            suffix += csuffix
        except sqlite3.OperationalError as e:
            if "no such index" in str(e):
                # 命名: 吞的是"冷库无 idx_chunks_source (生产冷库现状)" — 索引
                # 形态差异非故障, 静默跳过冷库段; DDL 方案窗补建后自动并入。
                logger.debug(f"[path-route] 冷库段跳过 (无 source 索引形态): {e}")
            else:
                # 命名: 吞的是冷库连接自身故障 — 主结果照常但照记账 (禁静默少返回
                # 不留痕, 与 _note_cold_degraded 全库口径一致)
                _note_cold_degraded("path_route_cold", e)
    if not exact and not suffix:
        # R1: 全 0 命中 → None 回退向量 (见 docstring ②), 不计 serve
        logger.info(f"[path-route] q={pathq[:80]!r} coll={coll!r} 0 命中 → 回退向量")
        return None
    exact.sort(key=lambda r: r[5] or 0, reverse=True)
    suffix.sort(key=lambda r: r[5] or 0, reverse=True)
    items = ([_path_item(r, 0.0) for r in exact[:n]]
             + [_path_item(r, 0.1) for r in suffix[:max(0, n - min(len(exact), n))]])
    _path_route_count += 1
    logger.info(f"[path-route] q={pathq[:80]!r} coll={coll!r} exact={len(exact)} "
                f"suffix={len(suffix)} serve={len(items)} (#{_path_route_count})")
    return items


def _should_use_vec(collection_name: str) -> bool:
    """hybrid 策略: 大集合走 vec0, 小集合走 legacy. 只在 vec 可用时判."""
    if not _check_vec_usable():
        return False
    try:
        n = collection_count(collection_name)
    except Exception:
        return False
    return n >= _VEC_HYBRID_THRESHOLD


def search(
    collection_name: str,
    query: str,
    query_embedding: list[float],
    n_results: int = 5,
    where: dict | None = None,
    min_distance: float | None = None,
    use_mmr: bool = True,
    use_feedback: bool = True,
    mode: str | None = None,
    gold_admission_distance: float = 0.50,
    tier_filter_hw: bool = False,
    seg_meta: dict | None = None,
) -> list[dict]:
    """单集合检索入口 (T-MEMSVC-IO-FIX-0930 #4 外壳: 分段计时 + seg_meta 出参).

    seg_meta: 调用方可选出参通道 — 检索结束后写入 ``seg_meta["db_seg_ms"]``
    (bit fetch / COUNT / EXISTS / 向量 / 合并 各段 ms, server 侧并入 slow-query
    归因)。嵌套调用 (跨集合编排内) 不夺外层上下文。
    """
    _owned = _seg_begin()
    _t0 = time.perf_counter()
    try:
        return _search_core(
            collection_name, query, query_embedding, n_results=n_results,
            where=where, min_distance=min_distance, use_mmr=use_mmr,
            use_feedback=use_feedback, mode=mode,
            gold_admission_distance=gold_admission_distance,
            tier_filter_hw=tier_filter_hw,
        )
    finally:
        if _owned:  # 嵌套层墙钟已含在外层 total 内, 不重复累加
            _seg_add("total_ms", time.perf_counter() - _t0)
        seg = _seg_collect(_owned)
        if seg is not None and seg_meta is not None:
            seg_meta["db_seg_ms"] = seg


def _search_core(
    collection_name: str,
    query: str,
    query_embedding: list[float],
    n_results: int = 5,
    where: dict | None = None,
    min_distance: float | None = None,
    use_mmr: bool = True,
    use_feedback: bool = True,
    mode: str | None = None,
    gold_admission_distance: float = 0.50,
    tier_filter_hw: bool = False,
) -> list[dict]:
    # MS-C hybrid: 大集合走 vec0 (消灭整矩阵物化), 小集合走 legacy numpy (快).
    # 失败自动降级 legacy (服务不中断) — P1-1 护栏 (GLM-5.3 复审): 大集合 vec0 故障
    # 时禁止降级物化整矩阵 (~430MB×3), 否则重演 OOM/watchdog kickstart 循环.
    # 大集合 → 返回 [] + 告警计数 (等 vec0 恢复); 小集合 (<阈值) 照旧 legacy 安全降级.
    #
    # W3 tier_filter_hw: True 时 legacy 矩阵只装 hot+warm 行 (tiered 默认路径的小集合
    # 段, 冷层移出默认扫描)。vec 分支忽略此参 (大集合 tiered 路径不经 search());
    # where/mode 调用方不传它 (需全池语义)。
    if _should_use_vec(collection_name):
        try:
            # T-MEMSVC-PARTITION-IMPL-0927 P5: 单集合默认模式 (无 where/mode) 走 tiered
            # (热分区 float 精确 + 温 bit 集合内粗召回精排 + 弱结果才扫该集合冷分区),
            # 与跨集合默认路径同一编排 (_tiered_cross_search single=True)。where/layered
            # 仍走三表并集精确扫 (需全池语义)。killswitch MEMSVC_TIERED_SINGLE=off。
            if (where is None and mode is None and _tiered_single_enabled()
                    and _tiering_enabled() and _tiered_layout()):
                return _tiered_cross_search(
                    query_embedding, [collection_name], [], n_results, use_mmr,
                    min_distance, use_feedback, {"query": (query or "")[:200]},
                    single=True,
                )
            _t_vec = time.perf_counter()
            try:
                r = _search_vec(
                    collection_name,
                    query_embedding,
                    n_results=n_results,
                    where=where,
                    min_distance=min_distance,
                    use_mmr=use_mmr,
                    use_feedback=use_feedback,
                    mode=mode,
                )
            finally:
                _seg_add("vec_ms", time.perf_counter() - _t_vec)
            return r
        except Exception:
            _count_vec_fallback()
            if collection_count(collection_name) >= _VEC_HYBRID_THRESHOLD:
                logger.error(f"[vec0] {collection_name} 大集合({collection_count(collection_name)}行) vec0 故障, "
                             f"禁止物化降级 — raise 护栏 (累计 {_vec_fallback_count} 次)")
                raise VecFallbackError(collection_name)
            logger.exception("[vec0] _search_vec 异常, 小集合降级 legacy 本次查询")

    # ── legacy 路径 (整矩阵物化 + numpy 点积) ──
    _t_leg = time.perf_counter()
    embs, ids = _load_collection_embeddings(
        collection_name, tiers=("hot", "warm") if (tier_filter_hw and _tiered_layout()) else None)
    if len(ids) == 0:
        return []

    q = np.array(query_embedding, dtype=np.float32)
    q = q / (np.linalg.norm(q) + 1e-10)

    candidate_multiplier = 5 if use_mmr else 3
    fetch_count = min(n_results * candidate_multiplier, len(ids))
    # where 过滤需要更大候选池 (传了 where 才放大, L3 默认路径不变)
    if where:
        fetch_count = len(ids)

    distances = 1.0 - embs @ q

    top_idx = np.argpartition(distances, min(fetch_count, len(distances) - 1))[:fetch_count]
    top_idx = top_idx[np.argsort(distances[top_idx])]

    # layered 两段式: 独立取金层 top-k, 不被 session(86%)距离竞争淹没 (设计 §0).
    # 默认模式不走此分支, top_idx 即全部候选 (L3 逐行不变).
    gold_extra_positions: np.ndarray = np.array([], dtype=int)
    if mode == "layered":
        gold_id_set = _get_gold_ids(collection_name)
        if gold_id_set:
            gpos = np.array([i for i, cid in enumerate(ids) if cid in gold_id_set])
            if len(gpos) > 0:
                gd = distances[gpos]
                gk = min(fetch_count, len(gpos))
                gtop = np.argpartition(gd, gk - 1)[:gk]
                gold_extra_positions = gpos[gtop]

    # 合并候选位置 (默认 top + 金层 top), 按距离排序去重
    all_positions = sorted(
        set(top_idx.tolist()) | set(gold_extra_positions.tolist()),
        key=lambda i: distances[i],
    )
    top_ids = [ids[i] for i in all_positions]

    conn = _get_conn()
    # R1245: 同 _search_vec 回连 — 去冗余 `AND collection=?` (id 全局 PK, 候选 ids
    # 本就来自 _compute_collection_arrays 的 WHERE collection=? 物化, 去谓词零语义差;
    # 误plan 机制见 _search_vec 回连处注释)。
    placeholders = ",".join("?" * len(top_ids))
    rows = conn.execute(
        f"SELECT id, text, metadata, source FROM chunks WHERE id IN ({placeholders})",
        top_ids,
    ).fetchall()
    conn.close()

    row_map = {r[0]: r for r in rows}
    # T-MEMDB-COLD-SPLIT: 全量矩阵路径 (tiers=None) 候选含冷行 → 冷库补捞未命中 id
    if _cold_split_active() and len(row_map) < len(top_ids):
        try:
            row_map.update(_cold_lookup_rows(top_ids, row_map, "id, text, metadata, source"))
        except Exception as e:
            _note_cold_degraded("search_legacy_join", e)

    items = []
    now = time.time()
    half_life = SEARCH.time_decay_half_life
    for i in all_positions:
        chunk_id = ids[i]
        dist = float(distances[i])
        if min_distance is not None and dist >= min_distance:
            continue
        row = row_map.get(chunk_id)
        if not row:
            continue
        meta = json.loads(row[2]) if row[2] else {}
        # 复活 where: 元数据过滤 (传了 where 才执行, 设计 §1 洞A)
        if where and not _match_where(meta, row[3] or "", where):
            continue
        file_mtime = float(meta.get("file_mtime", 0))
        decay = _time_decay(file_mtime, half_life, now)
        score = dist / max(decay, 0.01)
        if use_feedback:
            factor = get_feedback_factor(chunk_id)
            if factor != 1.0:
                score = score / factor

        items.append({
            "id": chunk_id,
            "collection": collection_name,
            "text": row[1],
            "metadata": meta,
            "distance": round(dist, 6),
            "time_decay": round(decay, 4),
            "score": round(score, 6),
            "_emb": embs[i].tolist(),
            "_layer": meta.get("layer") or classify_layer(row[3] or ""),
        })

    # 两段式召回 (mode='layered') 或纯距离 (默认, L3 灰度铁律: 逐行不变)
    if mode == "layered":
        # layered: 返回候选池 (金层 extra + session, 带 _layer 标记, 不做最终选择/MMM).
        # 调用方全库合并后用 layered_select 做最终选择 (单 collection 金层会被淹没, 设计 §0).
        for item in items:
            item.pop("_emb", None)
        _seg_add("vec_ms", time.perf_counter() - _t_leg)  # legacy 矩阵算作向量段
        return items

    items.sort(key=lambda x: x["score"])

    if use_mmr and len(items) > n_results:
        items = _mmr_rerank(items, query_embedding, SEARCH.mmr_lambda, n_results)
    else:
        items = items[:n_results]

    for rank, item in enumerate(items):
        item.pop("_emb", None)
        item.pop("_layer", None)
        if use_mmr:
            item["mmr_rank"] = rank

    _seg_add("vec_ms", time.perf_counter() - _t_leg)  # legacy 矩阵算作向量段
    return items


def get_stats() -> dict:
    # T-MEMSVC-HANG-ROOTCAUSE (2026-08-14): 旧查询 LEFT JOIN chunks 走
    # idx_chunks_dedup(collection, text_sha256) — 索引含 64 字符 hash, 701k chunks
    # 实测 7.7s 冷 / 1.4s 热, 卡死 /api/health (10s 看门狗阈值) → 当日 8 次
    # silent-hang kickstart。改单表 GROUP BY 走 idx_chunks_collection covering
    # index (0.02s, 365x), Python 合并 collections 保空集合显示 0。
    conn = _get_conn()
    names = [r[0] for r in conn.execute("SELECT name FROM collections ORDER BY name").fetchall()]
    conn.close()
    # T-MEMDB-COLD-SPLIT: 主库排除 cold 层 + 冷库并集 (过渡期主库残留拷贝不双算)
    counts = _main_collection_counts(exclude_cold=True)
    for c, n in _cold_collection_counts().items():
        counts[c] = counts.get(c, 0) + n
    return {
        "total_collections": len(names),
        "collections": [{"name": n, "count": counts.get(n, 0)} for n in names],
    }


def get_client():
    return None


def list_collections() -> list[str]:
    conn = _get_conn()
    rows = conn.execute("SELECT name FROM collections ORDER BY name").fetchall()
    conn.close()
    return [r[0] for r in rows]


def _cold_collection_counts() -> dict[str, int]:
    """冷库逐集合行数 (60s TTL 缓存 — 检索分类循环每查询调用多次, 禁反复扫).

    ⚠️ 与主库计数的关系: 分库 active 后冷行**单一归属在冷库**, 主库可能仍残留
    未 cleanup 的旧拷贝 (过渡期, 主库 cold 集 ⊆ 冷库集) — 调用方主库侧必须
    排除 cold 层 (见 collection_count), 否则过渡期双算。
    """
    global _cold_counts_cache
    if not _cold_split_active():
        return {}
    now = time.time()
    if _cold_counts_cache is not None and now - _cold_counts_cache[0] < 60:
        return _cold_counts_cache[1]
    try:
        conn = _get_cold_conn()
        counts = dict(conn.execute(
            "SELECT collection, COUNT(*) FROM chunks GROUP BY collection").fetchall())
        conn.close()
        _cold_counts_cache = (now, counts)
        return counts
    except Exception as e:
        _note_cold_degraded("collection_counts", e)
        return {}


def _main_collection_counts(exclude_cold: bool) -> dict[str, int]:
    """主库逐集合行数; exclude_cold=分库 active 时 (冷行归属冷库, 防过渡期双算).

    T-MEMSVC-FIX-E2 (C3): exclude_cold 且主库零 cold 注册行时短路 plain GROUP BY
    (反连接版 2712ms → plain 毫秒级, E1 §4 C3; 排除集恒空时两版同值)。"""
    conn = _get_conn()
    _t0 = time.perf_counter()
    if exclude_cold and _cold_split_active() and _main_has_cold_registry_rows(conn):
        counts = dict(conn.execute(
            "SELECT collection, COUNT(*) FROM chunks c "
            f"WHERE NOT EXISTS (SELECT 1 FROM {_TIER_REGISTRY} t "
            f"WHERE t.chunk_id = c.id AND t.tier = 'cold') "
            "GROUP BY collection").fetchall())
    else:
        counts = dict(conn.execute(
            "SELECT collection, COUNT(*) FROM chunks GROUP BY collection").fetchall())
    _seg_add("cnt_ms", time.perf_counter() - _t0)
    conn.close()
    return counts


def collection_count(collection_name: str) -> int:
    """集合行数 (60s TTL 缓存 — T-FIX-T-ALERT-memory-service-degraded 2026-08-30).

    背景: 检索热路径每搜必调 (server._do_search ==0 早退 / _should_use_vec 阈值判 /
    search_cross_collection 分类循环), cold-split active 后主库侧带 chunk_tier 反连接
    (377k 行实测冷 8.67s/热 1.86s), 迁移期写负载抢锁 → 慢查率 64.3% 持续 degraded。
    缓存 + 写路径 _invalidate_cache 同步失效; 语义边界见 _main_counts_cache 注释。

    opus5 复审加固 (HOLD 三缺陷):
    ① 冷库降级值禁落缓存 — _cold_collection_counts 失败时返回 {} (只记 degraded
      不抛), "少算"结果若入缓存, 全 cold 集合在冷库抖动时会 ==0 早退连续 TTL 返回
      空且不报错; 故检测 _cold_degraded_count 前后增量, 有增量 = 本值含降级口径,
      不写缓存 (本次返回仍正确可用的主库侧值, 下轮重查)。
    ② 版本 fence — miss 查询 2-10s 窗口内并发写 (indexer watcher 同进程) 会
      _invalidate_cache pop 空键, 慢查回填旧值照样陈旧一个 TTL; 故查前抓
      _collection_versions[name] (失效钩子已在自增), 查完版本没变才落缓存。
    线程安全: dict 单键 get/set/pop 原子; 竞争窗最坏 = 两线程同时 miss 各查一次
    (值相同), 与 _known_collections (R1260) 同款语义。
    """
    name = _safe_name(collection_name)  # key 归一 (幂等), 与 _invalidate_cache 失效键对齐
    now = time.time()
    hit = _main_counts_cache.get(name)
    if hit is not None and now - hit[0] < 60:
        return hit[1]
    # fence 抓点必须在 _get_conn() 之前: 覆盖"连接建立→SQL 执行→落缓存"整窗
    # (opus5 T12 复审实测: 抓点在连接后漏检连接时刻的并发写)。
    degraded_before = _cold_degraded_count
    ver_before = _collection_versions.get(name, 0)
    conn = _get_conn()
    _t0 = time.perf_counter()
    if _cold_split_active():
        # T-MEMSVC-FIX-E2 (C1a): 主库零 cold 注册行 → 反连接排除集恒空,
        # plain COUNT 同值 (~6ms vs 反连接 5465ms, E1 实测 900 倍);
        # 过渡态残留 cold 行时照走反连接防双算。冷库侧仍需并入 (冷行真相在冷库)。
        if _main_has_cold_registry_rows(conn):
            count = conn.execute(
                f"SELECT COUNT(*) FROM chunks c WHERE c.collection = ? "
                f"AND NOT EXISTS (SELECT 1 FROM {_TIER_REGISTRY} t "
                f"WHERE t.chunk_id = c.id AND t.tier = 'cold')",
                (name,),
            ).fetchone()[0]
        else:
            count = conn.execute(
                "SELECT COUNT(*) FROM chunks WHERE collection=?", (name,)
            ).fetchone()[0]
        conn.close()
        count += _cold_collection_counts().get(name, 0)
    else:
        count = conn.execute(
            "SELECT COUNT(*) FROM chunks WHERE collection=?", (name,)
        ).fetchone()[0]
        conn.close()
    _seg_add("cnt_ms", time.perf_counter() - _t0)
    # ① 冷库降级过 (少算口径) / ② 查询窗内有并发写 (版本已翻) → 不落缓存
    if (
        _cold_degraded_count == degraded_before
        and _collection_versions.get(name, 0) == ver_before
    ):
        _main_counts_cache[name] = (now, count)
    return count


def get_existing_chunks(collection_name: str, file_path: str) -> dict[int, dict]:
    """Read existing chunks for a file.

    2026-05-18: embedding 改返回 numpy ndarray (之前 .tolist() 把 4KB 紧凑数组
    转 28KB Python list, 540K chunks 时 startup 时 _init_project 触发 ~15GB bloat,
    macOS 压缩成 phys_footprint 21GB peak. 现在保持 numpy float32, 体积 1/7.

    注意 (GLM5.1 提醒): np.frombuffer 是 view, 基于 sqlite blob bytes.
    sqlite3 cursor.fetchall() 返回的 bytes 是 Python-owned 对象, 视图有 base 引用
    所以 conn.close() 之后还活. 但 .copy() 显式独立内存更安全, 释放原 blob.
    单 chunk copy 开销 = 4KB memcpy, 可接受.
    """
    name = _safe_name(collection_name)
    conn = _get_conn()
    rows = conn.execute(
        "SELECT id, metadata, embedding FROM chunks WHERE collection=? AND source=?",
        (name, file_path),
    ).fetchall()
    conn.close()
    # T-MEMDB-COLD-SPLIT: 旧文件冷块在冷库 — 并入 (idx 撞车时主库优先, 同 idx 同内容)
    if _cold_split_active():
        try:
            cconn = _get_cold_conn()
            crows = cconn.execute(
                "SELECT id, metadata, embedding FROM chunks WHERE collection=? AND source=?",
                (name, file_path),
            ).fetchall()
            cconn.close()
            have = set()
            for r in rows:
                m = json.loads(r[1]) if r[1] else {}
                have.add(m.get("chunk_index", 0))
            rows.extend(r for r in crows
                        if (json.loads(r[1]) if r[1] else {}).get("chunk_index", 0) not in have)
        except Exception as e:
            _note_cold_degraded("get_existing_chunks", e)
    existing = {}
    for r in rows:
        meta = json.loads(r[1]) if r[1] else {}
        idx = meta.get("chunk_index", 0)
        emb_blob = r[2]
        # 5/18: 改 numpy + copy (替代 .tolist() 防 7x bloat); copy 切断对 sqlite blob 的 view 引用
        emb = np.frombuffer(emb_blob, dtype=np.float32).copy() if emb_blob else None
        existing[idx] = {"id": r[0], "hash": meta.get("chunk_hash", ""), "embedding": emb}
    return existing


def delete_chunks_by_ids(chunk_ids: list[str], collection_name: str | None = None):
    if not chunk_ids:
        return
    conn = _get_conn()
    placeholders = ",".join("?" * len(chunk_ids))
    # 删前先取出涉及的集合名, 用于按集合粒度失效缓存 (R403). 调用方能传 collection_name
    # 时优先用 (indexer.py 传 collection, 避免一次 DISTINCT 扫描).
    if collection_name is None:
        try:
            touched = [
                r[0]
                for r in conn.execute(
                    f"SELECT DISTINCT collection FROM chunks WHERE id IN ({placeholders})",
                    chunk_ids,
                ).fetchall()
            ]
        except Exception as e:
            logger.warning(f"delete_chunks_by_ids: 取涉及集合失败, 退化为全清: {e}")
            touched = []
    else:
        touched = [collection_name]
    conn.execute(f"DELETE FROM chunks WHERE id IN ({placeholders})", chunk_ids)
    # MS-C: vec0 同步删除 (chunk_id 是 vec_chunks 主键, 直接 IN 删)
    if _VEC_AVAILABLE and _vec_initialized:
        try:
            _batch_delete_vec(conn, chunk_ids)
        except Exception:
            logger.warning("[vec0] delete_chunks_by_ids 同步删 vec_chunks 失败 — 将触发对账告警", exc_info=True)
    conn.commit()
    conn.close()
    # T-MEMDB-COLD-SPLIT: 冷库同 id 同删
    if _cold_split_active():
        try:
            _cold_delete_ids(chunk_ids)
        except Exception as e:
            _note_cold_degraded("delete_chunks_by_ids", e)
    if touched:
        for _c in touched:
            _invalidate_cache(_c)
    else:
        # ids 已不在库中查不到集合 → 安全兜底全清一次 (极少数边界情况).
        _invalidate_cache(None)


def load_collection_prefix_hashes(collection_name: str) -> set[tuple]:
    """跨文件内容去重: 返回集合内所有 chunk 的 (chunker_version, prefix_hash) 元组集合。

    两条索引路径 (server.py watcher + cron) 启动时调用此函数, 加载后存
    内存 set, 入库前检查 → 跳过内容重复的 chunk (6/9 回灌事故根因修复)。

    去重键含版本盐 (chunker_version, prefix_hash): v1 和 v2 切块边界差 ±1 字符,
    同内容不同版本的 prefix_hash 可能相同 — 不加版本盐会挡掉合法 v2 重切块,
    导致迁移悄悄卡死 (2026-06-11 架构师审查暗礁 1 修复)。
    """
    # MEMIDX (T-MEMIDX-PILOT-IMPL): 去重指纹**按版本感知** —— v2-pilot 块用 content_hash(全内容),
    # 其余用 prefix_hash(前200)。必须与入库侧(indexer/cron 的 change#1)用**同一指纹**, 否则重载后
    # 集合装 prefix_hash 而入库侧查 content_hash → 失配 → 内容重复块跨轮回灌(change#1 收益蒸发)。
    from chunking import prefix_hash as _ph, content_hash as _ch
    name = _safe_name(collection_name)
    conn = _get_conn()
    rows = conn.execute(
        "SELECT text, json_extract(metadata, '$.chunker_version') FROM chunks WHERE collection = ?",
        (name,),
    ).fetchall()
    conn.close()
    # T-MEMDB-COLD-SPLIT: 冷行指纹并入 (防旧文件冷块跨轮回灌)
    if _cold_split_active():
        try:
            cconn = _get_cold_conn()
            rows.extend(cconn.execute(
                "SELECT text, json_extract(metadata, '$.chunker_version') FROM chunks WHERE collection = ?",
                (name,),
            ).fetchall())
            cconn.close()
        except Exception as e:
            _note_cold_degraded("load_prefix_hashes", e)
    result = set()
    for (t, ver) in rows:
        v = str(ver) if ver is not None else "1"
        result.add((v, _ch(t) if v == "v2-pilot" else _ph(t)))
    return result


def search_openclaw_memory(query_embedding: list[float], n_results: int = 5) -> list[dict]:
    """Search OpenClaw's own memory DBs for cross-system access."""
    results = []
    q = np.array(query_embedding, dtype=np.float32)
    q = q / (np.linalg.norm(q) + 1e-10)

    if not OPENCLAW_MEMORY_DIR.exists():
        return results

    for db_file in OPENCLAW_MEMORY_DIR.glob("*.sqlite"):
        if db_file.name.endswith("-wal") or db_file.name.endswith("-shm"):
            continue
        try:
            conn = sqlite3.connect(str(db_file))
            rows = conn.execute(
                "SELECT id, path, text, embedding FROM chunks WHERE embedding IS NOT NULL LIMIT 5000"
            ).fetchall()
            conn.close()
        except Exception:
            continue

        if not rows:
            continue

        for row in rows:
            try:
                emb = json.loads(row[3]) if isinstance(row[3], str) else row[3]
                if not emb or len(emb) != len(query_embedding):
                    continue
                emb_arr = np.array(emb, dtype=np.float32)
                emb_norm = np.linalg.norm(emb_arr)
                if emb_norm == 0:
                    continue
                emb_arr = emb_arr / emb_norm
                dist = float(1.0 - np.dot(q, emb_arr))
                results.append({
                    "id": row[0],
                    "text": row[1][:200] if row[1] else "",
                    "content": row[2][:500] if row[2] else "",
                    "distance": round(dist, 6),
                    "source": f"openclaw:{db_file.stem}",
                    "metadata": {"source": f"openclaw:{db_file.stem}", "path": row[1] or ""},
                })
            except Exception:
                continue

    results.sort(key=lambda x: x["distance"])
    return results[:n_results]


# MS-C: 模块尾启动初始化 — 此时 _get_conn / _init_vec0 / logger 等都已定义.
# 建表幂等; 是否走 vec 搜索由首次 search 的 _check_vec_usable() 判.
if _VEC_AVAILABLE:
    _init_vec0()
