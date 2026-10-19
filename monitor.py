"""Monitoring metrics collection for memory-service.

Thread-safe in-memory metrics: request counts, latency tracking, disk info.

2026-08-15 T-MEMSVC-DEGRADED-FIX: search slow 统计从"进程内只增不重置的粘性
计数"改为滚动 1h 窗口 (deque 事件流)。窗内 slow 事件滚出后计数自动下降,
health 无需重启即自愈 (根治 8/15 一天 3 次的 degraded 粘性误报)。

2026-08-17 R1335 (W1.5 防御): in-flight 慢查 watchdog — 90s 慢查再现时, 外部
HTTP 健康探针不触发 (服务仍响应), 事后 dump 又只抓到空闲栈 (8/17 14:42 实测)。
机制: /api/search 进场登记 in-flight, watchdog 线程轮询, 龄期 ≥ 阈值时**趁卡顿
进行中**用 faulthandler dump 全线程 Python 栈 (与外部 hang dump 同目录, 文件名
inflight_stall_ 前缀区分)。killswitch: MEMSVC_STALL_WATCHDOG=off。
"""

import logging
import os
import shutil
import threading
import time
from collections import deque
from pathlib import Path

from config import MONITOR, DATA_DIR

logger = logging.getLogger("memory-service.monitor")

# ---------------------------------------------------------------------------
# Internal metrics store (thread-safe)
# ---------------------------------------------------------------------------

_metrics = {
    "search_count": 0,
    "search_total_ms": 0.0,
    "search_max_ms": 0.0,
    "embed_count": 0,
    "embed_total_ms": 0.0,
    "embed_max_ms": 0.0,
    "capture_count": 0,
    "capture_total_ms": 0.0,
    "capture_max_ms": 0.0,
    "error_count": 0,
    "start_time": time.time(),
}

# 滚动窗口事件流: (timestamp, duration_ms, project)。maxlen 仅作 OOM 兜底:
# 生产实测 ~18 请求/h (2026-08-15), 100000 容量 = 失真需 55 请求/s 持续 1h,
# 距现实 3 个数量级; 正常场景先被时间窗剪枝 (flash审D2: 防 maxlen 先于窗口截断)。
_search_events: deque = deque(maxlen=100000)

_metrics_lock = threading.Lock()


def _prune_search_events(now: float) -> None:
    """Drop events older than the rolling window. Caller holds _metrics_lock."""
    window = MONITOR.slow_window_seconds
    while _search_events and (now - _search_events[0][0]) > window:
        _search_events.popleft()


def _slow_query_label(detail: dict | None) -> str:
    """Best-effort label of which collection a search targeted."""
    project = (detail or {}).get("project") or ""
    return project if project else "(cross-collection)"


def _cache_attribution(detail: dict | None) -> str:
    """T-MEMSVC-SLOWQUERY-CACHEMISS: slow 查询的缓存归因短串 (判据用)。

    组合 db.matrix_cache_overlaps (查询窗口内发生过的矩阵缓存 miss 加载) +
    调用方塞进 detail 的 phase 分解 (embed_ms / search_ms / cache 加载明细)。
    读侧锁外完成 (只遍历 deque, 不写); db import 延迟到首次 slow 才发生。
    任何异常只降级为空串 — 绝不影响请求路径。
    """
    try:
        d = detail or {}
        now = time.time()
        dur_ms = float(d.get("duration_ms") or 0.0)
        overlaps = []
        db = __import__("db")
        overlaps = db.matrix_cache_overlaps(now - dur_ms / 1000.0 - 0.5, now)
        parts = []
        if overlaps:
            worst = max(overlaps, key=lambda o: o["load_ms"] + o["wait_ms"])
            parts.append(
                f"cache_miss_overlap={len(overlaps)}"
                f"(worst {worst['collection']} load={worst['load_ms']:.0f}ms"
                f" wait={worst['wait_ms']:.0f}ms)"
            )
        else:
            parts.append("cache_miss_overlap=0")
        ph = d.get("phase_ms")
        if isinstance(ph, dict):
            parts.append(
                "phase=embed {:.0f}ms/db_search {:.0f}ms".format(
                    ph.get("embed_ms") or 0.0, ph.get("search_ms") or 0.0
                )
            )
            # T-MEMSVC-IO-FIX-0930 #4: db 分段计时 (bit fetch/COUNT/EXISTS/向量/合并)
            # — 慢查归因不再靠 stall dump 猜。键随检索路径出现 (R1 订正为实测
            # 口径): 向量链 = fetch/bit/cnt/exists/vec/merge/total, 路径路由 =
            # path_ms (server 路径分支开分段上下文后真实落值, R1 前为空操作)。
            seg = ph.get("db_seg_ms")
            if isinstance(seg, dict) and seg:
                parts.append(
                    "db[" + " ".join(
                        f"{k}={v:.0f}ms" for k, v in sorted(seg.items())) + "]"
                )
        return " ".join(parts)
    except Exception as e:
        # 具名 catch: 吞的是归因计算自身异常 (db import 失败/deque 竞态等) —
        # 归因降级为空串绝不影响请求; debug 痕迹留排障线索区分"无overlap"vs"归因崩了"。
        logger.debug(f"[cache-attribution] degraded: {e!r}")
        return ""


def record_request(
    endpoint: str,
    duration_ms: float,
    success: bool = True,
    detail: dict | None = None,
) -> None:
    """Record a single API request's metrics (thread-safe).

    Classifies the endpoint and updates the corresponding counters.

    Args:
        endpoint: URL path of the request (e.g. ``/api/search``).
        duration_ms: Response time in milliseconds.
        success: Whether the request completed without error. 口径与旧版一致:
            仅成功请求进 search 统计/滚动窗口; 失败请求只计 error_count (flash审D6)。
        detail: Optional context. For ``/api/search``: ``project`` (target
            collection, empty = cross-collection) and ``query`` (search text,
            only used for slow-query logging). T-MEMSVC-SLOWQUERY-CACHEMISS:
            可选 ``phase_ms`` (embed/db_search 分解, server 塞入) 供 slow 归因。
    """
    # slow 归因在锁外算 (db.matrix_cache_overlaps 只读遍历; import 也可能首次加载)
    cache_ctx = ""
    if (
        success
        and endpoint == "/api/search"
        and duration_ms > MONITOR.slow_threshold_ms
    ):
        d = dict(detail or {})
        d["duration_ms"] = duration_ms
        cache_ctx = _cache_attribution(d)

    with _metrics_lock:
        if not success:
            _metrics["error_count"] += 1
            return

        if endpoint == "/api/search":
            _metrics["search_count"] += 1
            _metrics["search_total_ms"] += duration_ms
            _metrics["search_max_ms"] = max(_metrics["search_max_ms"], duration_ms)
            now = time.time()
            _prune_search_events(now)
            _search_events.append(
                (now, duration_ms, (detail or {}).get("project") or "", cache_ctx)
            )
            if duration_ms > MONITOR.slow_threshold_ms:
                # 逐条 slow 日志 (T-MEMSVC-DEGRADED-FIX 判据4): 治"复发时明细
                # 不可得"数据缺口。查询文本截 60 字符并去控制字符。
                # T-MEMSVC-SLOWQUERY-CACHEMISS: 附缓存归因 (overlap+phase) —
                # 每条 slow 自带"慢是不是 miss"的判据, 可直接证伪。
                query = str((detail or {}).get("query") or "")[:60]
                query = "".join(ch for ch in query if ch.isprintable())
                logger.warning(
                    f"[slow-query] {duration_ms:.0f}ms "
                    f"(threshold {MONITOR.slow_threshold_ms:.0f}ms, "
                    f"window {MONITOR.slow_window_seconds:.0f}s) "
                    f"project={_slow_query_label(detail)} "
                    f"{cache_ctx} "
                    f"query={query!r}"
                )
        elif endpoint in ("/api/memories", "/api/feedback"):
            _metrics["capture_count"] += 1
            _metrics["capture_total_ms"] += duration_ms
            _metrics["capture_max_ms"] = max(_metrics["capture_max_ms"], duration_ms)
        elif endpoint in (
            "/api/reindex",
            "/api/register",
            "/api/backup",
            "/api/restore",
            "/api/archive-snapshots",
        ):
            _metrics["embed_count"] += 1
            _metrics["embed_total_ms"] += duration_ms
            _metrics["embed_max_ms"] = max(_metrics["embed_max_ms"], duration_ms)


def get_metrics() -> dict:
    """Return a snapshot of all collected metrics.

    Returns:
        Dict with request counts, average latencies, max latencies,
        error count, uptime, and rolling-window slow-query statistics.
        ``search.slow_count`` 是滚动窗口内计数 (旧版为进程生命周期累计,
        粘性已移除); lifetime count/avg/max 保留供 /api/monitor 兼容。
    """
    with _metrics_lock:
        uptime = time.time() - _metrics["start_time"]
        sc = _metrics["search_count"]
        ec = _metrics["embed_count"]
        cc = _metrics["capture_count"]
        _prune_search_events(time.time())
        threshold = MONITOR.slow_threshold_ms
        window_count = len(_search_events)
        slow_events = [e for e in _search_events if e[1] > threshold]
        slow_count = len(slow_events)
        slow_recent = [
            {
                "ts": round(e[0], 1),
                "duration_ms": round(e[1], 1),
                "project": e[2] or "(cross-collection)",
                **({"cache": e[3]} if len(e) > 3 and e[3] else {}),
            }
            for e in slow_events[-10:]
        ]
        matrix_cache = None
        try:
            # T-MEMSVC-SLOWQUERY-CACHEMISS: 与 slow 同窗口口径的矩阵缓存统计。
            # db import 延迟到读侧 (monitor 单测/独立脚本不依赖 db 也能跑)。
            matrix_cache = __import__("db").matrix_cache_stats(
                MONITOR.slow_window_seconds
            )
        except Exception as e:
            logger.debug(f"matrix_cache_stats unavailable: {e}")
        return {
            "uptime_seconds": round(uptime, 1),
            "search": {
                "count": sc,
                "avg_ms": round(_metrics["search_total_ms"] / sc, 1) if sc > 0 else 0,
                "max_ms": round(_metrics["search_max_ms"], 1),
                "slow_count": slow_count,
                "window_count": window_count,
                "slow_ratio": round(slow_count / window_count, 4) if window_count > 0 else 0.0,
                "window_seconds": MONITOR.slow_window_seconds,
                "slow_recent": slow_recent,
            },
            "capture": {
                "count": cc,
                "avg_ms": round(_metrics["capture_total_ms"] / cc, 1) if cc > 0 else 0,
                "max_ms": round(_metrics["capture_max_ms"], 1),
            },
            "index": {
                "count": ec,
                "avg_ms": round(_metrics["embed_total_ms"] / ec, 1) if ec > 0 else 0,
                "max_ms": round(_metrics["embed_max_ms"], 1),
            },
            "error_count": _metrics["error_count"],
            "slow_threshold_ms": MONITOR.slow_threshold_ms,
            "matrix_cache": matrix_cache,  # embeddings 矩阵 LRU (db.py); 与 embedder 的 "cache" (文本→向量) 不同物
        }


def disk_info(path: str | None = None) -> dict:
    """Get disk usage information for the given path (defaults to DATA_DIR).

    Args:
        path: Path to check. Defaults to ``DATA_DIR``.

    Returns:
        Dict with ``total_gb``, ``free_gb``, ``used_gb``, and ``used_pct`` keys.
        Returns empty dict on error.
    """
    try:
        target = path or str(DATA_DIR)
        usage = shutil.disk_usage(target)
        total = round(usage.total / (1024**3), 1)
        free = round(usage.free / (1024**3), 1)
        used = round(usage.used / (1024**3), 1)
        used_pct = round(usage.used / usage.total * 100, 1) if usage.total > 0 else 0
        return {
            "total_gb": total,
            "free_gb": free,
            "used_gb": used,
            "used_pct": used_pct,
        }
    except Exception as e:
        logger.warning(f"Failed to get disk info: {e}")
        return {}


# ---------------------------------------------------------------------------
# R1335 (W1.5 防御): in-flight 慢查 watchdog — 90s stall 再现时进行中抓栈
# ---------------------------------------------------------------------------

# 与外部 footprint monitor 的 hang dump 同目录 (P01), 便于一处归档比对。
STALL_DUMP_DIR = Path(os.environ.get(
    "MEMSVC_STALL_DUMP_DIR", str(DATA_DIR / "logs" / "stall_dumps")))

_inflight_lock = threading.Lock()
_inflight: dict[int, tuple[float, str]] = {}   # token → (t0, label)
_inflight_next = 0
_stall_watchdog_started = False
_stall_last_dump_ts = 0.0


def _env_flag(name: str, default: str = "on") -> bool:
    return os.environ.get(name, default).lower() not in ("off", "0", "false")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def inflight_begin(label: str = "search") -> int:
    """登记一个进行中的请求 (server.py /api/search 进场调用)。返回 token。"""
    global _inflight_next
    with _inflight_lock:
        _inflight_next += 1
        _inflight[_inflight_next] = (time.time(), label)
        return _inflight_next


def inflight_end(token: int) -> None:
    """注销进行中请求 (finally 里必达; 未知 token 静默 = 防重复 end 炸查询)。"""
    with _inflight_lock:
        _inflight.pop(token, None)


def inflight_snapshot() -> list[tuple[int, float, str]]:
    """当前 in-flight 快照: [(token, age_secs, label)]。watchdog 判定用。"""
    now = time.time()
    with _inflight_lock:
        return [(tok, now - t0, label) for tok, (t0, label) in _inflight.items()]


def _dump_stall_scene(stalled: list[tuple[int, float, str]]) -> Path | None:
    """趁 stall 进行中 dump 全线程 Python 栈。失败只告警不抛 (绝不影响服务)。"""
    ts = time.strftime("%Y%m%d_%H%M%S")
    try:
        STALL_DUMP_DIR.mkdir(parents=True, exist_ok=True)
        path = STALL_DUMP_DIR / f"inflight_stall_{ts}.txt"
        import faulthandler
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"=== memsvc in-flight stall dump @ {ts} ===\n")
            f.write(
                f"stalled: "
                + ", ".join(f"[token={t} age={a:.1f}s label={l}]" for t, a, l in stalled)
                + "\n--- faulthandler all-thread traceback ---\n"
            )
            f.flush()
            faulthandler.dump_traceback(file=f)
        return path
    except Exception as e:  # dump 失败不影响服务 (具名 catch)
        logger.warning(f"[stall-watchdog] dump 失败: {type(e).__name__}: {e}")
        return None


def _stall_watchdog_loop() -> None:
    """轮询 in-flight; 龄期 ≥ 阈值且过冷却 → dump 栈。env 动态读 (免重启可调)。"""
    global _stall_last_dump_ts
    while True:
        time.sleep(_env_float("MEMSVC_STALL_POLL_SECS", 5.0))
        if not _env_flag("MEMSVC_STALL_WATCHDOG", "on"):
            continue
        threshold = _env_float("MEMSVC_STALL_STACK_SECS", 30.0)
        cooldown = _env_float("MEMSVC_STALL_DUMP_COOLDOWN", 300.0)
        stalled = [(t, a, l) for t, a, l in inflight_snapshot() if a >= threshold]
        if not stalled:
            continue
        now = time.time()
        if now - _stall_last_dump_ts < cooldown:
            continue
        _stall_last_dump_ts = now
        logger.warning(
            "[stall-watchdog] in-flight 龄期 %s ≥ %.1fs → 全线程栈 dump",
            ",".join(f"{a:.1f}s" for _, a, _ in stalled), threshold,
        )
        _dump_stall_scene(stalled)


def start_stall_watchdog() -> None:
    """启动 stall watchdog 线程 (幂等; daemon)。server.py 启动期调用。"""
    global _stall_watchdog_started
    if _stall_watchdog_started:
        return
    _stall_watchdog_started = True
    t = threading.Thread(target=_stall_watchdog_loop, name="stall-watchdog", daemon=True)
    t.start()
    logger.info(
        "[stall-watchdog] started (threshold=%ss poll=%ss dump_dir=%s)",
        os.environ.get("MEMSVC_STALL_STACK_SECS", "30"),
        os.environ.get("MEMSVC_STALL_POLL_SECS", "5"),
        STALL_DUMP_DIR,
    )
