"""File indexer: chunks files, embeds them, and stores via db.py (SQLite + sqlite-vec vec0)."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from fnmatch import fnmatch
from typing import TYPE_CHECKING

from watchdog.observers import Observer

from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent

from config import ProjectConfig, PROJECTS_DIR, _env_num, DEFAULT_EXCLUDE

# 2026-08-15 maintainer decision: watcher 默认从"实时 embed"改为"固定窗批量 embed" (GPU 空闲优先).
# 窗口 = 首事件起 N 秒后 flush (不会饥饿); 会话内容最多延迟一个窗口进记忆.
# 重启兜底链 = 启动 _init_project 全量重扫 × B5 offset 续读尾巴 (watermark 只防重读, 不单独兜底).
# env 可调: MEMSVC_WATCH_DEBOUNCE=3600 (1h); 0=回实时旧行为.
# GLM执行审HOLD修: _env_num 防非法 env 导入期 ValueError 起不来 (config.py:17 防御模式).
WATCH_DEBOUNCE_SECONDS = _env_num("MEMSVC_WATCH_DEBOUNCE", 3600.0)
from db import get_or_create_collection, upsert_chunks, get_existing_chunks, delete_chunks_by_ids, load_collection_prefix_hashes, classify_layer, count_collection_chunks
from embedder import embed_batch, embed_single

logger = logging.getLogger("memory-service.indexer")

_file_hashes: dict[str, str] = {}
_last_full_index: dict[str, float] = {}

# 跨文件内容去重: 每个集合的 prefix_hash 集合 (服务启动时懒加载, 之后增量维护)
# B1 修复 (2026-06-11): 与 cron 路径共用 db.load_collection_prefix_hashes + chunking.prefix_hash
_collection_prefix_hashes: dict[str, set[str]] = {}
_prefix_hashes_lock = threading.Lock()

# ── B5: 增量读 — 治"整读巨文件"内存泄漏 (NIGHT-20260804/B5) ──
# 会话 jsonl 文件 append-only + 实测近 GB, watcher 每次 mtime 变都 p.read_text() 整读
# → 单次 857MB alloc + split("\n") 16.8万块滞留 → 堆碎片 + RSS 震荡 (5/16 碎片化真身)。
# 修法: jsonl 大文件走 seek(last_offset) 只读新增 tail; offset 持久化到服务目录 (铁律: 不放 /tmp)。
_INCREMENTAL_THRESHOLD = 5 * 1024 * 1024   # 5MB: jsonl 超此走增量路径 (小文件整读无压力)
_MAX_TAIL_BYTES = 16 * 1024 * 1024         # 16MB: 单次 tail 读取上限 (防一次 append 巨量爆)
_HARD_FILE_LIMIT = 50 * 1024 * 1024        # 50MB: 非 jsonl 巨文件硬闸 — 跳过整读防复发泄漏 (②兜底)
_OFFSET_STATE_FILE = Path(__file__).parent / "data" / "jsonl_offsets.json"
_last_offsets: dict[str, int] = {}
_offset_lock = threading.Lock()
_offsets_loaded = False


def _load_offsets() -> None:
    """懒加载持久化 offset (重启不丢)。"""
    global _offsets_loaded
    if _offsets_loaded:
        return
    with _offset_lock:
        if _offsets_loaded:
            return
        try:
            if _OFFSET_STATE_FILE.exists():
                _last_offsets.update(
                    json.loads(_OFFSET_STATE_FILE.read_text(encoding="utf-8"))
                )
                logger.info("B5 jsonl_offsets loaded: %d files", len(_last_offsets))
        except Exception as exc:
            logger.warning("B5 jsonl_offsets load failed: %s", exc)
        _offsets_loaded = True


def _save_offsets() -> None:
    """原子写 offset 快照 (tmp+replace 防半写)。"""
    with _offset_lock:
        snapshot = dict(_last_offsets)
    try:
        _OFFSET_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _OFFSET_STATE_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
        tmp.replace(_OFFSET_STATE_FILE)
    except Exception as exc:
        logger.warning("B5 jsonl_offsets save failed: %s", exc)


def _ensure_prefix_hashes(collection_name: str) -> None:
    """懒加载: 若集合 prefix_hash 集合未初始化, 从 DB 全量加载一次。"""
    if collection_name in _collection_prefix_hashes:
        return
    with _prefix_hashes_lock:
        if collection_name in _collection_prefix_hashes:
            return
        try:
            hashes = load_collection_prefix_hashes(collection_name)
            _collection_prefix_hashes[collection_name] = hashes
            logger.info(
                "prefix_hash init: collection=%s loaded=%d",
                collection_name, len(hashes),
            )
        except Exception as exc:
            logger.warning("prefix_hash init failed for %s: %s", collection_name, exc)
            _collection_prefix_hashes[collection_name] = set()

# Index activity metrics (thread-safe)
_index_metrics = {
    "last_index_time": 0.0,
    "files_indexed": 0,
    "chunks_embedded": 0,
}
_index_metrics_lock = threading.Lock()


def _file_hash(path: str) -> str:
    """mtime_ns + size 双因子指纹 — T-MEMSVC-WALK-SPEEDUP (flash D1/CC GateA C1 同源):
    旧版 mtime 秒级截断, 持久化后同秒二次修改会被长期跳过 (问题被放大), 故升 ns+size;
    二者均来自 stat, 不读文件内容, 与旧版同为 O(1)。
    """
    try:
        st = Path(path).stat()
        return f"mtime_ns:{st.st_mtime_ns}:size={st.st_size}"
    except Exception:
        return ""


# ── T-MEMSVC-WALK-SPEEDUP P1: _file_hashes 持久化 (治"重启即全量重扫 2.5h") ──
# 设计: 值=stat 指纹 (不读内容) → 重启后 walk = 纯 stat 比对, 未变文件零读取零切块。
# 红线: 不碰 memory.db schema (缓存落独立 json); 损坏降级 = 空 dict 走全量 (不比今日更坏)。
# CC GateA C1: 持久化必须先 dict() 快照 (watcher 8 线程池与 walk 并发改全局 dict,
#              直接迭代会 RuntimeError: dictionary changed size during iteration)。
# CC GateA C2: 缓存-DB 绑定兜底 — memory.db 重建/恢复而缓存还在 → 全 skip → 检索永久缺数据;
#              verify_hash_cache_against_db() 在 walk 前对每个 project 查 collection COUNT=0 丢条目。
_HASH_CACHE_FILE = Path(__file__).parent / "data" / "file_hash_cache.json"
_persist_lock = threading.Lock()
# flash 执行审 D1: 迭代方锁 — persist/verify/cleanup 对 _file_hashes 做批量迭代/删除时
# 持此锁; 写点(index_file 各处)是单条 STORE_SUBSCR (CPython 原子, 不迭代), 不需要锁。
# 真正会崩的是"迭代中另一线程增删键" → 锁住全部迭代方即可。
_hashes_iter_lock = threading.Lock()
_HASH_CACHE_ENABLED = _env_num("MEMSVC_HASH_CACHE", 1) > 0
_last_flush_persist_ts: float = 0.0
_HASH_CACHE_FORMAT_RE = None  # 懒编译 (re 导入在文件头部已有)


def _load_hashes() -> None:
    """启动时加载持久化缓存。整体失败(文件缺失/json 坏/version 不匹配)→ 保持空 dict 走全量;
    单条 value 格式坏 → 只丢该条 (flash 条件3: 逐条容错, 不整体降级)。"""
    if not _HASH_CACHE_ENABLED:
        return
    global _HASH_CACHE_FORMAT_RE
    if _HASH_CACHE_FORMAT_RE is None:
        _HASH_CACHE_FORMAT_RE = re.compile(r"^mtime_ns:\d+:size=\d+$")
    try:
        if not _HASH_CACHE_FILE.exists():
            return
        obj = json.loads(_HASH_CACHE_FILE.read_text(encoding="utf-8"))
        if not isinstance(obj, dict) or obj.get("version") != 1:
            logger.warning("hash cache version mismatch/damaged, full rescan (expected v1)")
            return
        entries = obj.get("entries")
        if not isinstance(entries, dict):
            return
        loaded = 0
        for k, v in entries.items():
            if isinstance(k, str) and isinstance(v, str) and _HASH_CACHE_FORMAT_RE.match(v):
                _file_hashes[k] = v
                loaded += 1
        logger.info("hash cache loaded: %d entries from %s", loaded, _HASH_CACHE_FILE)
    except Exception:
        logger.exception("hash cache load failed, full rescan this boot")


def _persist_hashes() -> None:
    """原子持久化 (tmp + os.replace)。锁内先快照再序列化 (C1), 单写者防 tmp 互覆。"""
    if not _HASH_CACHE_ENABLED:
        return
    try:
        with _persist_lock:
            with _hashes_iter_lock:  # flash 执行审 D1: 快照持迭代锁
                snapshot = dict(_file_hashes)  # C1: 快照, 不迭代活 dict
            payload = json.dumps({"version": 1, "entries": snapshot}, ensure_ascii=False)
            _HASH_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)  # flash 次要: data 目录防御
            tmp = _HASH_CACHE_FILE.with_suffix(".json.tmp")
            tmp.write_text(payload, encoding="utf-8")
            os.replace(tmp, _HASH_CACHE_FILE)
    except Exception:
        logger.exception("hash cache persist failed (non-fatal, next walk will retry)")


def _maybe_persist_throttled(min_interval_s: float = 600.0) -> None:
    """watcher flush 出口节流持久化 (10min 至多一次; walk 的 project 粒度落盘不受此限)。"""
    global _last_flush_persist_ts
    now = time.time()
    if now - _last_flush_persist_ts < min_interval_s:
        return
    _last_flush_persist_ts = now
    _persist_hashes()


def verify_hash_cache_against_db(projects) -> int:
    """CC GateA C2: 缓存-DB 绑定兜底。对每个有缓存条目的 project 查其 collection 的
    chunk 总数, =0 视为"DB 被重建/清空而缓存还在" → 丢弃该 project 全部缓存条目走全量。
    返回丢弃的条目数。每集合一条 COUNT, 代价可忽略。"""
    if not _HASH_CACHE_ENABLED:
        return 0
    _ensure_hashes_loaded()  # R1197 §一④: 懒加载后 verify 可能先于任何 walk 被调
    if not _file_hashes:
        return 0
    try:
        with _hashes_iter_lock:  # flash 执行审 D1: 收集+删除整体持锁 (删除改 dict 大小, 与 persist 快照并发会 RuntimeError)
            by_proj: dict[str, list[str]] = {}
            for k in list(_file_hashes.keys()):
                proj = k.split(":", 1)[0]
                by_proj.setdefault(proj, []).append(k)
        dropped = 0
        for proj, keys in by_proj.items():
            if proj not in {p.name for p in projects}:
                continue  # 非当前配置的 project: 条目本就无害 (walk 只按前缀清理)
            try:
                n = count_collection_chunks(proj)
                if n == 0 and keys:
                    with _hashes_iter_lock:  # flash 复审 D1: 删除循环也必须持锁
                        for k in keys:
                            _file_hashes.pop(k, None)
                    dropped += len(keys)
                    logger.warning(
                        "hash cache vs DB drift: collection '%s' empty but %d cache entries "
                        "→ dropped (full rescan)", proj, len(keys),
                    )
            except Exception:
                logger.exception("verify_hash_cache_against_db failed for '%s' (skip)", proj)
        return dropped
    except Exception:
        logger.exception("verify_hash_cache_against_db top-level failed (skip)")
        return 0


# R1197 §一④ (源 r1155 §6-2): 导入期直接 _load_hashes() 时 logging.basicConfig
# 尚未配置 (server.py main 里才配, 晚于 import) → "hash cache loaded" INFO 被吞,
# 缓存加载只能靠间接证据确认。改懒加载: 首个读者进场才加载, 日志落在已配置期。
_hashes_loaded = False
_hashes_load_lock = threading.Lock()


def _ensure_hashes_loaded() -> None:
    """幂等懒加载 — index_project / index_file / verify 入口调用; 之后纯 flag 读零开销。
    竞态: 双检锁; watcher 早于首 walk 触发 index_file 也由此获得缓存 (不再导入期加载)。"""
    global _hashes_loaded
    if _hashes_loaded:
        return
    with _hashes_load_lock:
        if _hashes_loaded:
            return
        _load_hashes()
        _hashes_loaded = True


# 2026-06-11 审计A3: 切块统一走 chunking.py 单一实现 (与 cron 索引器同源),
# 终结双路径 chunk 边界不一致 → hash 对不齐 → 去重失效的真重复来源。
from chunking import chunk_text as _unified_chunk_text, CHUNKER_VERSION, prefix_hash as _prefix_hash, content_hash as _content_hash, pilot_blacklist_filter as _pilot_blacklist_filter, chunk_hash as _chunk_hash
import pilot_config  # MEMIDX 试点门控 (T-MEMIDX-PILOT-IMPL; fail-safe 默认全关, 仅作用 scope_project)


def _chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    return _unified_chunk_text(text, chunk_size, chunk_overlap)


def _is_excluded(path: str, patterns: list[str]) -> bool:
    # F-2 (T-VEC-ORPHAN-PURGE-0828 机制修): DEFAULT_EXCLUDE (SQLite 内部文件类型闸) 对
    # 所有项目强制生效 — 项目 yaml 的 exclude_patterns 是"追加"语义不是"整体替换"。
    # 8/25 事故形态: openclaw-sessions.yaml 自带 exclude_patterns → F-1 加在 DEFAULT_EXCLUDE
    # 的类型闸被 yaml 整体覆盖 → -wal/-shm/-journal 照进库 (1.97万条垃圾向量)。
    # yaml 列表无法豁免全局类型闸; 项目想索引某内部文件的需求不存在 (非文本文档)。
    name = Path(path).name
    for pat in patterns:
        if fnmatch(name, pat):
            return True
    for pat in DEFAULT_EXCLUDE:
        if fnmatch(name, pat):
            return True
    return False


def _parse_jsonl_session(text: str, min_text_len: int = 30, blacklist_cfg: dict | None = None) -> str:
    """Parse a JSONL session file and extract only meaningful messages.

    Keeps:
      - user messages (text content only, metadata/system headers stripped)
      - assistant text content (even when accompanied by toolCalls)
      - tool names used (for searchability), but not tool input/output
    Skips:
      - toolResult messages (raw API/tool output — too noisy)
      - Non-message lines (session metadata, model changes, etc.)
      - Text shorter than min_text_len characters
    """
    meaningful_parts = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            meaningful_parts.append(line)
            continue

        # 5/19 fix: 兼容 OpenClaw + Claude Code 两种 jsonl schema.
        # OpenClaw: 顶级 type='message', message={role,content}
        # Claude Code: 顶级 type='user'/'assistant', message={role,content[]}
        # (Claude Code content[] 元素含 text 字段, 直接走下面的 _extract_text_content)
        obj_type = obj.get("type", "")
        if obj_type not in ("message", "user", "assistant"):
            continue

        msg = obj.get("message", {})
        if not msg:
            continue
        # Claude Code: type 直接是 role; OpenClaw: role 在 message 里
        role = msg.get("role", "") or obj_type if obj_type in ("user", "assistant") else ""
        contents = msg.get("content", [])
        # Claude Code 有些 user message 的 content 是 string (不是 list), 兼容处理
        if isinstance(contents, str):
            contents = [{"type": "text", "text": contents}]
        ts = obj.get("timestamp", "")[:19]

        if role == "user":
            user_text = _extract_text_content(contents)
            user_text = _strip_user_metadata(user_text)
            if blacklist_cfg:  # MEMIDX change#2 门控
                user_text = _pilot_blacklist_filter(user_text, blacklist_cfg)
            if user_text.strip() and len(user_text.strip()) >= min_text_len:
                meaningful_parts.append(f"[{ts}] USER:\n{user_text.strip()}")

        elif role == "assistant":
            text_content = _extract_text_content(contents)
            if blacklist_cfg:  # MEMIDX change#2 门控
                text_content = _pilot_blacklist_filter(text_content, blacklist_cfg)
            tool_names = [
                c.get("name", "")
                for c in contents
                if isinstance(c, dict) and c.get("type") == "toolCall" and c.get("name")
            ]
            combined = text_content.strip()
            if tool_names and combined:
                combined += f"\n[tools: {', '.join(tool_names)}]"
            elif tool_names and not combined:
                combined = f"[called: {', '.join(tool_names)}]"
            if combined and len(combined) >= min_text_len:
                meaningful_parts.append(f"[{ts}] ASSISTANT:\n{combined}")

    if not meaningful_parts:
        return ""
    return "\n\n".join(meaningful_parts)


def _extract_text_content(contents) -> str:
    """Extract readable text from message content array."""
    parts = []
    for c in contents:
        if isinstance(c, str):
            parts.append(c)
        elif isinstance(c, dict):
            if c.get("type") == "text":
                parts.append(c.get("text", ""))
    return " ".join(parts)


_OPENCLAW_META_PATTERNS = [
    "Note: The previous agent run was aborted",
    "Sender (untrusted metadata):",
    "[Subagent Context]",
    "[cron:",
    "You are running as a subagent",
]


def _strip_user_metadata(text: str) -> str:
    """Strip OpenClaw system metadata from user messages."""
    lines = text.split("\n")
    cleaned = []
    skip = False

    for line in lines:
        s = line.strip()

        if skip:
            if s == "```":
                skip = False
            continue

        if s.startswith("Note: The previous agent run was aborted"):
            continue
        if s.startswith("Sender (untrusted metadata):"):
            skip = True
            continue
        if s == "```json" and len(cleaned) < 5:
            skip = True
            continue
        if "<<<BEGIN_OPENCLAW_INTERNAL_CONTEXT>>>" in s:
            skip = True
            continue
        if "<<<END_OPENCLAW_INTERNAL_CONTEXT>>>" in s:
            skip = False
            continue

        cleaned.append(line)

    return "\n".join(cleaned)


# MEMIDX change#2 黑名单过滤已收敛到 chunking.pilot_blacklist_filter (单一真相源, 与 cron 路径共用),
# 上方 import 为 _pilot_blacklist_filter。


def _get_existing_chunks(
    collection_name: str, file_path: str
) -> dict[int, dict]:
    try:
        return get_existing_chunks(collection_name, file_path)
    except Exception as e:
        logger.warning(f"Failed to get existing chunks for {file_path}: {e}")
        return {}


# MEMIDX 埋点 (T-MEMIDX-PILOT-IMPL 铁律): 新 chunk 数 / embed 时长 按 project×version 落 log,
# 供 3-5 天评估窗对比 pilot(v2-pilot) vs 非 pilot。ungated(全 project 记, 才能对比); fail-safe 绝不阻断索引。
# R1 (T-MEMSVC-IO-FIX-0930): 路径可用 env MEMIDX_METRIC_LOG 改道 — 测试进程
# (如 test_orphan_source_block_0828) 必须指到沙箱, 禁止往生产指标台账写测试行。
_MEMIDX_METRIC_LOG = os.environ.get("MEMIDX_METRIC_LOG", "")


def _pilot_metric(project_name, version, n_embedded, embed_seconds, n_chunks, n_skipped):
    if n_embedded <= 0 or not _MEMIDX_METRIC_LOG:
        return
    try:
        with open(_MEMIDX_METRIC_LOG, "a") as f:
            f.write(json.dumps({
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"), "project": project_name,
                "chunker_version": str(version), "n_embedded": n_embedded,
                "embed_seconds": round(embed_seconds, 3), "n_chunks_file": n_chunks,
                "n_skipped_dedup": n_skipped,
            }, ensure_ascii=False) + "\n")
    except Exception:
        pass  # 埋点失败绝不影响索引


# P0-4 加固 (2026-08-06 系统审计, GLM-5.2 审修订): 有界索引并发线程池.
# 8/4 16:15 FD 爆炸 (1464 次 Too many open files) 根因=无界 thread-per-file.
# ThreadPoolExecutor: 限并发 + submit 自动排队背压 + 池自管生命周期.
# 并发数默认 8 (与 server max_concurrency 对齐; 调 MEMIDX_MAX_CONCURRENT 覆盖,
# 与 MEMSVC_MAX_CONCURRENCY 同步调整 — 双 env 漂移注意, L 级).
_INDEX_POOL = None
if int(os.environ.get("MEMIDX_MAX_CONCURRENT", "8")) > 0:
    _INDEX_POOL = ThreadPoolExecutor(
        max_workers=int(os.environ.get("MEMIDX_MAX_CONCURRENT", "8")),
        thread_name_prefix="idx",
    )


def _index_file_bounded(file_path: str, project: ProjectConfig) -> None:
    """线程池任务包装: 异常不外泄 (池内吞掉, 与旧 thread 行为一致)."""
    try:
        index_file(file_path, project)
    except Exception:
        logging.getLogger("memory-service.indexer").exception(
            "index_file 异常 (池内捕获): %s", file_path)


def index_file(file_path: str, project: ProjectConfig) -> int:
    """Index a single file: chunk, embed, and upsert into SQLite (vec0/numpy via db.py).

    Uses incremental embedding: compares chunk content hashes against existing
    DB entries and only re-embeds chunks that changed. Unchanged chunks reuse
    their existing embeddings.

    Skips files that are excluded, unchanged (mtime cache), empty, or unreadable.

    Args:
        file_path: Absolute path to the file.
        project: Project configuration (chunk size, overlap, excludes).

    Returns:
        Number of chunks indexed (0 if skipped).
    """
    _ensure_hashes_loaded()  # R1197 §一④: watcher 早于首 walk 时也能命中缓存
    p = Path(file_path)
    if not p.exists() or _is_excluded(file_path, project.exclude_patterns):
        return 0

    current_hash = _file_hash(file_path)
    cache_key = f"{project.name}:{file_path}"

    if _file_hashes.get(cache_key) == current_hash:
        return 0

    # B5: jsonl 大文件走增量读, 避免整读 GB 级文件 (治 857MB alloc 泄漏)
    try:
        file_size = p.stat().st_size
    except Exception:
        file_size = 0
    if (
        file_path.endswith(".jsonl")
        and file_size > _INCREMENTAL_THRESHOLD
    ):
        return _index_jsonl_incremental(
            file_path, project, p, current_hash, cache_key, file_size
        )

    # B5 ② 大文件闸兜底: 非 jsonl 巨文件 (>50MB) 跳过整读, 防①有漏时复发泄漏
    if file_size > _HARD_FILE_LIMIT:
        logger.warning(
            "B5 hard limit skip %s: size=%d > %d (non-jsonl or under incremental threshold) — not indexed",
            file_path, file_size, _HARD_FILE_LIMIT,
        )
        _file_hashes[cache_key] = current_hash   # 防 retrigger
        return 0

    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        logger.warning(f"Cannot read {file_path}: {e}")
        return 0

    if not text.strip():
        return 0

    # F-3 (reviewer 20260828/maintainer-approved, 防递归白名单哲学): 二进制内容嗅探 —
    # 黑名单漏网时最后一道闸: NUL 字节或前2KB不可打印率>30% = 非文本文档,
    # 拒绝嵌入并高声告警(非静默) — "没有规则说可以就拒", 与黑名单/体积闸三层独立.
    _head = text[:2048]
    _nul = _head.count("\x00")
    _printable = sum(1 for c in _head if c.isprintable() or c in "\n\r\t")
    if _nul > 0 or (_printable / max(len(_head), 1)) < 0.7:
        logger.warning(
            "F-3 binary sniff reject %s: nul=%d printable_rate=%.2f — 非文本内容拒绝嵌入(防SQLite/二进制入库)",
            file_path, _nul, _printable / max(len(_head), 1),
        )
        _file_hashes[cache_key] = current_hash  # 防 retrigger
        return 0

    # MEMIDX 试点门控 (T-MEMIDX-PILOT-IMPL; None=不作用→逐字旧行为)。在 parse 前算, 供 change#2 黑名单。
    _pilot = pilot_config.pilot_for(project.name)
    _bl = (_pilot.get("blacklist") if _pilot else None)
    # isinstance 守: 防坏配置(blacklist 写成非 dict)时 .get() 崩索引 (GLM 互审 a: 类型 fail-safe)
    _bl = _bl if (isinstance(_bl, dict) and _bl.get("enabled")) else None

    # Pre-process JSONL session files: extract only meaningful messages
    if file_path.endswith(".jsonl"):
        text = _parse_jsonl_session(text, blacklist_cfg=_bl)  # change#2 门控(_bl=None→旧行为)
        if not text.strip():
            logger.debug(f"Skipped {file_path}: no meaningful content after JSONL parsing")
            return 0

    chunks = _chunk_text(text, project.chunk_size, project.chunk_overlap)
    if not chunks:
        return 0

    collection = get_or_create_collection(project.name)

    existing = _get_existing_chunks(collection, file_path)

    # B1: 加载集合 prefix_hash 集合 (懒加载, 首次调用时从 DB 全量读取)
    _ensure_prefix_hashes(collection)
    col_prefix_hashes = _collection_prefix_hashes[collection]

    # MEMIDX 试点 change#1 门控 (_pilot 已在 parse 前算)
    _ver = _pilot["chunker_version_tag"] if _pilot else CHUNKER_VERSION  # 新块版本标(旧块不改)
    _use_full_hash = bool(_pilot and (_pilot.get("dedup") or {}).get("full_content_hash"))

    # Compute file mtime once
    file_mtime = 0
    try:
        file_mtime = p.stat().st_mtime
    except Exception:
        pass

    # Classify chunks into: keep (unchanged), embed (new/changed), delete (removed)
    to_delete_ids = []
    to_upsert = []
    to_embed_texts = []
    to_embed_indices = []
    new_hash_map = {}
    skipped_dedup = 0

    for i, chunk in enumerate(chunks):
        chunk_hash = _chunk_hash(chunk)
        new_hash_map[i] = chunk_hash
        chunk_id = hashlib.md5(f"{file_path}#{i}".encode()).hexdigest()[:16]

        if i in existing and existing[i]["hash"] == chunk_hash:
            # Chunk unchanged — reuse existing embedding
            old = existing[i]
            emb = old["embedding"] if old["embedding"] is not None else []
            to_upsert.append(
                {
                    "id": old["id"],
                    "text": chunk,
                    "embedding": emb,
                    "metadata": {
                        "source": file_path,
                        "project": project.name,
                        "chunk_index": i,
                        "total_chunks": len(chunks),
                        "chunk_hash": chunk_hash,
                        "chunker_version": CHUNKER_VERSION,
                        "file_name": p.name,
                        "file_mtime": file_mtime,
                        "relative_path": str(p.relative_to(project.root))
                        if file_path.startswith(project.root)
                        else file_path,
                        "layer": classify_layer(file_path),
                    },
                }
            )
        else:
            # B1: 跨文件内容去重 — 若同 (chunker_version, hash) 已入库, 跳过
            # 去重键加版本盐: v1/v2 切块边界差 ±1 字符, 纯 prefix_hash 会误拦合法 v2 重切块
            # MEMIDX change#1(试点门控): full_content_hash → 用全内容规范化 hash(抓 >200 长模板),
            # 否则逐字旧路径 prefix_hash(前200字符); 版本用 _ver(试点=v2-pilot, 独立去重命名空间)
            ph = _content_hash(chunk) if _use_full_hash else _prefix_hash(chunk)
            dedup_key = (str(_ver), ph)
            if dedup_key in col_prefix_hashes:
                skipped_dedup += 1
                continue
            col_prefix_hashes.add(dedup_key)
            # Chunk new or changed — need to embed
            to_embed_texts.append(chunk)
            to_embed_indices.append(i)

    # Find chunks that were removed (exist in DB but not in new chunks)
    for idx, old in existing.items():
        if idx not in new_hash_map:
            to_delete_ids.append(old["id"])

    # Embed only the changed/new chunks
    new_embeddings = []
    _embed_seconds = 0.0
    if to_embed_texts:
        _t0 = time.time()
        try:
            new_embeddings = embed_batch(to_embed_texts)
        except Exception:
            new_embeddings = []
        _embed_seconds = time.time() - _t0

    # Build upsert entries for newly embedded chunks
    embed_success = 0
    embed_failed = 0
    for i, (idx, chunk, emb) in enumerate(
        zip(to_embed_indices, to_embed_texts, new_embeddings)
    ):
        if not emb:
            embed_failed += 1
            continue
        embed_success += 1
        chunk_id = hashlib.md5(f"{file_path}#{idx}".encode()).hexdigest()[:16]
        to_upsert.append(
            {
                "id": chunk_id,
                "text": chunk,
                "embedding": emb,
                "metadata": {
                    "source": file_path,
                    "project": project.name,
                    "chunk_index": idx,
                    "total_chunks": len(chunks),
                    "chunk_hash": new_hash_map[idx],
                    "chunker_version": _ver,  # MEMIDX: 新块打 _ver(试点=v2-pilot); 旧块 keep 分支不改(旧块零触碰)
                    "file_name": p.name,
                    "file_mtime": file_mtime,
                    "relative_path": str(p.relative_to(project.root))
                    if file_path.startswith(project.root)
                    else file_path,
                    "layer": classify_layer(file_path),
                },
            }
        )

    # MEMIDX 埋点: 本次 embed 的 新chunk数/耗时 按 project×version 落 log (fail-safe)
    _pilot_metric(project.name, _ver, embed_success, _embed_seconds, len(chunks), skipped_dedup)

    # 2026-05-11 守卫: 纯"假写入"场景跳过 upsert + invalidate_cache.
    # 真实场景: 外部进程 touch .export_meta.json 改 mtime, 但内容没变
    # → _file_hash (基于 mtime) 失效进入这里, 但所有 chunk_hash 都匹配
    # existing → embed_success=0 且 to_delete_ids=[]. 此时 to_upsert 里
    # 全是 "kept" 项, embedding bytes 跟 DB 里一字不差, 调 upsert 只会
    # 触发不必要的 _invalidate_cache → 下次 search 全 reload → mmap
    # 大块累积 (实测 500 MB/h, 4h 涨到 3.5 GB).
    if to_upsert and embed_success == 0 and not to_delete_ids:
        # 没有任何实质变化 — 只更新 mtime cache 防止重复触发, 然后跳过
        _file_hashes[cache_key] = current_hash
        logger.debug(
            f"Skipped no-op reindex for {file_path}: "
            f"kept={len(to_upsert)}, no embed/delete needed"
        )
        return 0

    # Delete removed chunks
    if to_delete_ids:
        try:
            delete_chunks_by_ids(to_delete_ids, collection)
        except Exception as e:
            logger.warning(f"Failed to delete removed chunks: {e}")

    # Upsert kept + new chunks
    if to_upsert:
        upsert_chunks(collection, to_upsert)
        # Yield GIL after each DB write so HTTP handler can process requests.
        # Without this, bulk SQLite writes hold an exclusive lock
        # that starves the HTTP thread during bulk indexing.
        time.sleep(0.2)
        kept = len(to_upsert) - embed_success
        # Only update mtime cache if all embeds succeeded — otherwise
        # failed chunks would be silently skipped on next trigger.
        if embed_failed == 0:
            _file_hashes[cache_key] = current_hash
        else:
            logger.warning(
                f"Not updating mtime cache for {file_path}: "
                f"{embed_failed} chunk(s) failed to embed"
            )
        logger.info(
            f"Indexed {file_path}: {len(to_upsert)} chunks "
            f"(kept={kept}, embedded={embed_success}, failed={embed_failed}, "
            f"deleted={len(to_delete_ids)}, dedup_skipped={skipped_dedup})"
        )
        with _index_metrics_lock:
            _index_metrics["last_index_time"] = time.time()
            _index_metrics["files_indexed"] += 1
            _index_metrics["chunks_embedded"] += embed_success
        return len(to_upsert)

    return 0


def _index_jsonl_incremental(
    file_path: str,
    project: ProjectConfig,
    p: Path,
    current_hash: str,
    cache_key: str,
    file_size: int,
) -> int:
    """B5: jsonl 大文件增量读 — 只读 last_offset 之后的新增 tail, 不整读巨文件。

    jsonl 是 append-only, 天然支持 seek 增量。offset 持久化到服务目录 (铁律: 不放 /tmp)。
    - truncate/轮转兜底 (size<offset): 删旧 chunks + 重置 offset=0
    - 首次迁移 (offset==0 且库内已有该文件 chunks): 历史已索引, offset 直接跳到 file_size
    - 单次 tail 读取上限 _MAX_TAIL_BYTES: 超 limit 截到最后一行边界, 剩余下次再处理
    - 行边界对齐: seek(last_offset) 后跳到下一个 \\n, 丢弃跨 offset 边界的残行
    - chunk_id 基于内容 md5(file_path#chunk_hash): 同内容同 id (upsert 幂等)
    - 去重: prefix_hash 内存层 + db 层 text_sha256 (双重保障)
    """
    _load_offsets()
    collection = get_or_create_collection(project.name)
    last_offset = _last_offsets.get(file_path, 0)

    # truncate/轮转兜底: size < offset → 删旧 chunks (轮转后内容不兼容) + reset offset=0
    if file_size < last_offset:
        logger.warning(
            "B5 reset (truncate/rotate): %s size=%d < offset=%d — deleting old chunks",
            file_path, file_size, last_offset,
        )
        old = _get_existing_chunks(collection, file_path)
        if old:
            try:
                delete_chunks_by_ids([c["id"] for c in old.values()], collection)
            except Exception as e:
                logger.warning("B5 reset delete old chunks failed: %s", e)
        last_offset = 0
        with _offset_lock:
            _last_offsets[file_path] = 0

    # 首次迁移: offset==0 且库内已有该文件 chunks → 历史已索引过, 直接 offset=file_size
    # 只处理未来 append。避免首次读 16MB tail 浪费 (历史整读路径已索引)。
    if last_offset == 0:
        existing = _get_existing_chunks(collection, file_path)
        if existing:
            logger.info(
                "B5 migrate %s: %d chunks already in DB, offset→%d (skip history)",
                file_path, len(existing), file_size,
            )
            with _offset_lock:
                _last_offsets[file_path] = file_size
            _save_offsets()
            _file_hashes[cache_key] = current_hash
            return 0

    # mtime 变但内容没变 (touch / 重写同内容) — 推进 cache 防 retrigger
    if file_size == last_offset:
        _file_hashes[cache_key] = current_hash
        return 0

    # ── 读 tail: 从 last_offset 起 (offset 语义=行首, 即上一行 \n 之后) ──
    # 不跳残行: offset 总记录在 \n 之后 (next_offset = last_nl+1 或 file_size 若以\n结尾),
    # 所以 seek(offset) 直接读到完整新行。若 offset 万一落在行中间 (异常), 残行会被
    # _parse_jsonl_session 的 json.loads 当文本保留 (不丢数据), 比跳过更安全。
    start_offset = last_offset
    try:
        with open(file_path, "rb") as f:
            f.seek(start_offset)
            raw = f.read(_MAX_TAIL_BYTES + 1)

        # 截到 _MAX_TAIL_BYTES 内的最后一行边界 (超限分批下次处理)
        if len(raw) > _MAX_TAIL_BYTES:
            last_nl = raw.rfind(b"\n", 0, _MAX_TAIL_BYTES)
            if last_nl == -1:
                # 单行 > 16MB (异常): 截断 + 推进, 下次继续 (防爆)
                tail_bytes = raw[:_MAX_TAIL_BYTES]
                next_offset = start_offset + _MAX_TAIL_BYTES
                logger.warning(
                    "B5 single-line > %d bytes at offset=%d in %s — truncating",
                    _MAX_TAIL_BYTES, start_offset, file_path,
                )
            else:
                tail_bytes = raw[: last_nl + 1]
                next_offset = start_offset + last_nl + 1
        else:
            tail_bytes = raw
            next_offset = file_size
    except Exception as e:
        logger.warning("B5 incremental read failed %s: %s", file_path, e)
        return 0

    if not tail_bytes.strip():
        with _offset_lock:
            _last_offsets[file_path] = next_offset
        _save_offsets()
        _file_hashes[cache_key] = current_hash
        return 0

    text = tail_bytes.decode("utf-8", errors="ignore")

    # MEMIDX 试点门控 (与原路径同源)
    _pilot = pilot_config.pilot_for(project.name)
    _bl = (_pilot.get("blacklist") if _pilot else None)
    _bl = _bl if (isinstance(_bl, dict) and _bl.get("enabled")) else None

    text = _parse_jsonl_session(text, blacklist_cfg=_bl)
    if not text.strip():
        with _offset_lock:
            _last_offsets[file_path] = next_offset
        _save_offsets()
        _file_hashes[cache_key] = current_hash
        return 0

    chunks = _chunk_text(text, project.chunk_size, project.chunk_overlap)
    if not chunks:
        with _offset_lock:
            _last_offsets[file_path] = next_offset
        _save_offsets()
        _file_hashes[cache_key] = current_hash
        return 0

    _ensure_prefix_hashes(collection)
    col_prefix_hashes = _collection_prefix_hashes[collection]
    _ver = _pilot["chunker_version_tag"] if _pilot else CHUNKER_VERSION
    _use_full_hash = bool(_pilot and (_pilot.get("dedup") or {}).get("full_content_hash"))

    file_mtime = 0
    try:
        file_mtime = p.stat().st_mtime
    except Exception:
        pass

    # 仅 embed 新内容 (不参与 existing 全局对比 — 增量不知道全局 i)
    # 去重靠: prefix_hash/content_hash 内存层 + db 层 text_sha256 (双重保障)
    to_embed_texts: list[str] = []
    to_embed_indices: list[int] = []
    skipped_dedup = 0
    for i, chunk in enumerate(chunks):
        ph = _content_hash(chunk) if _use_full_hash else _prefix_hash(chunk)
        dedup_key = (str(_ver), ph)
        if dedup_key in col_prefix_hashes:
            skipped_dedup += 1
            continue
        col_prefix_hashes.add(dedup_key)
        to_embed_texts.append(chunk)
        to_embed_indices.append(i)

    new_embeddings = []
    _embed_seconds = 0.0
    if to_embed_texts:
        _t0 = time.time()
        try:
            new_embeddings = embed_batch(to_embed_texts)
        except Exception:
            new_embeddings = []
        _embed_seconds = time.time() - _t0

    to_upsert = []
    embed_success = 0
    embed_failed = 0
    for idx, chunk, emb in zip(to_embed_indices, to_embed_texts, new_embeddings):
        if not emb:
            embed_failed += 1
            continue
        embed_success += 1
        ch = _chunk_hash(chunk)
        chunk_id = hashlib.md5(f"{file_path}#{ch}".encode()).hexdigest()[:16]
        to_upsert.append({
            "id": chunk_id,
            "text": chunk,
            "embedding": emb,
            "metadata": {
                "source": file_path,
                "project": project.name,
                "chunk_index": -1,   # B5: 增量读无全局 i, 用 -1 标记
                "total_chunks": -1,
                "chunk_hash": ch,
                "chunker_version": _ver,
                "file_name": p.name,
                "file_mtime": file_mtime,
                "relative_path": str(p.relative_to(project.root))
                if file_path.startswith(project.root)
                else file_path,
                "layer": classify_layer(file_path),
                "incremental": True,   # B5 标记
            },
        })

    _pilot_metric(
        project.name, _ver, embed_success, _embed_seconds, len(chunks), skipped_dedup
    )

    if to_upsert:
        upsert_chunks(collection, to_upsert)
        # 与原路径一致: 让出 GIL 防 SQLite 写锁饿死 HTTP handler
        time.sleep(0.2)

    # 推进 offset (单向: 即便 embed_failed 也推进, 防同 tail 无限重读;
    # failed chunks 接受丢失 — 与原路径不同, 因为增量是单向流)
    with _offset_lock:
        _last_offsets[file_path] = next_offset
    _save_offsets()
    _file_hashes[cache_key] = current_hash

    logger.info(
        "B5 incremental %s: tail=%d bytes (offset %d→%d / size=%d), "
        "chunks=%d embedded=%d failed=%d dedup_skipped=%d",
        file_path, next_offset - start_offset, start_offset, next_offset, file_size,
        len(chunks), embed_success, embed_failed, skipped_dedup,
    )
    with _index_metrics_lock:
        _index_metrics["last_index_time"] = time.time()
        _index_metrics["files_indexed"] += 1
        _index_metrics["chunks_embedded"] += embed_success
    return len(to_upsert) if to_upsert else 0


# Number of files to process before yielding the GIL during bulk indexing.
# This prevents bulk SQLite writes from starving the HTTP handler.
_INDEX_YIELD_INTERVAL = 5

# R1197 §一① (源 r1155 §4-1, GateB 03:42 实测): 旧版 `if files % 5 == 0: sleep(0.2)`
# 按**扫描文件数**睡, hash 缓存命中(纯 skip 零 DB 工作)也睡 → 40ms/文件硬地板
# (37951 文件/walk × 5 walk/日 ≈ 2.1h/日纯睡眠, 占 1h11m walk 的 36%)。
# 修: 睡眠挂到**真实入库工作**(indexed_since_yield, 仅 count>0 即 chunk 真入库时累加),
# 累计达阈值才 sleep(0.2); 纯 skip 批次只做 sleep(0) 细粒度让步(OS 级 yield, 有等待
# 线程才让出, μs 级) — skip 快路径本身是 stat 系调用(释放 GIL)+CPython switch interval
# (默认 5ms)兜底, 不睡长觉也不会饿死 HTTP handler。
# killswitch: MEMSVC_WALK_LEGACY_YIELD=1 恢复旧行为(全按扫描数睡)。
_WALK_LEGACY_YIELD = _env_num("MEMSVC_WALK_LEGACY_YIELD", 0) > 0


def index_project(project: ProjectConfig, force: bool = False, cleanup: bool = False) -> dict:
    """Index all files in a project's watch directories.

    Yield policy (R1197 §一①): long sleep(0.2) only after batches of files that
    were *actually indexed* (real DB writes), so bulk SQLite writes don't starve
    the HTTP handler; pure cache-hit skip batches only do sleep(0) fine-grained
    yields. Set MEMSVC_WALK_LEGACY_YIELD=1 to restore the old unconditional
    per-5-scanned-files sleep.

    Args:
        project: Project configuration.
        force: If True, reindex all files regardless of mtime cache.
        cleanup: T-MEMSVC-WALK-SPEEDUP (flash 条件3): 仅 walk 路径 (_init_project)
            显式传 True — 结束后删除该 project 前缀下未出现在本次扫描中的缓存条目
            (治"文件删了缓存还在→路径复用+mtime 碰撞错 skip"); 后台/API 路径默认
            False 不清理 (seen 语义不保证)。异常抛出时自然跳过清理 (只走成功路径)。

    Returns:
        Dict with ``indexed``, ``skipped``, and ``files`` counts.
    """
    _ensure_hashes_loaded()  # R1197 §一④: force 路径要迭代 _file_hashes, 须先加载
    if force:
        with _hashes_iter_lock:  # flash 执行审 D1: force 路径收集+删除同持锁
            keys_to_remove = [k for k in _file_hashes if k.startswith(f"{project.name}:")]
            for k in keys_to_remove:
                del _file_hashes[k]

    indexed = 0
    skipped = 0
    files = 0
    indexed_since_yield = 0  # R1197 §一①: 真实入库计数 (count>0), 睡眠只挂真实工作
    stale: list[str] = []        # cleanup 时填充
    seen_keys: set[str] = set()    # cache_key 格式 (project:path) — 清 _file_hashes 用
    seen_paths: set[str] = set()   # 裸 file_path — 清 _last_offsets 用 (其键为裸路径, 实证 718-796)

    watch_paths = [Path(project.root) / d for d in project.watch_dirs]
    for watch_file in project.watch_files:
        watch_paths.append(Path(watch_file))

    for watch_path in watch_paths:
        if watch_path.is_file():
            files += 1
            seen_keys.add(f"{project.name}:{watch_path}")
            seen_paths.add(str(watch_path))
            count = index_file(str(watch_path), project)
            if count > 0:
                indexed += 1
            else:
                skipped += 1
        elif watch_path.is_dir():
            for p in sorted(watch_path.rglob("*")):
                if not p.is_file():
                    continue
                if _is_excluded(str(p), project.exclude_patterns):
                    continue
                files += 1
                seen_keys.add(f"{project.name}:{p}")
                seen_paths.add(str(p))
                count = index_file(str(p), project)
                if count > 0:
                    indexed += 1
                    indexed_since_yield += 1  # R1197 §一①: 只有真实入库才计
                else:
                    skipped += 1
                # R1197 §一①: conditional yield — 详见 _WALK_LEGACY_YIELD 处注释
                # (flash 复审 D1: 两分支互斥 — legacy 必须逐字旧行为, 不得叠加工作计数睡)
                if _WALK_LEGACY_YIELD:
                    if files % _INDEX_YIELD_INTERVAL == 0:
                        time.sleep(0.2)
                else:
                    if files % _INDEX_YIELD_INTERVAL == 0:
                        time.sleep(0)  # 细粒度让步 (OS 级, 有等待线程才让, μs 级)
                    if indexed_since_yield >= _INDEX_YIELD_INTERVAL:
                        time.sleep(0.2)
                        indexed_since_yield = 0

    if cleanup:
        # flash D2: 删除清理 — 文件删除后不会产生 cache miss (不在 walk 结果里),
        # 残留条目遇"路径复用+mtime 相同"会错 skip → 按前缀清未出现者。
        prefix = f"{project.name}:"
        with _hashes_iter_lock:  # flash 执行审 D1: 批量删除持锁
            stale = [k for k in list(_file_hashes.keys()) if k.startswith(prefix) and k not in seen_keys]
            for k in stale:
                del _file_hashes[k]
        # CC GateA N3: 顺手清 _last_offsets 里已删文件的条目 (同病类)。
        # flash 执行审 D2 修: startswith 会命中嵌套 root (/proj 匹配 /proj2) →
        # 改 Path.relative_to 边界判定 (ValueError = 不在该 project 下, 跳过)。
        _root = Path(project.root)
        with _offset_lock:
            stale_offsets = []
            for fp in list(_last_offsets.keys()):
                if fp in seen_paths:
                    continue
                try:
                    Path(fp).relative_to(_root)
                except ValueError:
                    continue
                stale_offsets.append(fp)
            for fp in stale_offsets:
                del _last_offsets[fp]
        if stale_offsets:
            _save_offsets()  # 内存清理同步落盘, 防下次启动从旧 json 载回死条目

    _last_full_index[project.name] = time.time()
    # flash D3: 每完成一个 project 落盘一次 (首全量中途崩溃也留已完成 project 的缓存)。
    _persist_hashes()
    if cleanup and stale:
        logger.info("hash cache cleanup '%s': removed %d stale entries", project.name, len(stale))
    return {"indexed": indexed, "skipped": skipped, "files": files}


class _WatchHandler(FileSystemEventHandler):
    """Watchdog handler that indexes modified/created files.

    MEMIDX change#3 (T-MEMIDX-PILOT-IMPL): 试点门控下 debounce 微批 —— 会话文件逐条 append →
    watcher 每次触发 index_file(冗余重扫); 门控下窗口内同文件多事件合并成一次 index_file。
    debounce 状态 **per-handler(self)** 不用模块全局(GLM 互审 d: 防多 project 穿透)。
    总开关关 → 逐字旧行为(每事件即时起线程)。"""

    def __init__(self, project: ProjectConfig):
        self.project = project
        self._db_lock = threading.Lock()
        self._db_pending: dict = {}          # file_path -> ProjectConfig
        self._db_timer = None

    def _flush(self):
        with self._db_lock:
            pending = dict(self._db_pending)
            self._db_pending.clear()
            self._db_timer = None
        for fp, proj in pending.items():
            try:
                if _INDEX_POOL is not None:
                    _INDEX_POOL.submit(_index_file_bounded, fp, proj)
                else:
                    index_file(fp, proj)
            except Exception:
                logger.error("debounce flush index_file failed: %s", fp, exc_info=True)  # 互审 g: debug→error
        # T-MEMSVC-WALK-SPEEDUP: flush 出口节流持久化 (10min 至多一次; 纯内存退出=改动蒸发)。
        _maybe_persist_throttled()

    def _schedule(self, file_path: str, window_seconds: float):
        with self._db_lock:
            self._db_pending[file_path] = self.project   # 同文件多事件 → 覆盖(只留最后, 天然去重)
            if self._db_timer is None:                   # 固定窗: 首事件起 window 秒后 flush(不会饥饿)
                self._db_timer = threading.Timer(window_seconds, self._flush)
                self._db_timer.daemon = True
                self._db_timer.start()

    def _dispatch(self, file_path: str):
        """2026-08-15 maintainer decision: 默认固定窗批量 (WATCH_DEBOUNCE_SECONDS, 1h) — 不再实时 embed.

        试点 pilot debounce 开 → 用试点窗口; 否则用默认窗口 (0=回实时旧行为).
        isinstance 守坏配置. P0-4 有界并发保留在 flush 侧.
        """
        _pilot = pilot_config.pilot_for(self.project.name)
        _db = (_pilot.get("debounce") if _pilot else None)
        if isinstance(_db, dict) and _db.get("enabled"):
            self._schedule(file_path, float(_db.get("window_seconds", 600)))
        elif WATCH_DEBOUNCE_SECONDS > 0:
            self._schedule(file_path, WATCH_DEBOUNCE_SECONDS)
        else:
            # P0-4 加固 (2026-08-06 系统审计, GLM-5.2 审修订): 有界并发索引.
            # 8/4 16:15 FD 爆炸 (1464 次 Too many open files) 根因=无界 thread-per-file.
            # 用 ThreadPoolExecutor(max_workers=8): ①限并发防 FD 爆炸 ②submit 排队
            # 自动背压, 不阻塞 watchdog 分发线程 (信号量 acquire 会阻塞分发→FSEvents
            # 队列满丢事件, H 级缺陷已弃用该方案) ③线程池自管生命周期, 无跨作用域泄漏.
            # 回滚: _INDEX_POOL = None 走旧无界路径.
            if _INDEX_POOL is not None:
                _INDEX_POOL.submit(_index_file_bounded, file_path, self.project)
            else:
                threading.Thread(target=index_file, args=(file_path, self.project), daemon=True).start()

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent) and event.src_path:
            self._dispatch(event.src_path)

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent) and event.src_path:
            self._dispatch(event.src_path)


def start_watching(project: ProjectConfig):
    """Start a file watcher for the project's watch directories.

    Returns:
        A watchdog Observer instance (call ``observer.stop()`` to stop).
    """
    observer = Observer()
    handler = _WatchHandler(project)

    for d in project.watch_dirs:
        watch_path = Path(project.root) / d
        if watch_path.is_dir():
            observer.schedule(handler, str(watch_path), recursive=True)

    observer.start()
    logger.info(f"Watching project '{project.name}': {project.watch_dirs}")
    return observer


def register_project(project: ProjectConfig) -> None:
    """Register a project for indexing (add to internal tracking)."""
    _initialized_projects.add(project.name)
    logger.info(f"Registered project '{project.name}'")


_initialized_projects: set[str] = set()


def get_index_metrics() -> dict:
    """Return a copy of the index activity metrics (thread-safe)."""
    with _index_metrics_lock:
        return dict(_index_metrics)
