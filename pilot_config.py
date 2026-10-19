"""pilot_config.py — MEMIDX 记忆索引试点配置 (T-MEMIDX-PILOT-IMPL, 2026-08-01 engineer)。

shared by all agents Layer-2 只动**索引器入库路径**(检索路径零触碰)。总开关 enabled=false 一键回滚;
仅作用 scope_project(claude-code-sessions), 其余 project 与旧行为逐字不变。
读同目录 pilot.yaml, 进程内缓存(indexer 是长驻 watcher; 改配置后需重启进程或调 load(reload=True))。

fail-safe: 配置文件缺失/损坏/解析异常 → 一律当作全关(_DISABLED), 绝不因试点配置问题影响主索引。
"""
from __future__ import annotations

from pathlib import Path

_CFG_PATH = Path(__file__).parent / "pilot.yaml"
_DISABLED = {"enabled": False}
_cache: dict | None = None
_cache_mtime: float | None = None


def load(reload: bool = False) -> dict:
    """读 pilot.yaml。**按 mtime 热加载** —— 长驻 watcher 也能秒感知 enabled=false, 保证"一键回滚"
    不需重启进程(review note: pure cache would force a process kill to roll back)。任何异常 → _DISABLED(全关, fail-safe)。"""
    global _cache, _cache_mtime
    try:
        mtime = _CFG_PATH.stat().st_mtime
    except Exception:
        mtime = None
    if _cache is not None and not reload and mtime == _cache_mtime:
        return _cache   # 文件未变 → 用缓存(stat 开销极小)
    try:
        import yaml
        data = yaml.safe_load(_CFG_PATH.read_text(encoding="utf-8"))
        _cache = data if isinstance(data, dict) else dict(_DISABLED)
    except Exception:
        _cache = dict(_DISABLED)
    _cache_mtime = mtime
    return _cache


def pilot_for(project_name: str) -> dict | None:
    """该 project 生效的试点配置 dict; None = 试点不作用于此 project(走旧路径)。
    条件: 总开关 enabled=true ∧ project_name == scope_project。"""
    c = load()
    if not c.get("enabled"):
        return None
    if project_name != c.get("scope_project"):
        return None
    return c
