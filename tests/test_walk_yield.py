#!/usr/bin/env python3
"""R1197 单测 — walk conditional yield (§一①) + hash cache 懒加载日志 (§一④)

覆盖:
  1. 全 skip 批次零长睡 (40ms/文件地板消灭) — 真 index_file cache-hit 路径, 零 DB
  2. 真实入库批次按累计工作数睡 (indexed_since_yield 语义)
  3. killswitch MEMSVC_WALK_LEGACY_YIELD=1 恢复旧行为
  4. 懒加载: import 期不加载不吐日志; 首次 walk 加载且 "hash cache loaded" 恰好出现一次 (子进程)
  5. watcher 早于首 walk 的 index_file 也能命中缓存 (ensure 在 index_file 入口)
跑法: cd <repo> && venv/bin/python -m unittest test_walk_yield -v
"""
import io
import logging
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _fresh_indexer(tmpdir: str, env_overrides: dict | None = None):
    """隔离实例: 重载 indexer, 缓存/offset 文件指向 tmpdir (同 test_hash_cache_persistence 模式)。"""
    import importlib
    for k, v in (env_overrides or {}).items():
        os.environ[k] = v
    import indexer
    importlib.reload(indexer)
    indexer._HASH_CACHE_FILE = Path(tmpdir) / "file_hash_cache.json"
    indexer._OFFSET_STATE_FILE = Path(tmpdir) / "jsonl_offsets.json"
    indexer._file_hashes.clear()
    return indexer


class _SleepRec:
    def __init__(self):
        self.calls = []

    def __call__(self, s):
        self.calls.append(s)

    def long_sleeps(self):
        return [c for c in self.calls if c >= 0.1]


def _mk_project(n_files=30):
    from config import ProjectConfig
    base = Path(tempfile.mkdtemp())
    wd = base / "wd"
    wd.mkdir()
    for i in range(n_files):
        (wd / f"f{i:03d}.md").write_text(f"doc {i} " + "x" * (30 + i))
    return ProjectConfig(name="yield_t", root=str(base), watch_dirs=["wd"]), wd


class TestConditionalYield(unittest.TestCase):
    def test_all_skip_no_long_sleep(self):
        """全 cache-hit skip: 零次 sleep(>=0.1), 只有 sleep(0) 细粒度让步。"""
        idx = _fresh_indexer(tempfile.mkdtemp())
        proj, wd = _mk_project(30)
        for f in wd.iterdir():  # 预热缓存: 全部命中
            idx._file_hashes[f"yield_t:{f}"] = idx._file_hash(str(f))
        rec = _SleepRec()
        with mock.patch.object(idx.time, "sleep", rec):
            res = idx.index_project(proj)
        self.assertEqual(res, {"indexed": 0, "skipped": 30, "files": 30})
        self.assertEqual(rec.long_sleeps(), [], f"全 skip 不应有长睡: {rec.calls}")
        self.assertEqual(len([c for c in rec.calls if c == 0]), 6)  # 30/5 次 sleep(0)

    def test_work_batches_sleep_by_work_count(self):
        """12 个'真实入库'文件 → 长睡 2 次 (5,10); skip 不贡献长睡。"""
        idx = _fresh_indexer(tempfile.mkdtemp())
        proj, wd = _mk_project(12)
        idx.index_file = lambda fp, p: 1  # stub: 全部假装真实入库
        idx._persist_hashes = lambda: None
        rec = _SleepRec()
        with mock.patch.object(idx.time, "sleep", rec):
            res = idx.index_project(proj)
        self.assertEqual(res["indexed"], 12)
        self.assertEqual(len(rec.long_sleeps()), 2, f"应 5/10 各一觉: {rec.calls}")

    def test_mixed_skip_and_work(self):
        """12 文件: 前 6 命中缓存 skip, 后 6 stub 真入库 → 长睡仍只 1 次 (工作数=6)。"""
        idx = _fresh_indexer(tempfile.mkdtemp())
        proj, wd = _mk_project(12)
        files = sorted(wd.iterdir())
        for f in files[:6]:
            idx._file_hashes[f"yield_t:{f}"] = idx._file_hash(str(f))
        # 前 6 个命中缓存 → 0 (skip); 后 6 个 stub 真入库 → 1
        cache_keys = {f"yield_t:{f}" for f in files[:6]}

        def stub(fp, p):
            if f"{p.name}:{fp}" in cache_keys:
                return 0
            return 1

        idx.index_file = stub
        idx._persist_hashes = lambda: None
        rec = _SleepRec()
        with mock.patch.object(idx.time, "sleep", rec):
            res = idx.index_project(proj)
        self.assertEqual((res["indexed"], res["skipped"]), (6, 6))
        self.assertEqual(len(rec.long_sleeps()), 1, f"6 个工作 → 1 觉: {rec.calls}")

    def test_legacy_killswitch_restores_old_behavior(self):
        """MEMSVC_WALK_LEGACY_YIELD=1 → 全 skip 也按扫描数睡 (30/5=6 次长觉)。"""
        idx = _fresh_indexer(tempfile.mkdtemp(), {"MEMSVC_WALK_LEGACY_YIELD": "1"})
        try:
            self.assertTrue(idx._WALK_LEGACY_YIELD)
            proj, wd = _mk_project(30)
            for f in wd.iterdir():
                idx._file_hashes[f"yield_t:{f}"] = idx._file_hash(str(f))
            rec = _SleepRec()
            with mock.patch.object(idx.time, "sleep", rec):
                idx.index_project(proj)
            self.assertEqual(len(rec.long_sleeps()), 6, f"旧行为 6 觉: {rec.calls}")
        finally:
            os.environ.pop("MEMSVC_WALK_LEGACY_YIELD", None)

    def test_legacy_with_work_verbatim(self):
        """flash 复审 D1 回归: legacy + 12 真实入库 → 长觉恰 12//5=2, 不得叠加工作计数睡。
        (缺陷形态: 两分支不互斥时 legacy 会睡 2+2=4 觉)"""
        idx = _fresh_indexer(tempfile.mkdtemp(), {"MEMSVC_WALK_LEGACY_YIELD": "1"})
        try:
            proj, wd = _mk_project(12)
            idx.index_file = lambda fp, p: 1
            idx._persist_hashes = lambda: None
            rec = _SleepRec()
            with mock.patch.object(idx.time, "sleep", rec):
                res = idx.index_project(proj)
            self.assertEqual(res["indexed"], 12)
            self.assertEqual(len(rec.long_sleeps()), 2,
                             f"legacy 必须逐字旧行为(仅按扫描数 2 觉), 实际 {rec.calls}")
            self.assertEqual(len([c for c in rec.calls if c == 0]), 0,
                             f"legacy 不应有 sleep(0): {rec.calls}")
        finally:
            os.environ.pop("MEMSVC_WALK_LEGACY_YIELD", None)

    def test_verify_entry_loads_cache(self):
        """flash 复审缺口: verify 入口 ensure 生效 — 未 walk 先 verify 也能看到盘上缓存。"""
        idx = _fresh_indexer(tempfile.mkdtemp())
        import json
        entries = {f"vt:{tmp_path}": "mtime_ns:1:size=1" for tmp_path in ["a", "b", "c"]}
        idx._HASH_CACHE_FILE.write_text(json.dumps({"version": 1, "entries": entries}))
        self.assertFalse(idx._hashes_loaded)
        idx.count_collection_chunks = lambda name: 5  # stub: DB COUNT 非本测対象
        dropped = idx.verify_hash_cache_against_db([])
        self.assertEqual(dropped, 0)
        self.assertEqual(len(idx._file_hashes), 3, "verify 入口应已加载盘上缓存")
        self.assertTrue(idx._hashes_loaded)

    def test_persist_always_after_load(self):
        """flash 复审缺口: persist-先于-load 负向性质 — 任何 persist 调用时缓存必已加载。"""
        idx = _fresh_indexer(tempfile.mkdtemp())
        loaded_at_persist = []
        orig_persist = idx._persist_hashes

        def spy_persist():
            loaded_at_persist.append(idx._hashes_loaded)
            orig_persist()

        idx._persist_hashes = spy_persist
        proj, wd = _mk_project(6)
        idx.index_file = lambda fp, p: 1
        idx.index_project(proj)          # walk 路径: 末尾 persist
        idx._maybe_persist_throttled()   # flush 路径 (生产里后置于 index_file)
        self.assertTrue(loaded_at_persist, "应有 persist 调用被记录")
        self.assertTrue(all(loaded_at_persist),
                        f"存在 persist-先于-load: {loaded_at_persist}")

    def test_new_behavior_default_off_killswitch(self):
        idx = _fresh_indexer(tempfile.mkdtemp())
        self.assertFalse(idx._WALK_LEGACY_YIELD)


class TestLazyLoadSubprocess(unittest.TestCase):
    """§一④: 懒加载 + 日志可见性。子进程隔离 (对齐 test_hash_cache_persistence B5 模式,
    避免本进程 reload 状态泄漏)。"""

    CHILD = r"""
import io, logging, os, sys, tempfile
sys.path.insert(0, '.')
tmp = tempfile.mkdtemp()
from pathlib import Path
os.environ["MEMSVC_DB"] = os.path.join(tmp, "t.db")
# 先配 logging (模拟 server.py main 顺序: basicConfig 晚于 import)
buf = io.StringIO()
logging.basicConfig(level=logging.INFO, stream=buf, force=True)
import indexer
assert "hash cache loaded" not in buf.getvalue(), (
    f"import 期不应加载 hash cache: {buf.getvalue()!r}")  # db.py 自身日志 (vec0/feedback) 不在本单射程
# 放一个缓存文件: 3 条
indexer._HASH_CACHE_FILE = Path(tmp) / "cache.json"
import json
entries = {f"lazy_t:{tmp}/wd/f{i:03d}.md": f"mtime_ns:{i}:size={30+i}" for i in range(3)}
indexer._HASH_CACHE_FILE.write_text(json.dumps({"version": 1, "entries": entries}))
indexer._OFFSET_STATE_FILE = Path(tmp) / "off.json"
from config import ProjectConfig
base = Path(tmp) / "root"; (base / "wd").mkdir(parents=True)
for i in range(3):
    (base / "wd" / f"f{i:03d}.md").write_text("c" * 40)
proj = ProjectConfig(name="lazy_t", root=str(base), watch_dirs=["wd"])
indexer.index_file = lambda fp, p: 0   # stub: 只测 walk 入口的 ensure/load
indexer._persist_hashes = lambda: None
indexer._save_offsets = lambda: None
indexer.index_project(proj)
n_first = buf.getvalue().count("hash cache loaded")
assert n_first == 1, f"首次 walk 应恰加载并记一次日志, got {n_first}: {buf.getvalue()!r}"
assert "3 entries" in buf.getvalue(), buf.getvalue()
assert len(indexer._file_hashes) == 3, indexer._file_hashes
indexer.index_project(proj)  # 二次 walk 不再加载
assert buf.getvalue().count("hash cache loaded") == 1, "二次 walk 不应重复加载"
assert indexer._hashes_loaded is True
print("LAZY-CHILD-OK")
"""

    def test_lazy_load_logs_exactly_once(self):
        import subprocess
        r = subprocess.run(
            [sys.executable, "-c", self.CHILD],
            cwd=str(Path(__file__).resolve().parent.parent), capture_output=True, text=True, timeout=120)
        out = (r.stdout or "") + (r.stderr or "")
        self.assertEqual(r.returncode, 0, "子进程失败: " + out[-2000:])
        self.assertIn("LAZY-CHILD-OK", r.stdout)

    CHILD_PREWALK = r"""
import os, sys, tempfile
sys.path.insert(0, '.')
tmp = tempfile.mkdtemp()
from pathlib import Path
os.environ["MEMSVC_DB"] = os.path.join(tmp, "t.db")
import json
import indexer
indexer._HASH_CACHE_FILE = Path(tmp) / "cache.json"
indexer._OFFSET_STATE_FILE = Path(tmp) / "off.json"
base = Path(tmp) / "root"; (base / "wd").mkdir(parents=True)
f = base / "wd" / "solo.md"; f.write_text("prewalk content 123")
entries = {f"pw:{f}": indexer._file_hash(str(f))}
indexer._HASH_CACHE_FILE.write_text(json.dumps({"version": 1, "entries": entries}))
assert not indexer._file_hashes, "import 后应为空 (未加载)"
from config import ProjectConfig
proj = ProjectConfig(name="pw", root=str(base), watch_dirs=["wd"])
n = indexer.index_file(str(f), proj)  # 不经过 index_project, 直接 watcher 式调用
assert n == 0, f"缓存应命中 skip, got {n}"
assert len(indexer._file_hashes) == 1, "index_file 入口 ensure 应已加载缓存"
print("PREWALK-CHILD-OK")
"""

    def test_watcher_prewalk_cache_hit(self):
        import subprocess
        r = subprocess.run(
            [sys.executable, "-c", self.CHILD_PREWALK],
            cwd=str(Path(__file__).resolve().parent.parent), capture_output=True, text=True, timeout=120)
        out = (r.stdout or "") + (r.stderr or "")
        self.assertEqual(r.returncode, 0, "子进程失败: " + out[-2000:])
        self.assertIn("PREWALK-CHILD-OK", r.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
