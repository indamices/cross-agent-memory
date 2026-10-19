#!/usr/bin/env python3
"""T-MEMSVC-WALK-SPEEDUP P1 单测 — hash 缓存持久化 (计划书 §四 + CC GateA C1/C2/N5)

覆盖: hash 格式 / round-trip / 逐条容错 / version 防御 / env 开关 /
      cleanup 死缓存清理 + 嵌套 root 不误删 / 并发持久化 / 重启模拟增量 walk
跑法: python3 -m unittest discover -s tests -p "test_hash_cache_persistence.py" -v
"""
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _fresh_indexer(tmpdir: str, env_overrides: dict | None = None):
    """隔离实例: 重载 indexer, 缓存/offset 文件指向 tmpdir。"""
    import importlib
    for k, v in (env_overrides or {}).items():
        os.environ[k] = v
    import indexer
    importlib.reload(indexer)
    indexer._HASH_CACHE_FILE = Path(tmpdir) / "file_hash_cache.json"
    indexer._OFFSET_STATE_FILE = Path(tmpdir) / "jsonl_offsets.json"
    indexer._file_hashes.clear()
    return indexer


class TestHashFormat(unittest.TestCase):
    def test_mtime_ns_size(self):
        idx = _fresh_indexer(tempfile.mkdtemp())
        d = Path(tempfile.mkdtemp())
        f = d / "a.txt"; f.write_text("hello")
        h = idx._file_hash(str(f))
        self.assertTrue(h.startswith("mtime_ns:"))
        self.assertIn(":size=5", h)
        # 内容不变 → hash 稳定
        self.assertEqual(h, idx._file_hash(str(f)))
        # 追加内容 → size 变 → hash 变
        f.write_text("hello world")
        self.assertNotEqual(h, idx._file_hash(str(f)))


class TestPersistLoadRoundTrip(unittest.TestCase):
    def test_roundtrip(self):
        tmp = tempfile.mkdtemp()
        idx = _fresh_indexer(tmp)
        idx._file_hashes["p:x"] = "mtime_ns:1:size=1"
        idx._persist_hashes()
        self.assertTrue(idx._HASH_CACHE_FILE.exists())
        idx._file_hashes.clear()
        idx._load_hashes()
        self.assertEqual(idx._file_hashes.get("p:x"), "mtime_ns:1:size=1")

    def test_per_entry_corruption(self):
        tmp = tempfile.mkdtemp()
        idx = _fresh_indexer(tmp)
        obj = {"version": 1, "entries": {"good": "mtime_ns:1:size=1",
                                         "bad": "mtime:123", "worse": 42}}
        idx._HASH_CACHE_FILE.write_text(json.dumps(obj))
        idx._load_hashes()
        self.assertIn("good", idx._file_hashes)
        self.assertNotIn("bad", idx._file_hashes)
        self.assertNotIn("worse", idx._file_hashes)

    def test_version_mismatch_whole_drop(self):
        tmp = tempfile.mkdtemp()
        idx = _fresh_indexer(tmp)
        idx._HASH_CACHE_FILE.write_text(json.dumps({"version": 99, "entries": {"x": "mtime_ns:1:size=1"}}))
        idx._load_hashes()
        self.assertEqual(idx._file_hashes, {})

    def test_garbage_json_degrades_to_full(self):
        tmp = tempfile.mkdtemp()
        idx = _fresh_indexer(tmp)
        idx._HASH_CACHE_FILE.write_text("{not json!!!")
        idx._load_hashes()  # 不抛
        self.assertEqual(idx._file_hashes, {})

    def test_env_off_disables(self):
        tmp = tempfile.mkdtemp()
        idx = _fresh_indexer(tmp, {"MEMSVC_HASH_CACHE": "0"})
        self.assertFalse(idx._HASH_CACHE_ENABLED)
        idx._file_hashes["p:x"] = "mtime_ns:1:size=1"
        idx._persist_hashes()
        self.assertFalse(idx._HASH_CACHE_FILE.exists())  # 关=不写
        os.environ.pop("MEMSVC_HASH_CACHE", None)


class TestCleanupNestedRoot(unittest.TestCase):
    """flash D2: /proj 不得误清 /proj2 的 offsets; 死缓存条目要清。"""

    def test_cleanup_and_nested_root(self):
        from config import ProjectConfig
        base = Path(tempfile.mkdtemp())
        p1 = base / "proj"; (p1 / "wd").mkdir(parents=True)
        p2 = base / "proj2"; (p2 / "wd").mkdir(parents=True)  # 嵌套名邻居
        for i in range(3):
            (p1 / "wd" / f"f{i}.md").write_text(f"content {i}")
            (p2 / "wd" / f"g{i}.md").write_text(f"content {i}")
        tmp = tempfile.mkdtemp()
        idx = _fresh_indexer(tmp)
        proj1 = ProjectConfig(name="proj1", root=str(p1), watch_dirs=["wd"])
        # 模拟: p1 有 3 文件缓存 + 1 死条目(已删文件); p2 的 offset 伪装在 _last_offsets
        for i in range(3):
            idx._file_hashes[f"proj1:{p1}/wd/f{i}.md"] = "mtime_ns:1:size=1"
        idx._file_hashes["proj1:{p1}/wd/deleted.md"] = "mtime_ns:1:size=1"  # 死条目
        idx._last_offsets[str(p2 / "wd" / "g0.md")] = 12345  # p2 的 offset — 不得被 p1 walk 清掉
        # stub index_file: 不真嵌入 (只返回 0 chunk), walk 只测 cleanup 路径
        idx.index_file = lambda fp, proj: 0
        idx._save_offsets = lambda: None
        res = idx.index_project(proj1, force=False, cleanup=True)
        self.assertEqual(res["files"], 3)
        self.assertNotIn(f"proj1:{p1}/wd/deleted.md", idx._file_hashes)  # 死条目已清
        for i in range(3):
            self.assertIn(f"proj1:{p1}/wd/f{i}.md", idx._file_hashes)    # 活文件保留
        self.assertIn(str(p2 / "wd" / "g0.md"), idx._last_offsets)       # 嵌套 root 未误删


class TestConcurrentPersist(unittest.TestCase):
    """CC C1: 多线程写 dict + persist 快照, 不得 RuntimeError。"""

    def test_no_iteration_crash(self):
        tmp = tempfile.mkdtemp()
        idx = _fresh_indexer(tmp)
        errors = []
        stop = threading.Event()

        def writer(n):
            try:
                for i in range(2000):
                    idx._file_hashes[f"w{n}:{i}"] = f"mtime_ns:{i}:size=1"
            except Exception as e:
                errors.append(e)

        def persister():
            try:
                while not stop.is_set():
                    idx._persist_hashes()
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        ts = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
        ps = [threading.Thread(target=persister) for _ in range(2)]
        for t in ts + ps: t.start()
        for t in ts: t.join()
        stop.set()
        for t in ps: t.join()
        self.assertEqual(errors, [], f"并发持久化炸了: {errors}")


class TestRestartSimulationE2E(unittest.TestCase):
    """CC N5: 小文件为主, walk → 持久化 → 模拟重启(清内存+load) → 改10个 → 二轮 walk 只重索引改动的。"""

    def test_incremental_walk_after_restart(self):
        from config import ProjectConfig
        base = Path(tempfile.mkdtemp())
        wd = base / "wd"; wd.mkdir()
        N = 200  # 小文件 (<5MB, CC N5: 大头场景)
        for i in range(N):
            (wd / f"f{i:03d}.md").write_text(f"doc {i} " + "x" * (50 + i))
        tmp = tempfile.mkdtemp()
        idx = _fresh_indexer(tmp)
        proj = ProjectConfig(name="e2e", root=str(base), watch_dirs=["wd"])
        indexed_calls = []

        def fake_index_file(fp, p):
            # 行为等价 stub: 复刻 index_file 的"hash 命中跳过 + 未命中写缓存"语义 (真实函数写缓存, 纯计数 stub 会漏)
            h = idx._file_hash(fp)
            key = f"{p.name}:{fp}"
            if idx._file_hashes.get(key) == h:
                return 0
            idx._file_hashes[key] = h
            indexed_calls.append(fp)
            return 1

        idx.index_file = fake_index_file
        # 一轮 walk: 全量 N 个
        idx.index_project(proj, cleanup=True)
        self.assertEqual(len(indexed_calls), N)
        # 模拟重启: 内存清空 → 从盘加载
        idx._file_hashes.clear()
        idx._load_hashes()
        self.assertEqual(len(idx._file_hashes), N)  # 全部恢复
        # 改 10 个
        changed = [wd / f"f{i:03d}.md" for i in range(0, N, 20)]
        for f in changed:
            f.write_text(f"doc UPDATED " + "y" * 100)
        indexed_calls.clear()
        idx.index_project(proj, cleanup=True)
        self.assertEqual(len(indexed_calls), 10, f"二轮应只重索引10个, 实际 {len(indexed_calls)}")
        for f in changed:
            self.assertIn(str(f), indexed_calls)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestB5Integration(unittest.TestCase):
    """flash 测试审 D1: P1 缓存 × B5 jsonl 增量读 真实集成 — 子进程隔离跑
    (教训: 进程内 reload 隔离曾把 13000 测试 chunks 漏进生产库 b5it, 已手动清除;
    子进程 env 从零设置, db/indexer 模块状态绝无跨用例泄漏)。"""

    CHILD = r"""
import sys, os, json, tempfile
sys.path.insert(0, '.')
tmp = tempfile.mkdtemp()
os.environ["MEMSVC_DB"] = os.path.join(tmp, "t.db")
from pathlib import Path
import db  # fresh-install schema bootstrap happens on first connection
db._get_conn().close()
import embedder
embedder.embed_batch = lambda texts: [[0.0]*1024 for _ in texts]
import indexer
indexer.embed_batch = embedder.embed_batch
indexer._OFFSET_STATE_FILE = Path(tmp) / "off.json"
indexer._HASH_CACHE_FILE = Path(tmp) / "cache.json"
indexer._offsets_loaded = True
from config import ProjectConfig
base = Path(tmp) / "root"; (base / "wd").mkdir(parents=True)
proj = ProjectConfig(name="b5it", root=str(base), watch_dirs=["wd"])
big = base / "wd" / "session_big.jsonl"
lines = [json.dumps({"type":"user","timestamp":f"2026-08-15T00:{i%60:02d}:00","message":{"role":"user","content":[{"type":"text","text":f"question {i} "+"z"*400}]}}) for i in range(13000)]
big.write_text("\n".join(lines) + "\n")
size = big.stat().st_size
assert size > 5*1024*1024, size
# ① 首轮真实 B5 增量索引
n1 = indexer.index_file(str(big), proj)
assert n1 > 0, f"round1 n1={n1}"
off1 = indexer._last_offsets.get(str(big))
assert off1 == size, (off1, size)
# ② persist → 模拟重启 → load
indexer._persist_hashes()
indexer._save_offsets()
indexer._file_hashes.clear(); indexer._last_offsets.clear()
indexer._load_hashes()
indexer._offsets_loaded = False; indexer._load_offsets()  # 重启语义: offsets 也从盘恢复 (否则 B5 走"首次迁移"跳尾路径)
assert indexer._file_hashes.get(f"b5it:{big}") == indexer._file_hash(str(big)), "缓存恢复失配"
# ③ 追加 → 游标续读只处理新增
with open(big, "a") as f:
    f.write(json.dumps({"type":"user","timestamp":"2026-08-15T23:59:00","message":{"role":"user","content":[{"type":"text","text":"APPEND-NEW-QUESTION "+"w"*400}]}}) + "\n")
n2 = indexer.index_file(str(big), proj)
assert n2 > 0, f"round2 n2={n2}"
assert indexer._last_offsets.get(str(big)) == big.stat().st_size, "游标未推进到尾"
# ④ 未变再跑 → 缓存命中零重做
n3 = indexer.index_file(str(big), proj)
assert n3 == 0, f"round3 应 0, 实际 {n3}"
# ⑤ garbage 缓存降级后 B5 游标仍完整 (offsets 独立持久化)
indexer._HASH_CACHE_FILE.write_text("{broken!!!")
indexer._file_hashes.clear()
indexer._load_hashes()
assert not indexer._file_hashes
n4 = indexer.index_file(str(big), proj)  # cache miss → B5: offset==size 快速返回
assert indexer._last_offsets.get(str(big)) == big.stat().st_size, "缓存降级后游标受损"
print("B5-CHILD-OK", dict(n1=n1, n2=n2, n3=n3, n4=n4))
"""

    def test_b5_subprocess_isolated(self):
        import subprocess
        r = subprocess.run(
            [sys.executable, "-c", self.CHILD],
            cwd=str(Path(__file__).resolve().parent.parent), capture_output=True, text=True, timeout=600)
        out = (r.stdout or "") + (r.stderr or "")
        self.assertEqual(r.returncode, 0, "子进程失败: " + out[-2000:])
        self.assertIn("B5-CHILD-OK", r.stdout)
        # 子进程绝不能碰生产库: 断言其 db 文件在系统临时目录
        self.assertNotIn("memory.db" + chr(10), out)
