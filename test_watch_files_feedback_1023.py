"""test_watch_files_feedback_1023.py
独立测试：ProjectConfig.watch_files 单文件持续更新（watch_files 持续监听）。
测试作者：GLM-5.3，仅依据 watch-test-spec.md (2026-10-23) 编写，未阅读任何实现代码。
本文件须在独立子进程中执行；环境隔离在 import indexer 之前完成。
"""

import os
import sys
import tempfile
import threading
import time

# ---- 环境隔离：必须在 import indexer 之前设置（config 为全局导入）----
_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_ISOLATION_ROOT = tempfile.mkdtemp(prefix="memsvc_watchfiles_1023_")
os.environ["MEMSVC_DB"] = os.path.join(_ISOLATION_ROOT, "isolated.db")
os.environ["MEMSVC_DATA_DIR"] = os.path.join(_ISOLATION_ROOT, "data")
os.environ["MEMSVC_HASH_CACHE"] = "0"
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.makedirs(os.environ["MEMSVC_DATA_DIR"], exist_ok=True)

sys.path.insert(0, _TEST_DIR)

from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileMovedEvent

import indexer


# ---------- 测试辅助（不依赖任何实现细节，仅规格声明的公开接口）----------

def _rp(path):
    return os.path.realpath(os.fspath(path))


class DispatchRecorder:
    """替换 _WatchHandler._dispatch，捕获真实交付路径；线程安全。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._seen = []

    def __call__(self, file_path):
        with self._lock:
            self._seen.append(os.fspath(file_path))

    def count_of(self, path):
        target = _rp(path)
        with self._lock:
            return sum(1 for p in self._seen if _rp(p) == target)

    def total(self):
        with self._lock:
            return len(self._seen)


class FakeObserver:
    """记录 schedule/start 调用，不启动真实监听线程。"""

    def __init__(self):
        self.schedules = []  # [(handler, path_str, recursive_bool)]
        self.started = False

    def schedule(self, handler, path, recursive=False):
        self.schedules.append((handler, os.fspath(path), bool(recursive)))

    def start(self):
        self.started = True

    def stop(self):
        pass

    def join(self, timeout=None):
        pass


def make_project(name, root, watch_dirs=(), watch_files=(), exclude_patterns=()):
    return indexer.ProjectConfig(
        name=name,
        root=os.fspath(root),
        watch_dirs=[os.fspath(p) for p in watch_dirs],
        watch_files=[os.fspath(p) for p in watch_files],
        exclude_patterns=list(exclude_patterns),
    )


def install_class_recorder(monkeypatch):
    rec = DispatchRecorder()
    monkeypatch.setattr(indexer._WatchHandler, "_dispatch", rec)
    return rec


def use_fake_observer(monkeypatch):
    monkeypatch.setattr(indexer, "Observer", FakeObserver)


def _wait_until(predicate, timeout=5.0, step=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


# ---------- 结果态 1：仅 watch_files → 父目录非递归 watch + start ----------

def test_watch_files_only_registers_nonrecursive_parent_watch_and_starts(tmp_path, monkeypatch):
    rule = tmp_path / "RULE.md"
    rule.write_text("v1", encoding="utf-8")
    use_fake_observer(monkeypatch)
    proj = make_project("p-onlyfiles", tmp_path, watch_files=[rule])

    obs = indexer.start_watching(proj)

    assert isinstance(obs, FakeObserver)
    assert obs.started, "注册后必须调用 start()"
    assert obs.schedules, "不能只返回一个无 schedule 的 Observer"
    parent_matches = [s for s in obs.schedules if _rp(s[1]) == _rp(tmp_path)]
    assert parent_matches, "必须给文件的存在父目录注册 watch"
    assert all(not s[2] for s in parent_matches), "单文件 watch 必须非递归"


# ---------- 结果态 2：同父目录只注册一次；父目录兼为 watch_dir → recursive 且不重复 ----------

def test_two_files_same_parent_register_once(tmp_path, monkeypatch):
    a = tmp_path / "A.md"
    b = tmp_path / "B.md"
    a.write_text("a", encoding="utf-8")
    b.write_text("b", encoding="utf-8")
    use_fake_observer(monkeypatch)
    proj = make_project("p-sameparent", tmp_path, watch_files=[a, b])

    obs = indexer.start_watching(proj)

    parent_matches = [s for s in obs.schedules if _rp(s[1]) == _rp(tmp_path)]
    assert len(parent_matches) == 1, "同父目录两个文件必须合并为一次注册"


def test_parent_also_watch_dir_keeps_recursive_and_no_duplicate(tmp_path, monkeypatch):
    rule = tmp_path / "RULE.md"
    rule.write_text("v1", encoding="utf-8")
    use_fake_observer(monkeypatch)
    proj = make_project("p-parentdir", tmp_path, watch_dirs=[tmp_path], watch_files=[rule])

    obs = indexer.start_watching(proj)

    parent_matches = [s for s in obs.schedules if _rp(s[1]) == _rp(tmp_path)]
    assert len(parent_matches) == 1, "父目录不得重复注册"
    assert parent_matches[0][2] is True, "父目录同时是 watch_dir 时必须保留 recursive=True"


# ---------- 结果态 3：事件级文件过滤 ----------

def test_handler_modified_created_dispatch_only_watched_file(tmp_path, monkeypatch):
    rule = tmp_path / "RULE.md"
    other = tmp_path / "OTHER.md"
    rule.write_text("v1", encoding="utf-8")
    other.write_text("o", encoding="utf-8")
    proj = make_project("p-filter", tmp_path, watch_files=[rule])
    handler = indexer._WatchHandler(proj)
    rec = DispatchRecorder()
    monkeypatch.setattr(handler, "_dispatch", rec)

    handler.on_modified(FileModifiedEvent(str(rule)))
    handler.on_created(FileCreatedEvent(str(rule)))
    handler.on_modified(FileModifiedEvent(str(other)))
    handler.on_created(FileCreatedEvent(str(other)))

    assert rec.count_of(rule) == 2, "允许文件的 modified/created 均须交付"
    assert rec.count_of(other) == 0, "同目录 OTHER.md 不得交付"


def test_handler_rejects_temp_and_editor_backup_siblings(tmp_path, monkeypatch):
    rule = tmp_path / "RULE.md"
    rule.write_text("v1", encoding="utf-8")
    junk_names = [".RULE.md.tmp", "RULE.md~", "RULE.md.swp", "#RULE.md#"]
    junk_files = []
    for name in junk_names:
        p = tmp_path / name
        p.write_text("junk", encoding="utf-8")
        junk_files.append(p)
    proj = make_project("p-junk", tmp_path, watch_files=[rule])
    handler = indexer._WatchHandler(proj)
    rec = DispatchRecorder()
    monkeypatch.setattr(handler, "_dispatch", rec)

    for p in junk_files:
        handler.on_created(FileCreatedEvent(str(p)))
        handler.on_modified(FileModifiedEvent(str(p)))

    assert rec.total() == 0, "临时文件/编辑器备份不得交付"


# ---------- 结果态 4：FileMovedEvent 原子替换 ----------

def test_moved_event_atomic_replace_dispatches_target_not_tmp(tmp_path, monkeypatch):
    rule = tmp_path / "RULE.md"
    rule.write_text("v1", encoding="utf-8")
    tmpf = tmp_path / ".RULE.md.tmp"
    tmpf.write_text("v2", encoding="utf-8")
    proj = make_project("p-move", tmp_path, watch_files=[rule])
    handler = indexer._WatchHandler(proj)
    rec = DispatchRecorder()
    monkeypatch.setattr(handler, "_dispatch", rec)

    handler.on_moved(FileMovedEvent(str(tmpf), str(rule)))

    assert rec.count_of(rule) == 1, "原子替换必须交付目标路径"
    assert rec.count_of(tmpf) == 0, "tmpfile 路径不得被索引"

    src = tmp_path / "x.tmp"
    src.write_text("x", encoding="utf-8")
    handler.on_moved(FileMovedEvent(str(src), str(tmp_path / "unrelated.md")))
    assert rec.count_of(rule) == 1, "移到无关文件不得产生额外交付"


# ---------- 结果态 5：watch_dirs 递归语义不被文件过滤破坏 ----------

def test_watch_dirs_recursive_semantics_preserved(tmp_path, monkeypatch):
    notes = tmp_path / "notes"
    sub = notes / "sub"
    sub.mkdir(parents=True)
    inside = sub / "n.md"
    inside.write_text("n", encoding="utf-8")
    outside = tmp_path / "outside.md"
    outside.write_text("o", encoding="utf-8")
    proj = make_project("p-dirs", tmp_path, watch_dirs=[notes])
    handler = indexer._WatchHandler(proj)
    rec = DispatchRecorder()
    monkeypatch.setattr(handler, "_dispatch", rec)

    handler.on_modified(FileModifiedEvent(str(inside)))
    assert rec.count_of(inside) == 1, "已有目录树内文件必须继续被接受"

    handler.on_modified(FileModifiedEvent(str(outside)))
    assert rec.count_of(outside) == 0, "目录树外文件（未列入 watch_files）不得接受"


# ---------- 结果态 6：软链与不存在目标 ----------

def test_symlink_watches_resolved_real_parent_and_accepts_real_target(tmp_path, monkeypatch):
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    real_target = real_dir / "RULE.md"
    real_target.write_text("v1", encoding="utf-8")
    link = tmp_path / "RULE.md"
    link.symlink_to(real_target)

    use_fake_observer(monkeypatch)
    proj = make_project("p-symlink", tmp_path, watch_files=[link])
    obs = indexer.start_watching(proj)

    assert [s for s in obs.schedules if _rp(s[1]) == _rp(real_dir)], \
        "必须监听 resolve 后的真实父目录，而非链接所在目录"

    handler = indexer._WatchHandler(proj)
    rec = DispatchRecorder()
    monkeypatch.setattr(handler, "_dispatch", rec)
    handler.on_modified(FileModifiedEvent(str(real_target)))
    assert rec.count_of(real_target) == 1, "真实目标修改必须按 realpath 交付"


def test_missing_target_watches_existing_parent_for_future_creation(tmp_path, monkeypatch):
    future = tmp_path / "NEW.md"
    assert not future.exists()
    use_fake_observer(monkeypatch)
    proj = make_project("p-future", tmp_path, watch_files=[future])

    obs = indexer.start_watching(proj)
    assert [s for s in obs.schedules if _rp(s[1]) == _rp(tmp_path)], \
        "目标不存在但父目录存在时，必须监听父目录等待未来创建"

    handler = indexer._WatchHandler(proj)
    rec = DispatchRecorder()
    monkeypatch.setattr(handler, "_dispatch", rec)
    handler.on_created(FileCreatedEvent(str(future)))
    assert rec.count_of(future) == 1, "未来创建目标时创建事件必须被接受"


# ---------- 结果态 7：真实 watchdog Observer E2E ----------

def test_e2e_real_write_delivers_target_not_sibling(tmp_path, monkeypatch):
    rule = tmp_path / "RULE.md"
    other = tmp_path / "OTHER.md"
    rule.write_text("v1", encoding="utf-8")
    proj = make_project("p-e2e-write", tmp_path, watch_files=[rule])
    rec = install_class_recorder(monkeypatch)

    obs = indexer.start_watching(proj)
    try:
        time.sleep(0.5)  # 等真实 Observer 注册生效
        baseline = rec.count_of(rule)
        rule.write_text("v2", encoding="utf-8")
        assert _wait_until(lambda: rec.count_of(rule) > baseline), \
            "目标真实 write 后必须在 5s 内最终交付"

        other.write_text("noise", encoding="utf-8")
        time.sleep(1.5)  # 负向断言观察窗
        assert rec.count_of(other) == 0, "无关同目录文件不得交付"
    finally:
        obs.stop()
        obs.join(timeout=5)


def test_e2e_atomic_replace_delivers_target(tmp_path, monkeypatch):
    rule = tmp_path / "RULE.md"
    rule.write_text("v1", encoding="utf-8")
    tmpf = tmp_path / ".RULE.md.tmp"
    proj = make_project("p-e2e-move", tmp_path, watch_files=[rule])
    rec = install_class_recorder(monkeypatch)

    obs = indexer.start_watching(proj)
    try:
        time.sleep(0.5)
        baseline = rec.count_of(rule)
        tmpf.write_text("v2", encoding="utf-8")
        os.replace(str(tmpf), str(rule))
        assert _wait_until(lambda: rec.count_of(rule) > baseline), \
            "原子替换（tmp→目标）后必须在 5s 内最终交付目标"
        assert rec.count_of(tmpf) == 0, "tmpfile 自身不得被索引"
    finally:
        obs.stop()
        obs.join(timeout=5)


# 追加到 test_watch_files_feedback_1023.py（现有 11 个测试保留）

import os


def test_symlink_source_identity_keeps_config_path(tmp_path, monkeypatch):
    """补充用例12：realpath 仅用于 OS 监听/匹配，source 身份必须保持配置路径。

    场景：配置软链 /tmp/repo/RULE.md -> /tmp/real/RULE.md
      - handler 接受真实目标事件（监听真实父目录）
      - 交付给 _dispatch 的原始字符串必须等于配置路径（非 realpath）
      - 目录软链同理：事件映射回配置目录 + 相对路径
    """
    import indexer

    repo_dir = tmp_path / "repo"
    real_dir = tmp_path / "real"
    repo_dir.mkdir()
    real_dir.mkdir()

    # 真实目标 + 配置软链（文件）
    real_rule = real_dir / "RULE.md"
    real_rule.write_text("v1\n")
    cfg_rule = repo_dir / "RULE.md"
    cfg_rule.symlink_to(real_rule)

    # 真实子目录 + 配置软链（目录）
    real_notes = real_dir / "notes"
    real_notes.mkdir()
    (real_notes / "a.md").write_text("note-a\n")
    cfg_notes = repo_dir / "notes"
    cfg_notes.symlink_to(real_notes, target_is_directory=True)

    cfg = indexer.ProjectConfig(
        name="symlink-identity",
        root=str(repo_dir),
        watch_dirs=[str(cfg_notes)],
        watch_files=[str(cfg_rule)],
        exclude_patterns=[],
    )

    delivered = []
    handler = indexer._WatchHandler(cfg)
    monkeypatch.setattr(
        handler, "_dispatch",
        lambda file_path: delivered.append(str(file_path)),
    )

    # --- 文件软链：真实目标事件被接受，source = 配置路径 ---
    ev = indexer.FileModifiedEvent(str(real_rule))
    handler.on_modified(ev)
    assert delivered == [str(cfg_rule)], (
        f"文件软链事件必须交付配置路径，实际交付: {delivered}"
    )

    # --- 目录软链：真实子树事件映射回配置目录 + 相对路径 ---
    delivered.clear()
    ev2 = indexer.FileModifiedEvent(str(real_notes / "a.md"))
    handler.on_modified(ev2)
    expected = os.path.join(str(cfg_notes), "a.md")
    assert delivered == [expected], (
        f"目录软链事件必须映射回配置目录+相对路径，期望 {expected}，实际 {delivered}"
    )

    # --- 反向守卫：交付值不得是 realpath 归一化结果 ---
    delivered.clear()
    ev3 = indexer.FileModifiedEvent(str(real_rule))
    handler.on_modified(ev3)
    assert str(real_rule) not in delivered, "交付了真实目标路径=身份丢失"
    assert os.path.realpath(str(cfg_rule)) not in delivered or delivered == [
        str(cfg_rule)
    ], "不得以 realpath 计数替代配置路径身份"
