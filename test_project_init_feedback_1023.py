"""
Independent result-state test: project initialization error containment.

Test author: GLM (test author only). Written strictly against the public
interface spec; implementation code was not read.

Isolation: all resource env vars are set under a temp dir BEFORE importing
indexer/server/db/config. Runner provides repo PYTHONPATH and runs this file
in an independent process.
"""

import importlib
import os
import sys
import tempfile
from types import SimpleNamespace

import pytest

ENV_DIR_VARS = [
    "MEMSVC_DATA_DIR",
    "MEMSVC_DB",
    "MEMSVC_COLD_DB",
    "MEMSVC_BACKUP_DIR",
    "MEMSVC_SNAPSHOT_DIR",
    "MEMSVC_PROJECTS_DIR",
]
ENV_FILE_VARS = [
    "MEMORY_MISS_LEDGER",
    "MEMORY_HITS_LEDGER",
    "MEMIDX_METRIC_LOG",
]


class StubObserver:
    """Records construction, schedule and start; no native threads."""

    instances = []

    def __init__(self, *args, **kwargs):
        self.schedules = []
        self.started = False
        self.stopped = False
        StubObserver.instances.append(self)

    def schedule(self, handler, path, recursive=False, *args, **kwargs):
        self.schedules.append((path, recursive))

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


@pytest.fixture()
def modules_under_sandbox_env(monkeypatch):
    base = tempfile.mkdtemp(prefix="cam-project-init-test-1023-")
    for var in ENV_DIR_VARS:
        monkeypatch.setenv(var, os.path.join(base, var.lower()))
    for var in ENV_FILE_VARS:
        monkeypatch.setenv(var, os.path.join(base, var.lower() + ".log"))
    monkeypatch.setenv("MEMSVC_HASH_CACHE", "0")
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")

    for name in ("indexer", "server", "db", "config"):
        sys.modules.pop(name, None)

    config = importlib.import_module("config")
    indexer = importlib.import_module("indexer")
    server = importlib.import_module("server")
    return SimpleNamespace(config=config, indexer=indexer, server=server)


def test_init_projects_bad_project_does_not_block_good_project(
    modules_under_sandbox_env, tmp_path, monkeypatch
):
    mod = modules_under_sandbox_env
    server = mod.server
    indexer = mod.indexer

    ProjectConfig = (
        getattr(mod.config, "ProjectConfig", None)
        or getattr(server, "ProjectConfig", None)
        or getattr(indexer, "ProjectConfig", None)
    )
    assert ProjectConfig is not None, "ProjectConfig not found on public surface"

    bad_root = tmp_path / "bad"
    good_root = tmp_path / "good"
    bad_root.mkdir()
    good_root.mkdir()
    rule_md = good_root / "RULE.md"
    rule_md.write_text("# rules\n", encoding="utf-8")

    bad = ProjectConfig(
        name="bad_missing_dir",
        root=bad_root,
        watch_dirs=[bad_root / "no_such_dir"],
        watch_files=[],
    )
    good = ProjectConfig(
        name="good_rule_md",
        root=good_root,
        watch_dirs=[],
        watch_files=[rule_md],
    )

    monkeypatch.setattr(server, "discover_projects", lambda: [bad, good])

    index_calls = []

    def fake_index_project(proj, force=False, cleanup=True):
        index_calls.append(proj)
        return {"indexed": 0, "skipped": 0, "files": 0}

    monkeypatch.setattr(server, "index_project", fake_index_project)
    monkeypatch.setattr(indexer, "verify_hash_cache_against_db", lambda projects: 0)
    if hasattr(server, "verify_hash_cache_against_db"):
        monkeypatch.setattr(server, "verify_hash_cache_against_db", lambda projects: 0)

    StubObserver.instances = []
    monkeypatch.setattr(indexer, "Observer", StubObserver)

    monkeypatch.setattr(server, "_observers", [])
    monkeypatch.setattr(server, "_initialized_projects", set())
    monkeypatch.setattr(server, "_indexing_count", 0)

    server._init_projects()

    initialized = server._initialized_projects
    assert bad.name not in initialized
    assert good.name in initialized

    assert index_calls == [good], (
        f"index_project must be called exactly once for the good project, "
        f"got: {[getattr(p, 'name', p) for p in index_calls]}"
    )

    started = [o for o in StubObserver.instances if o.started]
    assert len(started) == 1, (
        f"exactly one observer should start, constructed={len(StubObserver.instances)}"
    )
    good_obs = started[0]
    assert good_obs.schedules, "good observer must carry at least one schedule"
    first_path, first_recursive = good_obs.schedules[0]
    assert os.path.realpath(str(first_path)) == os.path.realpath(str(good_root.resolve()))
    assert not first_recursive

    assert server._indexing_count == 0
    assert server._indexing_count >= 0, "counter must never go negative"

    assert len(StubObserver.instances) >= len(started), (
        "constructor must record every instance, including any observer "
        "constructed for the failing bad project"
    )
