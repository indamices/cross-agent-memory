"""Offline tests may only access disposable resources, set before imports."""
import os
import tempfile
from pathlib import Path

_ROOT = tempfile.TemporaryDirectory(prefix="memsvc-release-")
root = Path(_ROOT.name)
for key, value in {
    "MEMSVC_DATA_DIR": root,
    "MEMSVC_DB": root / "memory.db",
    "MEMSVC_COLD_DB": root / "cold.db",
    "MEMSVC_BACKUP_DIR": root / "backups",
    "MEMSVC_SNAPSHOT_DIR": root / "snapshots",
    "MEMSVC_PROJECTS_DIR": root / "projects",
    "MEMORY_MISS_LEDGER": root / "miss.jsonl",
    "MEMORY_HITS_LEDGER": root / "hits.jsonl",
    "MEMIDX_METRIC_LOG": root / "index.jsonl",
}.items():
    os.environ[key] = str(value)
