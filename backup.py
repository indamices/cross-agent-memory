"""SQLite backup and snapshot archive for memory-service.

Provides file-level backup of the SQLite database and archival of
old snapshot files to compressed zip archives.
"""

import logging
import os
import re
import shutil
import sqlite3
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from config import BACKUP, SQLITE_DB, SERVICE_DIR

# Snapshot dir is relative to the service directory by default;
# override with MEMSVC_SNAPSHOT_DIR.
_API_SNAPSHOT_DIR = Path(os.environ.get(
    "MEMSVC_SNAPSHOT_DIR", str(SERVICE_DIR / "api_snapshots")))

logger = logging.getLogger("memory-service.backup")

_BACKUP_ROOT = SERVICE_DIR / BACKUP.backup_dir

def get_api_snapshot_dir() -> Path:
    return _API_SNAPSHOT_DIR


def backup_sqlite() -> dict:
    """Create a timestamped backup of the SQLite database file.

    Returns:
        Dict with ``status``, ``backup_dir``, ``files``, and ``size_mb``.
        On failure, includes an ``error`` key.
    """
    if not SQLITE_DB.exists():
        return {"status": "error", "error": f"DB file not found: {SQLITE_DB}"}

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    backup_path = _BACKUP_ROOT / timestamp
    backup_path.mkdir(parents=True, exist_ok=True)

    target = backup_path / "memory.db"
    try:
        # Use the SQLite backup API for a consistent snapshot — a plain file
        # copy can miss committed transactions that still live in the -wal
        # file while WAL mode is active.
        src = sqlite3.connect(f"file:{quote(str(SQLITE_DB))}?mode=ro", uri=True,
                              timeout=30)
        try:
            dst = sqlite3.connect(str(target))
            try:
                with dst:
                    src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
        size_bytes = target.stat().st_size
        size_mb = round(size_bytes / (1024**2), 1)

        logger.info(f"SQLite backup created: {backup_path} ({size_mb} MB)")

        cleanup_old_backups()

        try:
            rel_dir = str(backup_path.relative_to(SERVICE_DIR))
        except ValueError:
            rel_dir = str(backup_path)  # custom MEMSVC_BACKUP_DIR outside SERVICE_DIR
        return {
            "status": "ok",
            "backup_dir": rel_dir,
            "backup_name": timestamp,
            "files": 1,
            "size_mb": size_mb,
        }
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        if backup_path.exists():
            shutil.rmtree(backup_path, ignore_errors=True)
        return {"status": "error", "error": str(e)}




def list_backups() -> list[dict]:
    """List all existing backups with metadata.

    Returns:
        List of dicts sorted by name (newest first), each with
        ``name``, ``files``, ``size_mb``, and ``age_hours``.
    """
    if not _BACKUP_ROOT.exists():
        return []

    backups = []
    now = datetime.now()
    for d in sorted(_BACKUP_ROOT.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        file_count = sum(1 for _ in d.rglob("*") if _.is_file())
        size_bytes = sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
        size_mb = round(size_bytes / (1024**2), 1)

        # Parse age from directory name
        try:
            backup_time = datetime.strptime(d.name, "%Y-%m-%d_%H%M%S")
            age_hours = round((now - backup_time).total_seconds() / 3600, 1)
        except ValueError:
            age_hours = -1

        backups.append(
            {
                "name": d.name,
                "files": file_count,
                "size_mb": size_mb,
                "age_hours": age_hours,
            }
        )

    return backups


def cleanup_old_backups() -> int:
    """Remove oldest backups, keeping only ``max_backups`` most recent.

    Returns:
        Number of backups removed.
    """
    backups = list_backups()
    if len(backups) <= BACKUP.max_backups:
        return 0

    removed = 0
    for backup in backups[BACKUP.max_backups :]:
        backup_path = _BACKUP_ROOT / backup["name"]
        try:
            shutil.rmtree(backup_path)
            removed += 1
            logger.info(f"Removed old backup: {backup['name']}")
        except Exception as e:
            logger.warning(f"Failed to remove backup {backup['name']}: {e}")

    return removed


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------


_BACKUP_NAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{6}$")


def restore_sqlite(backup_name: str) -> dict:
    """Restore SQLite database from a named backup.

    ``backup_name`` must be a backup directory name produced by
    :func:`backup_sqlite` (``YYYY-MM-DD_HHMMSS``). Anything else — including
    path separators or ``..`` — is rejected before touching the filesystem.

    Args:
        backup_name: Backup directory name (e.g. ``2026-03-29_153000``).

    Returns:
        Dict with ``status`` and details. On failure, includes ``error``.
    """
    if not isinstance(backup_name, str) or not _BACKUP_NAME_RE.fullmatch(backup_name):
        return {
            "status": "error",
            "error": f"Invalid backup name (expected YYYY-MM-DD_HHMMSS): {backup_name!r}",
        }
    backup_path = _BACKUP_ROOT / backup_name
    if backup_path.resolve().parent != _BACKUP_ROOT.resolve():
        return {"status": "error", "error": "Invalid backup path"}

    if not backup_path.is_dir():
        return {"status": "error", "error": f"Backup not found: {backup_name}"}

    backup_db = backup_path / "memory.db"
    if not backup_db.exists():
        return {
            "status": "error",
            "error": "Backup missing memory.db file",
        }

    try:
        # Restore THROUGH SQLite (the backup API) instead of file surgery.
        # Unlinking -wal/-shm and copying files behind SQLite's back is both
        # unsafe (a connection that has OS state from earlier in this process
        # yields "disk I/O error" on the next open) and incorrect (a stale
        # -wal resurrects pre-restore writes). The backup API is consistent,
        # atomic, and needs no file deletion; then checkpoint to trim the WAL.
        src = sqlite3.connect(f"file:{quote(str(backup_db))}?mode=ro", uri=True)
        try:
            dst = sqlite3.connect(str(SQLITE_DB))
            try:
                with dst:
                    src.backup(dst)
                dst.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                dst.close()
        finally:
            src.close()

        logger.info(f"Restored SQLite from backup: {backup_name}")
        return {
            "status": "ok",
            "restored_from": backup_name,
            "restored_to": str(SQLITE_DB),
        }
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        return {"status": "error", "error": str(e)}




# ---------------------------------------------------------------------------
# Snapshot archival
# ---------------------------------------------------------------------------

_SNAPSHOT_DIRS = [
    _API_SNAPSHOT_DIR,
    Path(_API_SNAPSHOT_DIR.parent / "pre_compress"),
]

_ARCHIVE_ROOT = SERVICE_DIR / "archives"


def archive_snapshots(days: int | None = None) -> dict:
    """Compress snapshot files older than ``days`` into zip archives.

    Processes both ``api_snapshots/`` and ``pre_compress/`` directories.
    Old files are moved into ``archives/YYYY-MM-DD_snapshots.zip`` and
    deleted from the source directory.

    Args:
        days: Minimum age in days. Defaults to ``BACKUP.archive_days``.

    Returns:
        Dict with ``status``, ``archived`` count, and ``freed_mb`` size.
    """
    days = days if days is not None else BACKUP.archive_days
    cutoff = datetime.now().timestamp() - (days * 86400)

    total_archived = 0
    total_freed = 0

    for snap_dir in _SNAPSHOT_DIRS:
        if not snap_dir.exists():
            continue

        # Find old files
        old_files = []
        for f in snap_dir.iterdir():
            if f.is_file() and f.stat().st_mtime < cutoff:
                old_files.append(f)

        if not old_files:
            continue

        # Create archive
        _ARCHIVE_ROOT.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y-%m-%d")
        zip_path = _ARCHIVE_ROOT / f"{date_str}_snapshots.zip"

        dir_freed = 0
        try:
            with zipfile.ZipFile(zip_path, "a", zipfile.ZIP_DEFLATED) as zf:
                for f in old_files:
                    arc_name = f"{snap_dir.name}/{f.name}"
                    zf.write(f, arc_name)
                    dir_freed += f.stat().st_size
                    total_archived += 1

            # Delete archived files
            for f in old_files:
                f.unlink()

            total_freed += dir_freed
            logger.info(
                f"Archived {len(old_files)} snapshots from {snap_dir.name} "
                f"to {zip_path.name} ({round(dir_freed / (1024**2), 1)} MB)"
            )
        except Exception as e:
            logger.error(f"Failed to archive snapshots from {snap_dir}: {e}")

    return {
        "status": "ok",
        "archived": total_archived,
        "freed_mb": round(total_freed / (1024**2), 1),
    }
