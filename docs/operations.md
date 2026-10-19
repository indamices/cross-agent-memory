# Operations and verification

Use a dedicated MEMSVC_DATA_DIR. Core runtime files, snapshots, backups and
feedback default there; every path is configurable via environment variables
(see README Configuration). Nothing writes outside the configured data dir.
Install never starts a daemon or changes launchd/systemd. Ctrl-C stops a
foreground service.

Use backup.py's SQLite backup API for consistent WAL snapshots. Never copy only
a live main DB file or invoke restore during validation. Keep backups private.

Default release tests: `python -m pytest tests -v`. Tests set all paths to
throwaway directories before importing runtime modules. The storage test seeds a
synthetic DB, uses SQLite backup to create an isolated clone, sets MEMSVC_DB to
it and queries that clone. Production is never
opened. HTTP tests use the real handler with an in-memory transport, suitable for
sandboxes that cannot bind sockets. They do not certify an actual network server.

Historical tests must run one module per process; many override env at import or
are script-style checks. A safe candidate group is:

```bash
python -m pytest test_monitor_rolling_window.py -v
python -m pytest test_backup_restore_guard_1019.py -v
```

Set MEMSVC_DATA_DIR, MEMSVC_DB, MEMSVC_COLD_DB, MEMSVC_BACKUP_DIR,
MEMSVC_SNAPSHOT_DIR and ledger paths to a disposable directory before each
process. Do not run root-level blanket discovery. Model/data-dependent vec/bit
regressions need a consistent isolated snapshot and Ollama; skipped tests do not
prove these features. CI rejects skips in the offline release suite.
