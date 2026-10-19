# Repository instructions

Read INSTALL-FOR-AI.md for installation and docs/security.md for trust boundaries.
Work only in this checkout. Do not commit, publish, send messages or modify system
services without a corresponding user request. Never open a production database
for testing. Set MEMSVC_DATA_DIR, MEMSVC_DB, MEMSVC_COLD_DB, backup, snapshot,
project and ledger paths before importing config/db/server. Use disposable data.

Default verification: `python -m pytest tests -v`, `bash -n install.sh`,
`./install.sh --dry-run`. These tests need declared dependencies, no Ollama.
Historical tests at repository root are opt-in; each module needs its own process.
Do not run broad root discovery: some historical scripts execute during import.

Installation is authorized by a user asking to install this project. Execute the
protocol without extra confirmations; stop on actual errors, preserve data and
report the failing command. Installing must not register personal projects,
restore a backup, deploy to a public interface, or create a login daemon.
