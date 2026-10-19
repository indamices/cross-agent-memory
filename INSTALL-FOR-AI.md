# AI installation protocol

Target: macOS/Linux, Python >=3.10, local single-user service. Commands run in
this checkout. If given a repository URL, clone that exact URL to a new directory
and enter it; never replace an existing checkout. No repository URL is assumed.

1. **Preflight:** read AGENTS.md; run `git status --short`,
   `python3 --version`, `./install.sh --dry-run`. Check `python3 -m venv --help`.
   If Python/venv/pip is missing, report the package required for this OS; do not
   mutate system Python. `PYTHON=/path/to/python3.12 ./install.sh` selects Python.
2. **Install:** run `./install.sh`. It reuses `.venv`, installs this package,
   tests sqlite-vec extension loading and FTS5 trigram, initializes the schema,
   prints the exact start command, and exits on errors. If extension loading is
   disabled, use a Python/SQLite build with extension support; do not disable
   error checks. Installation never starts a daemon.
3. **Data:** export `MEMSVC_DATA_DIR="$PWD/.local-data"` and
   `MEMSVC_DB="$MEMSVC_DATA_DIR/memory.db"` in each terminal. For a customized
   installation preserve the paths printed by the installer. Run
   `.venv/bin/python -c "import db; db.get_or_create_collection('install-probe')"`
   twice; both must succeed (the schema is created on first use, idempotently).
   Do not point these variables at an existing production DB for validation.
4. **Model:** check `ollama --version`; start Ollama using its normal local
   application/service; run `ollama pull bge-m3`. Verify
   `curl --fail http://localhost:11434/api/tags` lists bge-m3. On network, disk or
   model error, stop and report it. Model provisioning is separate from pip.
5. **Start:** run `.venv/bin/memory-service` in a dedicated terminal using the
   data environment above. Default address is 127.0.0.1:5370; `MEMSVC_PORT` can
   select a free port. On port conflict identify the existing process; do not
   kill it or treat its health as verification of this installation.
6. **Verify:** `curl --fail http://127.0.0.1:5370/api/health`; inspect embedding
   and DB fields, not just HTTP 200. Then run
   `.venv/bin/python examples/run.py decisions` (real model embedding, write,
   and retrieval in a temporary DB). Repeat for `debugging` and `cross-client`.
   Any failed assertion or unavailable model means installation verification is
   incomplete. `--offline` only checks storage and cannot certify model readiness.
7. **Connect:** add examples/client-instructions.md to the user's chosen
   assistant's project instruction file, preserving existing content. Register
   only a user-selected project (docs/API.md), with narrow watch targets.
8. **Report:** exact paths, Python, commands, health fields, example results,
   and client instruction destination. State unverified steps explicitly.

Optional authentication: set `MEMSVC_AUTH=1` and a nonempty secret
`MEMSVC_API_TOKEN` in the launching process; never commit it. Write routes
require it; set `MEMSVC_READ_REQUIRES_TOKEN=1` to protect reads too. The server
refuses to start with auth enabled but no token. This protocol never exposes the
service publicly.
Stop a foreground process with Ctrl-C. Uninstall by removing only the newly
created venv; preserve data unless its deletion is explicitly requested.
