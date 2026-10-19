#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mode="${1:-install}"
case "$mode" in install|--dry-run) ;; *) echo 'Usage: ./install.sh [--dry-run]' >&2; exit 2;; esac
case "$(uname -s)" in Darwin|Linux) ;; *) echo 'macOS or Linux required' >&2; exit 1;; esac
py="${PYTHON:-python3}"
"$py" -c 'import sys,venv,sqlite3; assert sys.version_info >= (3,10), "Python 3.10+ required"; print("Python",sys.version.split()[0],"SQLite",sqlite3.sqlite_version)'
if [ "$mode" = --dry-run ]; then
  echo 'DRY-RUN: create/reuse .venv; pip install .; check sqlite-vec + FTS5; initialize isolated data dir'
  echo 'DRY-RUN: no files created, no service started, no model downloaded'
  exit 0
fi
if [ ! -x .venv/bin/python ]; then
  if ! "$py" -m venv .venv; then
    echo 'note: full venv creation failed (broken ensurepip) — creating pip-less venv'
    "$py" -m venv --without-pip .venv
  fi
fi
# Some Python builds ship a broken ensurepip (venv exists but has no pip).
# Fall back to driving the parent interpreter's pip into the venv.
if .venv/bin/python -m pip --version >/dev/null 2>&1; then
  .venv/bin/python -m pip install -q --disable-pip-version-check .
else
  echo 'note: venv pip missing (broken ensurepip) — bootstrapping via parent pip'
  "$py" -m venv --without-pip .venv
  "$py" -m pip install -q --disable-pip-version-check --python .venv .
fi
export MEMSVC_DATA_DIR="${MEMSVC_DATA_DIR:-$PWD/.local-data}"
export MEMSVC_DB="${MEMSVC_DB:-$MEMSVC_DATA_DIR/memory.db}"
.venv/bin/python - <<'PY'
import sqlite3, sqlite_vec
with sqlite3.connect(':memory:') as c:
    c.enable_load_extension(True)
    sqlite_vec.load(c)
    c.execute("create virtual table probe using fts5(text, tokenize='trigram')")
    print('sqlite-vec', c.execute('select vec_version()').fetchone()[0], 'FTS5 OK')
import db
probe_a = db.get_or_create_collection("install-probe")
probe_b = db.get_or_create_collection("install-probe")
assert probe_a == probe_b, "bootstrap not idempotent"
print("schema bootstrap OK (idempotent)")
PY
printf 'Installation complete. Start:\nMEMSVC_DATA_DIR=%q .venv/bin/memory-service\n' "$MEMSVC_DATA_DIR"
echo 'Next: follow INSTALL-FOR-AI.md for Ollama, health and real write/search verification.'
