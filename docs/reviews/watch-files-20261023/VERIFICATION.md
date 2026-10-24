# Single-file watcher and capture verification

Projects using only `watch_files` were indexed at startup but subsequent edits were not watched. `indexer.py` now watches existing file parents, filters configured files, handles atomic editor saves, and preserves configured source paths. Directory watching remains recursive. A project with no existing watch directory or file parent reports a watcher error.

`server.py` now balances the initialization counter even when watcher startup fails. An independent two-project test first reproduced `-1 == 0` in both related repositories; after the fix the bad project is isolated, the good project initializes once, and the counter returns to zero.

The capture instructions distinguish selected Markdown files from an explicitly connected MCP client. File updates retain the default one-hour batch window. MCP callers must check errors/indexed status and project-scoped readback; `api://mcp` identifies the capture route, not original-document provenance. Installing the service alone does not connect clients or ensure memory adoption.

## Validation

GLM-5.3 and GPT-6.1-Sol each personally ran the pinned final isolated commands. DeepSeek V4.1 Flash reviewed the materials and did not claim execution. The linked model reports contain harness metadata followed by verbatim original and incremental opinions.

- 14 watcher tests, with real Observer writes and atomic replacement.
- 50 package regressions, with scratch-only paths; backup test checks ten actual bound resource paths, preserving backup/restore/traversal assertions.
- 1 separate two-project initialization test. Root-level test modules require independent processes.
- `bash -n install.sh` and `./install.sh --dry-run`; no installation performed.
- A disposable local harness verified watch → real Ollama `bge-m3` → SQLite → retrieval of the updated version. Its batch window was 0.2 seconds; production default remains 3600 seconds.
- Existing decisions, debugging and cross-client examples passed real embedding write/search round trips.

Tests were committed before the corresponding implementation. Original public watcher baseline was 11 failed/1 passed; two later empty-target checks each failed on original main; the project-init counter test also failed before its fix. The corrected backup fixture changes isolation, not functional assertions.

## Boundaries and record corrections

This does not certify full MCP client setup, automatic memory adoption, universal semantic recall, runtime symlink rebinding, duplicate aliases, deletion cleanup, or future creation of missing watch directories. Future creation of a selected file is supported when its watched parent already exists. SQLite capture success does not certify the best-effort snapshot copy.

Two editorial errors in the incremental DS report are corrected here without altering its original text: the failing project constructs an Observer object but never schedules/starts it; five prior files are unchanged, with two new/changed files added to this public candidate. The independent native runs satisfy the conditional execution requirements in the material reviews.

At review time this public checkout was a local branch; its original main was preserved. No push, PR, installation or service deployment of this checkout was performed. Only these relative-path review records are intended for public copy; private review packets and raw runtime records are excluded.

[GLM report](./glm.md) · [DeepSeek report](./ds.md) · [Sol report](./sol.md)
