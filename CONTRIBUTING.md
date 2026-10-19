# Contributing

Thanks for considering a contribution! This project is a local-first memory
service for AI coding agents, and the highest-value contributions are new
connectors and battle reports from real corpora.

## Development setup

```bash
git clone https://github.com/indamices/cross-agent-memory.git
cd cross-agent-memory
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

Run the service:

```bash
python3 server.py
```

Run the hermetic test suite (no Ollama, no network needed):

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

Run an end-to-end smoke test (needs Ollama + `bge-m3`):

```bash
python3 server.py &
curl -s -X POST http://127.0.0.1:5370/api/register \
  -H 'Content-Type: application/json' -d '{"root": "/tmp/some/docs/dir"}'
curl -s -X POST http://127.0.0.1:5370/api/search \
  -H 'Content-Type: application/json' -d '{"query": "hello"}'
```

> Use a scratch database (`MEMSVC_DB=/tmp/cam-test.db`) when experimenting —
> never point tests at real data.

## Adding a connector

A connector is a small script that:

1. reads a tool's native session store (JSONL file, SQLite db, …);
2. extracts user/assistant text and chunks it via `chunking.chunk_text`;
3. embeds in batches (`embedder.embed_batch`) and calls
   `db.upsert_chunks(collection, chunks)` with
   `{id, text, embedding, metadata}` dicts;
4. keeps a **watermark file** (`.last_run_*`) with the largest processed
   timestamp, so re-runs are cheap no-ops;
5. exits non-zero when ingestion errors occurred (fail loud, don't be silent).

Look at `connectors/index_claude_code.py` for a complete example. Register the
connector in `docs/ARCHITECTURE.md` (Components table).

## Guidelines

- **No data leaves the machine.** Any network call that isn't `localhost`
  (or clearly documented, e.g. optional alert webhooks) will be rejected in
  review.
- **Fail loud, degrade gracefully.** Distinguish "no data" from "failure";
  return non-zero exit codes for partial failures.
- **Idempotent writes.** Re-running any indexer over the same data must be a
  no-op. Use content hashes and watermarks.
- **Chinese or English comments are both fine.** Explain *why*, not *what*.
- **Keep dependencies small.** Stdlib first, then the four core deps. New
  dependencies need a strong reason.

## Pull requests

1. Fork, branch, make focused changes.
2. Add or update tests for behavior changes (hermetic preferred).
3. Make sure `python -m compileall` and the unittest suite pass.
4. Describe **what** changed and **why** in the PR. If your change affects
   retrieval quality, include a before/after example.

## Reporting issues

Great bug reports include: what you ran, what happened, what you expected, and
(if relevant) `/api/health` output and the service log. For retrieval-quality
issues, include the query and the top results you got vs. expected.
