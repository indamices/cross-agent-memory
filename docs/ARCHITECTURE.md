# Architecture

This document explains how cross-agent-memory is put together and why.
It is the "what actually happens when you call /api/search" reference.

## Components

| Component | File | Role |
|---|---|---|
| HTTP API | `server.py` | REST endpoints, request dispatch, bounded concurrency |
| Storage & retrieval | `db.py` | SQLite schema, vector search, tiering, dedup, caches |
| Watcher / file indexer | `indexer.py` | Directory watching, incremental file indexing, chunk upsert |
| Embeddings | `embedder.py` | Ollama (or local model) client, LRU cache, fallback |
| Chunking | `chunking.py` | Unified text chunker + hashes (single source of truth) |
| Monitoring | `monitor.py` | Metrics, rolling windows, stall watchdog, disk info |
| Backup | `backup.py` | DB backups, restore, snapshot archiving |
| MCP server | `mcp_server.py` | Zero-dependency stdio MCP bridge to the HTTP API |
| Connectors | `connectors/` | Per-tool session ingestion (Claude Code / OpenCode / ZCode) |

## Data model

Everything lives in one SQLite database (default `./memory.db`):

- **`chunks`** — the source of truth. One row per chunk:
  `id, collection, text, embedding (float32 blob), metadata (json),
  source, updated_at, text_sha256 (dedup)`.
- **`collections`** — collection registry (a collection per project / tool).
- **`vec_chunks`** — derived sqlite-vec `vec0` index
  (`chunk_id, embedding float[1024] cosine, collection, layer`).
  Rebuildable at any time from `chunks` (`scripts/migrate_to_vec0.py`).
- **Tier tables** (optional, large corpora): `vec_chunks_hot/_warm/_cold`
  plus `chunk_tier` / `tier_meta` registries, built by
  `scripts/tier_rebuild.py`. Rows move between tiers based on age and access
  (hot stays small; the storage layer supports splitting cold into a separate
  database file, though the split tooling is not included in this release).

Because vectors are derived and files on disk are the ultimate source,
`memory.db` is fully disposable: delete it and re-run the connectors to rebuild.

## Ingestion

Two paths feed the database:

**1. File watcher** (`indexer.py`)

- `POST /api/register` or a YAML in `projects/` registers a root directory.
- Files and session connectors both chunk at 800/100 chars (size/overlap);
  the `examples/` session-watcher configs demonstrate a 1200/200 setting for
  long transcripts.
- A watchdog observer batches changes in a debounce window (`MEMSVC_WATCH_DEBOUNCE`,
  default **3600 s**) and then re-indexes only changed files (mtime + content
  hash). Set it to `0` for immediate indexing, or trigger
  `POST /api/reindex` to pick changes up on demand.
- Deleted content is removed per-source, never globally.

**2. Session connectors** (`connectors/`)

- Each connector reads its tool's native store (JSONL files or SQLite db),
  extracts user/assistant text, chunks and embeds it.
- Incremental by **watermark file** (`.last_run_*`): only data newer than the
  last processed timestamp is considered; hot runs exit in seconds.
- Sessions modified within the last 5 minutes are skipped (still in progress)
  and reconsidered next run.
- Identical text (content hash) is deduplicated at write time.

All write paths go through `db.upsert_chunks`, which:

- batches writes (default ≤500 rows per batch, `MEMSVC_UPSERT_BATCH_MAX_ROWS`)
  and paces bursts (`MEMSVC_UPSERT_MIN_INTERVAL_MS`) to avoid starving reads;
- updates `chunks` + `vec_chunks` (+ tier tables when enabled) consistently;
- is idempotent: re-running an indexer over the same data is a no-op.

## Retrieval pipeline

`POST /api/search` (and every MCP `memory_search`) runs:

1. **Query embedding** — local model call, LRU-cached by text hash.
2. **Candidate generation**, one of:
   - single-collection `vec0` KNN (exact cosine over `vec_chunks`);
   - cross-collection partitioned scan (per-collection KNN, bounded);
   - **1-bit coarse recall** — each chunk's embedding has a 1-bit-per-dimension
     code kept in memory; candidates are prefiltered by Hamming distance, then
     re-ranked with exact vector similarity. Skipped for small collections.
3. **Re-ranking** — time decay (30-day half-life), MMR diversity
   (`lambda = 0.7`) and a small per-chunk feedback factor from
   `POST /api/feedback`.
4. **Optional quality gates** — minimum-distance filtering and layered
   admission (curated "gold" layers rank first when enabled).

Fallbacks are explicit: if sqlite-vec is unavailable or the vec table is empty,
a numpy brute-force path serves the same API (`MEMSVC_SEARCH_MODE=legacy`
forces it). Degradations are counted and surfaced in `/api/health`.

## Tiering (optional, for large corpora)

When the corpus grows into millions of chunks, keeping every vector in the hot
index is wasteful. Tiering moves rows between three physical tables:

- **hot** — recent/active content; the only set searched by default.
- **warm** — older but still relevant (default ≤30 days).
- **cold** — old, rarely accessed; can be split into a separate database file.

Age is computed from the source timestamp (`metadata.file_mtime`, falling back
to `updated_at`). Protection rules keep curated/gold content hot. A cooldown
scheduler demotes rows over time; frequently returned chunks can be promoted
back (`_promote_cold_to_hot`) with a promotion ledger for auditability.

## Concurrency & durability

- **Single writer discipline** — one service process writes the database.
  Indexer processes (connectors) coordinate via lazy locks and idempotent
  upserts; readers never block on writers thanks to WAL.
- **WAL tuning** — `synchronous=NORMAL`, bounded `journal_size_limit`,
  larger `wal_autocheckpoint`: fewer fsyncs under write bursts, bounded WAL
  growth. See `_get_conn()` in `db.py`.
- **Graceful shutdown** — SIGTERM/SIGINT handlers flush and close cleanly
  (prevents SQLite corruption on restarts).
- **Backups** — `scripts/backup_silent.py` is the cron-friendly path; backups
  are plain file copies (plus WAL checkpoint) into `backups/<timestamp>/`.

## Observability

- `/api/health` — embedding status, disk usage, and *degraded reasons*
  (slow-query ratio computed over a rolling window with minimum sample size, so
  brief hiccups self-heal without restarting anything).
- `/api/monitor` — request latency, cache hit rates, bounded-server rejections.
- **Slow query attribution** — searches are timed in segments (candidate fetch,
  vector scan, merge) and emitted to logs/monitor with the phase breakdown.
- **Stall watchdog** — if an in-flight search stalls beyond a threshold
  (`MEMSVC_STALL_STACK_SECS`), all Python thread stacks are dumped for
  post-mortem debugging (`MEMSVC_STALL_DUMP_DIR`).
- **Search quality ledgers** (opt-in) — `MEMORY_MISS_LEDGER` records queries
  that returned nothing useful (write-back candidates); `MEMORY_HITS_LEDGER`
  samples top hits. Both are append-only JSONL and fail-silent by design.

## Security model

- Binds to `127.0.0.1` by default.
- Optional bearer-token auth via env: `MEMSVC_AUTH=1` + `MEMSVC_API_TOKEN`
  protect write endpoints; `MEMSVC_READ_REQUIRES_TOKEN=1` also protects reads.
- `POST /api/restore` additionally validates the backup name against the
  `YYYY-MM-DD_HHMMSS` format and confines it to the backup root (no path
  traversal).
- No telemetry, no external calls except the local Ollama endpoint (the sole
  opt-in exception is the alert webhook in `scripts/health_check.py`, which is
  disabled unless `.alert.env` is configured). The
  embedder explicitly bypasses HTTP proxies so local calls can never leak
  through a system proxy.

## Extension points

- **New connectors** — copy a file in `connectors/`, read your tool's session
  store, emit `{id, text, embedding, metadata}` dicts to `db.upsert_chunks`.
  Keep a watermark for incrementality.
- **New chunkers** — everything goes through `chunking.chunk_text`; the
  `pilot.yaml` mechanism lets you trial a chunker/dedup variant on one project
  before rolling it out.
- **Different embedding backends** — `embedder.py` supports Ollama and local
  sentence-transformers; keep vectors consistent per database (dimension is
  fixed in the vec0 declaration).
