# API reference

Base URL: `http://127.0.0.1:5370` (configurable via `MEMSVC_PORT`).
All responses are JSON.

**Auth**: start the server with `MEMSVC_AUTH=1` and `MEMSVC_API_TOKEN=<secret>`
to require `Authorization: Bearer <secret>` on write endpoints (add
`MEMSVC_READ_REQUIRES_TOKEN=1` to also protect reads).

---

## GET /api/ping

Liveness probe. Does not touch the database.

```bash
curl -s http://127.0.0.1:5370/api/ping
```

```json
{"pong": true, "time": 1792372275.4}
```

## GET /api/health

Health check for monitoring. Includes embedding availability, disk usage and
structured `degraded_reasons` (empty when healthy).

```bash
curl -s http://127.0.0.1:5370/api/health
```

```json
{
  "status": "ok",
  "degraded_reasons": [],
  "ollama": true,
  "disk": {"used_pct": 61.2, "free_gb": 182.0},
  "uptime_seconds": 4288.7,
  "recent_latency_ms": {"search_avg": 812.3, "search_max": 1927.5},
  "projects": ["my-project"],
  "watchers": 1
}
```

Key fields:

- `status`: `ok` | `degraded` | `error`
- `degraded_reasons[]`: `{"type": "slow_query"|"disk", "detail": "..."}` —
  slow-query judgment uses a rolling window with a minimum sample size, so it
  self-heals without restarts.

## GET /api/stats

Collections and chunk counts.

```bash
curl -s http://127.0.0.1:5370/api/stats
```

```json
{
  "total_collections": 2,
  "collections": [
    {"name": "my_project", "count": 512},
    {"name": "claude_code_sessions", "count": 145525}
  ]
}
```

## GET /api/projects

Configured projects (watched directories).

## GET /api/monitor

Runtime metrics: request counts/latency, embedding cache stats, disk, bounded
server rejections.

---

## POST /api/search

Semantic search. Body:

| Field | Type | Required | Description |
|---|---|---|---|
| `query` | string | ✔ | Natural-language query |
| `n_results` | int | – | Number of results (default 5) |
| `project` | string | – | Restrict to a collection |
| `min_distance` | float | – | Drop results further than this distance |

```bash
curl -s -X POST http://127.0.0.1:5370/api/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "why tiered storage", "n_results": 3}'
```

```json
{
  "results": [
    {
      "id": "1e4702551765cbc8",
      "collection": "my_project",
      "text": "# Retrieval pipeline design ...",
      "metadata": {"source": "/path/to/docs/architecture.md", "layer": "doc"},
      "distance": 0.4038,
      "time_decay": 1.0,
      "score": 0.41,
      "mmr_rank": 0
    }
  ]
}
```

- `distance`: cosine distance (lower = closer). Typical: `<0.25` strong,
  `0.25–0.38` usable, `>0.38` mostly noise.
- `score`: final ranking score (distance combined with time decay, MMR,
  feedback).

## POST /api/memories

Directly capture memory items (no source file needed).

```bash
curl -s -X POST http://127.0.0.1:5370/api/memories \
  -H 'Content-Type: application/json' \
  -d '{
    "items": [
      {
        "type": "decision",
        "content": "Use sqlite-vec over a server vector DB for local-first operation.",
        "project": "my-project",
        "source": "session-end",
        "tags": ["architecture"]
      }
    ]
  }'
```

```json
{
  "status": "ok",
  "accepted": 1,
  "total": 1,
  "items": [{"chunk_id": "80b7395bbf4ad90e", "indexed": true, "error": null}]
}
```

`type` is one of `decision | discovery | insight | error` (free-form is fine).

## POST /api/feedback

Adjust ranking for a chunk.

```bash
curl -s -X POST http://127.0.0.1:5370/api/feedback \
  -H 'Content-Type: application/json' \
  -d '{"id": "80b7395bbf4ad90e", "helpful": true}'
```

## POST /api/register

Register a project directory (immediate index + live watcher).

```bash
curl -s -X POST http://127.0.0.1:5370/api/register \
  -H 'Content-Type: application/json' \
  -d '{"root": "/absolute/path/to/project"}'
```

Optional `.memory-service.yaml` in the project root:

```yaml
name: my-project
watch_files: [README.md, AGENTS.md]
watch_dirs: [docs, scripts]
exclude_patterns: ["*.pyc", "__pycache__"]
chunk_size: 800
chunk_overlap: 100
```

## POST /api/reindex

```bash
# one project
curl -s -X POST http://127.0.0.1:5370/api/reindex \
  -H 'Content-Type: application/json' -d '{"project": "my-project"}'

# everything (force = re-check all files)
curl -s -X POST http://127.0.0.1:5370/api/reindex \
  -H 'Content-Type: application/json' -d '{"force": true}'
```

## POST /api/backup and /api/restore

```bash
curl -s -X POST http://127.0.0.1:5370/api/backup
curl -s -X POST http://127.0.0.1:5370/api/restore \
  -H 'Content-Type: application/json' -d '{"backup": "2026-10-19_094000"}'
```

> Restore replaces the database file. Stop the service first to avoid mixing
> file handles.

## POST /api/archive-snapshots

Compress snapshot files older than N days (default: `backup.archive_days`,
30).

```bash
curl -s -X POST http://127.0.0.1:5370/api/archive-snapshots \
  -H 'Content-Type: application/json' -d '{"days": 30}'
```

---

## MCP tools

`mcp_server.py` (stdio) exposes:

| Tool | Arguments | Returns |
|---|---|---|
| `memory_search` | `query` (req), `n_results?`, `project?` | Formatted list of hits (source, score, text) |
| `memory_write` | `content` (req), `type?`, `project?`, `tags?` | Write confirmation + chunk id |
| `memory_stats` | – | Collection names + counts |

Configuration via env: `MEMORY_SERVICE_URL`, `MEMORY_SERVICE_TOKEN`,
`MEMORY_MCP_TIMEOUT`.
