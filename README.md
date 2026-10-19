<h1 align="center">Cross-Agent Memory</h1>

<p align="center">
  <b>One local memory for all your AI coding agents.</b><br>
  Your Claude Code history, Codex sessions, Cursor work, project docs and decisions —
  indexed once, searchable from every agent, forever yours.
</p>

<p align="center">
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
  <img alt="Python 3.10+" src="https://img.shields.io/badge/python-3.10%2B-blue.svg">
  <a href="https://github.com/indamices/cross-agent-memory/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/indamices/cross-agent-memory/actions/workflows/ci.yml/badge.svg"></a>
  <img alt="Local-first" src="https://img.shields.io/badge/cloud-none%20%C2%B7%20data%20stays%20local-2ea44f.svg">
</p>

**English** | [中文](README.zh-CN.md)

<p align="center">
  <img src="docs/assets/demo.gif" alt="Terminal demo: install, verify, store and search memory" width="860">
</p>
<p align="center"><i>Real run: install → verify → store → search. No mock-ups.</i></p>

---

## The problem

Every AI coding tool starts from zero.

You spend a week with Claude Code designing an architecture, switch to Codex next
week, and it knows nothing about what you decided or why. You paste context again.
You re-explain the project. Three tools later, the same conversation has happened
five times, and none of it is searchable.

**cross-agent-memory fixes this.** It runs a small local memory service that:

1. **Ingests** your session history from any AI tool (Claude Code / Codex-style
   CLIs / OpenCode / ZCode / …) and any project folder you point it at.
2. **Indexes** everything with local embeddings into a single searchable store.
3. **Serves** it back to every agent through an HTTP API or directly as
   **MCP tools** — so any MCP-capable agent can recall what every other agent
   (and you) already figured out.

No cloud. No accounts. Nothing ever leaves your machine.

## Features

- **🔒 Local-first, private by design** — embeddings and search run on your
  machine (Ollama + SQLite + sqlite-vec). Airplane mode is a supported mode.
- **🧠 Cross-tool by design** — connectors for Claude Code, OpenCode and ZCode
  session stores, plus a generic file watcher for any project/docs folder.
  Agents share *one* memory regardless of which tool produced it.
- **🔌 One-line MCP integration** — `memory_search`, `memory_write` and
  `memory_stats` are exposed over the Model Context Protocol. Claude Code,
  Codex CLI, Cursor and anything else MCP-capable can use them today.
- **⚡ Fast at real scale** — built for large corpora: sqlite-vec vec0 KNN with a
  **1-bit coarse recall** prefilter and **hot/warm/cold tiering**. The reference
  deployment searches **2,000,000+ chunks in 0.36–1.20 s** (see
  [measured numbers](#measured-numbers)).
- **🛠 Ops included** — health/monitor endpoints, rolling-window degradation
  detection, backups & restore, snapshot archiving, tier maintenance scripts.
- **📈 Battle-tested** — extracted from a continuously running multi-agent
  system where four different AI tools share one memory daily, with millions of
  chunks and daily incremental indexing.

## Architecture

```
        Claude Code      Codex-style CLI     OpenCode / ZCode      any project
             │                  │                  │                  │
             ▼                  ▼                  ▼                  ▼
      ┌────────────────────────────────────────────────────────────────────┐
      │                         Connectors / Indexers                      │
      │   incremental watermarks · content-hash dedup · active-session     │
      │   skip · unified chunker · local embeddings (Ollama, e.g. bge-m3)  │
      └────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
      ┌────────────────────────────────────────────────────────────────────┐
      │                      SQLite memory store (single file)             │
      │   chunks (source of truth)  ├─ vec0 KNN index (sqlite-vec)         │
      │   FTS-ready metadata        ├─ 1-bit recall codes (Hamming)        │
      │                             └─ hot / warm / cold tier tables       │
      └────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴───────────────────┐
                    ▼                                     ▼
        HTTP API (port 5370)                    MCP server (stdio)
        /api/search /api/memories               memory_search
        /api/stats  /api/health ...             memory_write / memory_stats
                    ▲                                     ▲
                    └─────────────────┬───────────────────┘
                                      │
                    every agent you use, in any language/tool
```

Design notes and the full retrieval pipeline are documented in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## 🤖 Let your AI agent install it (one paste)

The fastest path: open this repository in your AI coding assistant and paste
the block below. It installs everything locally — venv, dependencies, a fresh
isolated database — verifies a real write/search round trip, and reports back
with exact commands and results. Nothing is sent anywhere; the agent works on
your machine only.

```text
Install this repository locally. Read AGENTS.md and INSTALL-FOR-AI.md first.
Use ./install.sh --dry-run, then ./install.sh. Keep all runtime data isolated
from existing databases and bind only 127.0.0.1. Verify initialization twice,
Ollama bge-m3, health, and a real write/search round trip using examples/run.py.
Add the client instructions from examples/client-instructions.md. Report exact
commands, results and blockers; do not claim success from health alone.
Do not import personal projects, restore backups or change system services.
```

Prefer doing it yourself? The manual path is right below. The same protocol
is documented for agents in [INSTALL-FOR-AI.md](INSTALL-FOR-AI.md) and
[AGENTS.md](AGENTS.md).

## Quickstart

### 0. Prerequisites

- **macOS or Linux** (Windows is not supported yet — connectors use POSIX
  file locks)
- Python **3.10+**
- [Ollama](https://ollama.com) running locally (for embeddings)

```bash
ollama pull bge-m3            # ~1.2 GB, good multilingual embedding model
```

### 1. Install

```bash
git clone https://github.com/indamices/cross-agent-memory.git
cd cross-agent-memory
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. Start the service

```bash
python3 server.py
# Server listening on 127.0.0.1:5370
```

### 3. Give it something to remember

Register any project folder (docs, decisions, notes):

```bash
curl -s -X POST http://127.0.0.1:5370/api/register \
  -H 'Content-Type: application/json' \
  -d '{"root": "/path/to/your/project"}'
```

> The folder needs a `.memory-service.yaml` or sensible defaults are used —
> see [examples/](examples/). Files are indexed immediately; later changes are
> batched by a debounce window (default **1 hour**, `MEMSVC_WATCH_DEBOUNCE`).
> For instant pickup: `POST /api/reindex {"project": "..."}` or start with
> `MEMSVC_WATCH_DEBOUNCE=0`.

Or ingest AI-tool session history with a connector:

```bash
python3 connectors/index_claude_code.py     # ~/.claude/projects/**/*.jsonl
python3 connectors/index_opencode.py        # ~/.local/share/opencode/opencode.db
python3 connectors/index_zcode.py           # ~/.zcode/v2/tasks-index.sqlite
```

### 4. Search from anywhere

```bash
curl -s -X POST http://127.0.0.1:5370/api/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "why did we choose the tiered storage design", "n_results": 5}'
```

Or write a memory directly (decisions, insights — great for agent hooks):

```bash
curl -s -X POST http://127.0.0.1:5370/api/memories \
  -H 'Content-Type: application/json' \
  -d '{"items": [{"type": "decision", "content": "We picked sqlite-vec over a server-based vector DB for local-first operation."}]}'
```

## Connect your agents (MCP)

`mcp_server.py` is a zero-dependency MCP (stdio) server that exposes the
memory API as tools. Point your agent at it:

**Claude Code**

```bash
claude mcp add memory -- python3 /ABS/PATH/cross-agent-memory/mcp_server.py
```

or in `.mcp.json`:

```json
{
  "mcpServers": {
    "memory": {
      "command": "python3",
      "args": ["/ABS/PATH/cross-agent-memory/mcp_server.py"],
      "env": { "MEMORY_SERVICE_URL": "http://127.0.0.1:5370" }
    }
  }
}
```

**Codex CLI** (`~/.codex/config.toml`)

```toml
[mcp_servers.memory]
command = "python3"
args = ["/ABS/PATH/cross-agent-memory/mcp_server.py"]
env = { MEMORY_SERVICE_URL = "http://127.0.0.1:5370" }
```

**Cursor / other MCP agents** — same server command and args in that tool's MCP
settings. If you started the service with `MEMSVC_TOKEN` set, also set
`MEMORY_SERVICE_TOKEN` for the MCP server.

Once connected, your agent gets:

| Tool | What it does |
|------|--------------|
| `memory_search` | Semantic search across **all** indexed sessions/projects of every tool |
| `memory_write`  | Store a decision / insight / discovery for future sessions |
| `memory_stats`  | List collections and sizes |

## How retrieval works

Each `/api/search` request runs a staged pipeline:

1. **Query embedding** — computed locally (Ollama), cached by text hash.
2. **Coarse recall** — a 1-bit-per-dimension code per chunk is prefiltered by
   Hamming distance (skipped automatically for small collections).
3. **Exact KNN** — sqlite-vec `vec0` cosine KNN over the shortlist
   (falls back to a numpy brute-force path when sqlite-vec is unavailable).
4. **Re-ranking** — time decay (30-day half-life) + MMR diversity + a small
   `helpful` feedback factor.
5. **Result** — text + source + distance + score per hit.

Under the hood it also handles the boring-but-critical parts: content-hash
deduplication, per-connector increment watermarks, skipping sessions that are
still being written, batch-write pacing, WAL tuning, and stale-index self-healing.

## Measured numbers

From the reference deployment (a Mac mini M4 Pro running this service 24/7 for
a multi-agent setup):

| Metric | Value |
|---|---|
| Indexed chunks | **2,013,030** across 16 collections |
| Tools sharing one memory | 4 (Claude Code, OpenCode, ZCode, OpenClaw) |
| Sampled `/api/search` latency | **0.36 s – 1.20 s** (5 production queries, warm cache) |
| Embedding model | `bge-m3` (1024-dim), local Ollama |
| Storage | single SQLite file + derived vec0 index (rebuildable at any time) |

These are real numbers from a heavy, messy, constantly-growing corpus — not a
toy benchmark. Your numbers will vary with corpus size and hardware; on a fresh
database everything is sub-100 ms.

## API overview

| Endpoint | Method | Description |
|---|---|---|
| `/api/ping` | GET | Liveness (no DB access) |
| `/api/health` | GET | Health incl. embedding status, disk, degraded reasons |
| `/api/stats` | GET | Collections and chunk counts |
| `/api/projects` | GET | Configured projects |
| `/api/monitor` | GET | Latency/cache/disk metrics |
| `/api/search` | POST | Semantic search (`query`, `n_results?`, `project?`) |
| `/api/memories` | POST | Directly capture memory items |
| `/api/feedback` | POST | Mark a chunk `helpful`/`unhelpful` (influences ranking) |
| `/api/register` | POST | Register a project directory for indexing |
| `/api/reindex` | POST | Force re-index (single project or all) |
| `/api/backup` / `/api/restore` | POST | Backup / restore the database |
| `/api/archive-snapshots` | POST | Compress old snapshot files |

Full reference with request/response examples: [docs/API.md](docs/API.md).

## Configuration

Everything works with defaults; tune via environment variables when needed:

| Variable | Default | Purpose |
|---|---|---|
| `MEMSVC_PORT` | `5370` | HTTP port |
| `MEMSVC_DB` | `./memory.db` | SQLite database path |
| `MEMSVC_SEARCH_MODE` | `vec` | `vec` (sqlite-vec) or `legacy` (numpy) |
| `MEMSVC_BIT_RECALL` | `on` | 1-bit coarse recall prefilter (`off` to disable) |
| `MEMSVC_MAX_CONCURRENCY` | `8` | HTTP worker cap (503 fast-fail beyond) |
| `MEMSVC_STALL_WATCHDOG` | `on` | Dump stacks when a search stalls |
| `MEMSVC_AUTH` | *(unset)* | `1` enables bearer-token auth (writes require `Authorization: Bearer <token>`) |
| `MEMSVC_API_TOKEN` | *(unset)* | Bearer secret; setting it also enables auth |
| `MEMSVC_READ_REQUIRES_TOKEN` | *(unset)* | `1` also requires the token for reads |
| `MEMSVC_BACKUP_DIR` | `backups` | Database backup directory |
| `MEMSVC_SLOW_THRESHOLD_MS` | `8000` | Slow-query threshold for health |
| `MEMORY_MISS_LEDGER` | *(disabled)* | JSONL of zero/low-quality results (write-back candidates) |
| `MEMORY_HITS_LEDGER` | *(disabled)* | JSONL sampling of top hits per query |
| `CLAUDE_PROJECTS_DIR` | `~/.claude/projects` | Claude Code connector source |
| `OPENCODE_DB` / `OPENCODE_DBS` | `~/.local/share/opencode/…` | OpenCode connector source |
| `ZCODE_DB` | `~/.zcode/v2/tasks-index.sqlite` | ZCode connector source |
| `MEMORY_SERVICE_URL` | `http://127.0.0.1:5370` | Used by the MCP server |
| `MEMORY_SERVICE_TOKEN` | *(unset)* | Bearer token if auth is enabled |

## Operations

- **Health**: `GET /api/health` reports `status`, disk usage, embedding
  availability and *degraded reasons* (e.g. slow queries, disk pressure) —
  designed to be scraped by a cron/monitor without false alarms.
- **Backups**: `scripts/backup_silent.py` (cron-friendly) or
  `POST /api/backup`. Restore with `POST /api/restore`.
- **Tiering** (optional): `scripts/migrate_to_vec0.py` builds the vec0 index;
  `scripts/tier_rebuild.py` builds hot/warm/cold tier tables to keep the hot
  set small on very large corpora.
- **Snapshot archiving**: `scripts/archive_silent.py`.
- **Health cron with alerts**: `scripts/health_check.py` (optional Feishu
  webhook support is built in — configure `.alert.env`).

## Design choices (and non-goals)

- **SQLite, not a vector server.** One file, zero services, trivially
  backed up, works offline. The vec index is derived and rebuildable.
- **Local embeddings, not an API.** Privacy and zero marginal cost; swap the
  model in `config.py` if you prefer something else.
- **Files are the source of truth.** Everything indexed can be re-ingested from
  its source; deleting the database loses nothing that lives on disk.
- **Not a cloud sync product.** This is a local service for one machine
  (multiple agents, one memory). Multi-user/server mode is on the roadmap.
- **Not a vector database benchmark entry.** It prioritizes a small
  dependency surface and operability over raw ANN throughput.

## Roadmap

- [ ] Web console (browse / search / curate memories)
- [ ] More connectors: Codex CLI session store, Cursor, Aider, generic JSONL
- [ ] First-class memory types (facts / lessons / code-change context) with
      extraction pipelines
- [ ] Multi-machine sync for people who work across hardware
- [ ] Python package / Docker image for one-command installs

Issues and PRs are welcome — especially new connectors. See
[CONTRIBUTING.md](CONTRIBUTING.md).

## Acknowledgements

Built on the shoulders of excellent tools: [SQLite](https://sqlite.org),
[sqlite-vec](https://github.com/asg017/sqlite-vec),
[Ollama](https://ollama.com), [numpy](https://numpy.org),
[watchdog](https://github.com/gorakhargosh/watchdog).

## License

[MIT](LICENSE) © 2026 indamices
