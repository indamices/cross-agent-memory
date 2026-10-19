# Roadmap

Current release: **v0.1** — local-first shared memory over HTTP + MCP.

## Next up

- [ ] **Web console** — browse, search and curate memories in the browser
- [ ] **More connectors** — Codex CLI session store, Cursor, Aider, generic JSONL
- [ ] **First-class memory types** — facts / lessons / code-change context, with
      extraction pipelines
- [ ] **Multi-machine sync** — for people working across several computers
- [ ] **One-command installs** — PyPI package + Docker image
- [ ] **Windows support** — connectors currently rely on POSIX file locks

## Later / exploring

- Team mode (shared server, access control)
- Retrieval-quality benchmarks on public corpora
- Automatic memory consolidation (dedupe, conflict resolution, decay)

## How to influence the roadmap

Open an issue or start a discussion: real-world usage reports (corpus size,
latency, connector requests) weigh the most. Pull requests are welcome —
especially new connectors. See [CONTRIBUTING.md](../CONTRIBUTING.md).
