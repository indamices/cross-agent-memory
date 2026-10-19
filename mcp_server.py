#!/usr/bin/env python3
"""Minimal zero-dependency MCP server for cross-agent-memory.

Speaks the Model Context Protocol (MCP) over stdio (newline-delimited
JSON-RPC 2.0) and exposes the memory-service HTTP API as MCP tools, so any
MCP-capable agent (Claude Code, Codex CLI, Cursor, ...) can search and write
shared memory with a one-line config.

Tools:
  - memory_search: semantic search across all indexed collections
  - memory_write:  directly capture a memory item (decision/insight/...)
  - memory_stats:  collection statistics

Config (env):
  MEMORY_SERVICE_URL    default http://127.0.0.1:5370
  MEMORY_SERVICE_TOKEN  optional bearer token (if the server enables auth)
  MEMORY_MCP_TIMEOUT    HTTP timeout seconds, default 30

Only the Python standard library is used.
"""

import json
import os
import sys
import urllib.error
import urllib.request

PROTOCOL_VERSION = "2025-06-18"
SERVER_NAME = "cross-agent-memory"
SERVER_VERSION = "1.0.0"

SERVICE_URL = os.environ.get("MEMORY_SERVICE_URL", "http://127.0.0.1:5370").rstrip("/")
SERVICE_TOKEN = os.environ.get("MEMORY_SERVICE_TOKEN", "")
HTTP_TIMEOUT = float(os.environ.get("MEMORY_MCP_TIMEOUT", "30"))

TOOLS = [
    {
        "name": "memory_search",
        "description": (
            "Search the shared memory across ALL agent tools (session logs of this "
            "and other AI coding tools, project docs, decisions, notes). Use it "
            "whenever you need past context: previous decisions, how a problem was "
            "solved before, architecture rationale, user preferences. Returns the "
            "most relevant text snippets with their source and relevance score."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural-language search query.",
                },
                "n_results": {
                    "type": "integer",
                    "description": "Number of results to return (default 8).",
                    "default": 8,
                },
                "project": {
                    "type": "string",
                    "description": (
                        "Optional collection/project to search within. Omit to "
                        "search everything."
                    ),
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "memory_write",
        "description": (
            "Store an item into shared memory so future sessions (in this tool or "
            "any other connected AI tool) can find it. Use it for durable facts: "
            "decisions made, discoveries, important insights, user preferences."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "The text to remember (self-contained, specific).",
                },
                "type": {
                    "type": "string",
                    "description": "Item kind: decision | discovery | insight | error.",
                    "default": "insight",
                },
                "project": {
                    "type": "string",
                    "description": "Optional project/collection name.",
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional tags for later filtering.",
                },
            },
            "required": ["content"],
        },
    },
    {
        "name": "memory_stats",
        "description": (
            "Get memory-service statistics: which collections exist and how many "
            "chunks each holds. Useful to see what has been indexed."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _request(method: str, path: str, payload: dict | None = None) -> dict:
    url = f"{SERVICE_URL}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", f"{SERVER_NAME}/{SERVER_VERSION} (mcp)")
    if SERVICE_TOKEN:
        req.add_header("Authorization", f"Bearer {SERVICE_TOKEN}")
    # The service is local by design; never route it through an HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=HTTP_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"HTTP {e.code} from memory-service: {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Cannot reach memory-service at {SERVICE_URL} ({e.reason}). "
            "Is the service running? (python3 server.py)"
        ) from e
    try:
        return json.loads(body)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON from memory-service: {body[:200]}") from e


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------


def tool_memory_search(args: dict) -> str:
    query = str(args.get("query") or "").strip()
    if not query:
        raise ValueError("query is required")
    n_results = int(args.get("n_results") or 8)
    payload = {"query": query, "n_results": n_results}
    project = args.get("project")
    if project:
        payload["project"] = str(project)
    resp = _request("POST", "/api/search", payload)
    results = resp.get("results") or []
    if not results:
        return f"No memories found for: {query}"
    lines = [f"Found {len(results)} result(s) for: {query}", ""]
    for i, r in enumerate(results, 1):
        meta = r.get("metadata") or {}
        source = meta.get("source") or r.get("collection") or "unknown"
        score = r.get("score")
        text = (r.get("text") or "").strip()
        if len(text) > 1200:
            text = text[:1200] + " …[truncated]"
        lines.append(f"[{i}] source: {source}  collection: {r.get('collection', '')}  score: {score}")
        lines.append(text)
        lines.append("")
    return "\n".join(lines)


def tool_memory_write(args: dict) -> str:
    content = str(args.get("content") or "").strip()
    if not content:
        raise ValueError("content is required")
    item = {
        "type": str(args.get("type") or "insight"),
        "content": content,
        "source": "mcp",
    }
    if args.get("project"):
        item["project"] = str(args["project"])
    if args.get("tags"):
        item["tags"] = [str(t) for t in args["tags"]]
    resp = _request("POST", "/api/memories", {"items": [item]})
    written = resp.get("results") or resp
    return f"memory_write ok: {json.dumps(written, ensure_ascii=False)[:500]}"


def tool_memory_stats(_args: dict) -> str:
    resp = _request("GET", "/api/stats")
    cols = resp.get("collections") or []
    if not cols:
        return "No collections found."
    total = sum(c.get("count", 0) for c in cols)
    lines = [f"Total chunks: {total}", ""]
    for c in sorted(cols, key=lambda x: -x.get("count", 0)):
        lines.append(f"{c.get('name', '?'):<28} {c.get('count', 0)}")
    return "\n".join(lines)


TOOL_IMPL = {
    "memory_search": tool_memory_search,
    "memory_write": tool_memory_write,
    "memory_stats": tool_memory_stats,
}


# ---------------------------------------------------------------------------
# JSON-RPC / MCP plumbing
# ---------------------------------------------------------------------------


def _send(msg: dict) -> None:
    sys.stdout.write(json.dumps(msg, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _result(req_id, result) -> None:
    _send({"jsonrpc": "2.0", "id": req_id, "result": result})


def _error(req_id, code: int, message: str) -> None:
    _send({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})


def _handle(req: dict) -> None:
    method = req.get("method")
    req_id = req.get("id")
    params = req.get("params") or {}

    if method == "initialize":
        _result(req_id, {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        })
    elif method in ("notifications/initialized", "initialized"):
        pass  # notification: no response
    elif method == "ping":
        _result(req_id, {})
    elif method == "tools/list":
        _result(req_id, {"tools": TOOLS})
    elif method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        impl = TOOL_IMPL.get(name)
        if impl is None:
            _result(req_id, {
                "content": [{"type": "text", "text": f"Unknown tool: {name}"}],
                "isError": True,
            })
            return
        try:
            text = impl(args)
            _result(req_id, {"content": [{"type": "text", "text": text}], "isError": False})
        except Exception as e:  # tool failure is reported in-band, not as a crash
            _result(req_id, {
                "content": [{"type": "text", "text": f"{type(e).__name__}: {e}"}],
                "isError": True,
            })
    elif method is not None and method.startswith("notifications/"):
        pass  # any other notification: ignore
    else:
        _error(req_id, -32601, f"Method not found: {method}")


def main() -> int:
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            _error(None, -32700, "Parse error")
            continue
        if not isinstance(req, dict):
            _error(None, -32600, "Invalid request")
            continue
        try:
            _handle(req)
        except Exception as e:  # last-resort guard: keep the stdio loop alive
            print(f"[mcp] internal error: {e}", file=sys.stderr)
            if req.get("id") is not None:
                _error(req.get("id"), -32603, f"Internal error: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
