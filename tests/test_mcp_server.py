#!/usr/bin/env python3
"""Hermetic test for mcp_server.py: speaks the MCP stdio protocol against a
stub memory-service HTTP endpoint. No real service, no Ollama, no network
beyond localhost.

Run: python3 tests/test_mcp_server.py
"""

import json
import subprocess
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

STUB_RESULTS = {
    "/api/search": {
        "results": [
            {
                "id": "abc123",
                "collection": "demo",
                "text": "We chose SQLite for local-first storage.",
                "distance": 0.21,
                "score": 0.8,
                "metadata": {"source": "docs/decisions.md"},
            }
        ]
    },
    "/api/stats": {
        "collections": [
            {"name": "demo", "count": 42},
            {"name": "sessions", "count": 7},
        ]
    },
    "/api/memories": {"status": "ok", "accepted": 1, "total": 1,
                      "items": [{"chunk_id": "x1", "indexed": True, "error": None}]},
}


class _StubHandler(BaseHTTPRequestHandler):
    def _respond(self):
        body = STUB_RESULTS.get(self.path, {"error": "not found"})
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = _respond
    do_POST = _respond

    def log_message(self, *args):
        pass


class TestMcpServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _StubHandler)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def _run_session(self, messages):
        import os
        env = dict(os.environ)
        env["MEMORY_SERVICE_URL"] = f"http://127.0.0.1:{self.port}"
        proc = subprocess.run(
            [sys.executable, str(REPO / "mcp_server.py")],
            input="\n".join(json.dumps(m) for m in messages) + "\n",
            capture_output=True, text=True, timeout=60, env=env, cwd=str(REPO),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr[-1000:])
        return [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]

    def test_full_handshake_and_tools(self):
        out = self._run_session([
            {"jsonrpc": "2.0", "id": 1, "method": "initialize",
             "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                        "clientInfo": {"name": "t", "version": "0"}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
             "params": {"name": "memory_search", "arguments": {"query": "storage"}}},
            {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
             "params": {"name": "memory_write", "arguments": {"content": "note"}}},
            {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
             "params": {"name": "memory_stats", "arguments": {}}},
        ])
        by_id = {m.get("id"): m for m in out if "id" in m}

        self.assertEqual(by_id[1]["result"]["serverInfo"]["name"], "cross-agent-memory")
        tools = {t["name"] for t in by_id[2]["result"]["tools"]}
        self.assertEqual(tools, {"memory_search", "memory_write", "memory_stats"})

        search_text = by_id[3]["result"]["content"][0]["text"]
        self.assertIn("SQLite", search_text)
        self.assertFalse(by_id[3]["result"]["isError"])

        self.assertFalse(by_id[4]["result"]["isError"])
        self.assertIn("ok", by_id[4]["result"]["content"][0]["text"])

        stats_text = by_id[5]["result"]["content"][0]["text"]
        self.assertIn("demo", stats_text)
        self.assertIn("49", stats_text)  # 42 + 7

    def test_unknown_tool_is_reported_in_band(self):
        out = self._run_session([
            {"jsonrpc": "2.0", "id": 1, "method": "initialize",
             "params": {"protocolVersion": "2025-06-18"}},
            {"jsonrpc": "2.0", "id": 9, "method": "tools/call",
             "params": {"name": "nope", "arguments": {}}},
        ])
        by_id = {m.get("id"): m for m in out if "id" in m}
        self.assertTrue(by_id[9]["result"]["isError"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
