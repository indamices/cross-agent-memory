# Examples

Run `python examples/run.py decisions`, `debugging`, or `cross-client` with the
installed dependencies and Ollama bge-m3. Each uses its own temporary DB, writes a
real document, retrieves it, asserts the result, and removes the DB. Add --offline
for deterministic storage-only verification without Ollama.

Use client-instructions.md to connect an assistant to the local HTTP API.
legacy-projects contains the original personal project configuration for historical
reference only: paths must be replaced and content reviewed before opting in.
