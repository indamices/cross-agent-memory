"""Three disposable write/search examples. Real Ollama by default; --offline for storage only."""
import argparse
import os
from pathlib import Path
import sys
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument("case", choices=["decisions", "debugging", "cross-client"])
parser.add_argument("--offline", action="store_true")
args = parser.parse_args()
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
with tempfile.TemporaryDirectory(prefix="memsvc-example-") as work:
    # Set every resource before importing runtime modules. Never touch service DB.
    for key, suffix in {"MEMSVC_DATA_DIR": "", "MEMSVC_DB": "memory.db",
                        "MEMSVC_COLD_DB": "cold.db", "MEMSVC_PROJECTS_DIR": "projects",
                        "MEMSVC_BACKUP_DIR": "backups", "MEMSVC_SNAPSHOT_DIR": "snapshots"}.items():
        os.environ[key] = str(Path(work) / suffix)
    import db
    db.get_or_create_collection(args.case)  # first connection bootstraps a fresh schema
    text = (Path(__file__).parent / args.case / "memory.md").read_text()
    if args.offline:
        vector = [1.0] + [0.0] * 1023
    else:
        from embedder import embed_single
        vector = embed_single(text)
        if not vector:
            raise RuntimeError("Ollama embedding unavailable")
    coll = db.get_or_create_collection(args.case)
    db.upsert_chunks(coll, [{"id": args.case, "text": text, "embedding": vector,
                            "metadata": {"source": "memory.md"}}])
    hits = db.search(coll, text, vector, n_results=1)
    assert hits and hits[0]["id"] == args.case, hits
    print(f"PASS {args.case}: {'offline storage' if args.offline else 'real embedding'} write/search round trip")
