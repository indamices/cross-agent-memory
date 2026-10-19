"""Silent snapshot archival script for memory-service.

Run from cron / a scheduler. Runs POST /api/archive-snapshots,
logs result to archive_cron.log. Completely silent.
"""

import json
import logging
import os
import sys
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add service dir to path so we can import config
SERVICE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SERVICE_DIR))

from config import SERVER  # noqa: E402

TOKEN = os.environ.get("MEMSVC_TOKEN", "").strip()

LOG_FILE = SERVICE_DIR / "logs" / "archive_cron.log"
API_URL = f"http://{SERVER.host}:{SERVER.port}/api/archive-snapshots"

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("archive-cron")


def main():
    if not SERVICE_DIR.exists():
        return

    try:
        body = json.dumps({"days": 30}).encode()
        req = Request(API_URL, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        if TOKEN:
            req.add_header("Authorization", f"Bearer {TOKEN}")
        with urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode())
            if data.get("archived", 0) > 0:
                log.info(
                    f"Archived {data['archived']} snapshots "
                    f"({data.get('freed_mb', 0)} MB freed)"
                )
            else:
                log.info("No snapshots to archive")
    except HTTPError as e:
        log.error(f"HTTP {e.code} from service: {e.read()[:200]!r} "
                  f"(401 = token required; set MEMSVC_TOKEN)")
    except URLError as e:
        log.error(f"Cannot connect to service: {e}")
    except Exception as e:
        log.error(f"Archive error: {e}")


if __name__ == "__main__":
    main()
