"""Silent backup script for memory-service.

Called by Windows scheduled task. Runs POST /api/backup,
logs result to backup_cron.log. Completely silent — no console output.
"""

import json
import logging
import sys
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

# Add service dir to path so we can import config
SERVICE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SERVICE_DIR))

from config import SERVER  # noqa: E402

LOG_FILE = SERVICE_DIR / "logs" / "backup_cron.log"
API_URL = f"http://{SERVER.host}:{SERVER.port}/api/backup"

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("backup-cron")


def main():
    if not SERVICE_DIR.exists():
        return

    try:
        req = Request(API_URL, data=b"{}", method="POST")
        req.add_header("Content-Type", "application/json")
        with urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())
            if data.get("status") == "ok":
                log.info(
                    f"Backup OK: {data.get('backup_name')} "
                    f"({data.get('files')} files, {data.get('size_mb')} MB)"
                )
            else:
                log.error(f"Backup FAILED: {data.get('error')}")
    except URLError as e:
        log.error(f"Cannot connect to service: {e}")
    except Exception as e:
        log.error(f"Backup error: {e}")


if __name__ == "__main__":
    main()
