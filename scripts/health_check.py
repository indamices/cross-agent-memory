"""Health check + Feishu alert for memory-service.

Run from cron / a scheduler every 5 minutes.
- If health is OK → do nothing (completely silent)
- If health is degraded or error → send Feishu card message
- If service is unreachable → send Feishu card message
- Rate-limited: at most 1 alert per 30 minutes to avoid spam
- Retries: up to 2 attempts on alert failure
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

# Add service dir to path so we can import config
SERVICE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SERVICE_DIR))

from config import SERVER, ALERT  # noqa: E402

LOG_FILE = SERVICE_DIR / "logs" / "health_cron.log"
STATE_FILE = SERVICE_DIR / "logs" / ".health_last_alert_ts"
API_URL = f"http://{SERVER.host}:{SERVER.port}/api/health"
PING_URL = f"http://{SERVER.host}:{SERVER.port}/api/ping"

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("health-cron")


def _can_alert() -> bool:
    """Check if enough time has passed since last alert (rate limiting)."""
    if not STATE_FILE.exists():
        return True
    try:
        last_ts = float(STATE_FILE.read_text().strip())
        return (time.time() - last_ts) > ALERT.alert_cooldown_seconds
    except (ValueError, OSError):
        return True


def _mark_alert_sent():
    """Record timestamp of last alert."""
    STATE_FILE.write_text(str(time.time()))


def _get_feishu_token() -> str | None:
    """Get Feishu tenant access token."""
    if not ALERT.feishu_app_id or not ALERT.feishu_app_secret:
        log.error("Feishu credentials not configured in .alert.env")
        return None
    try:
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        body = json.dumps(
            {
                "app_id": ALERT.feishu_app_id,
                "app_secret": ALERT.feishu_app_secret,
            }
        ).encode()
        req = Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == 0:
                return data.get("tenant_access_token")
            log.error(f"Feishu token failed: {data}")
    except Exception as e:
        log.error(f"Feishu token error: {e}")
    return None


def _send_feishu_alert(title: str, details: str, color: str = "red") -> bool:
    """Send an interactive card message to Feishu.

    Returns:
        True if sent successfully, False otherwise.
    """
    token = _get_feishu_token()
    if not token:
        log.error("Cannot get Feishu token, skipping alert")
        return False

    url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": color,
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": details},
            },
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": f"Memory Service Alert | {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    }
                ],
            },
        ],
    }
    body = json.dumps(
        {
            "receive_id": ALERT.feishu_chat_id,
            "msg_type": "interactive",
            "content": json.dumps(card),
        }
    ).encode()

    for attempt in range(1, ALERT.alert_retry_count + 1):
        try:
            req = Request(url, data=body, method="POST")
            req.add_header("Authorization", f"Bearer {token}")
            req.add_header("Content-Type", "application/json")
            with urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode())
                if result.get("code") == 0:
                    log.info(f"Feishu alert sent: {title}")
                    return True
                log.error(f"Feishu send failed (attempt {attempt}): {result}")
        except Exception as e:
            log.error(f"Feishu send error (attempt {attempt}): {e}")
        if attempt < ALERT.alert_retry_count:
            time.sleep(2)

    return False


def _check_health() -> dict | None:
    """Call /api/health and return parsed JSON, or None if unreachable.

    Falls back to /api/ping if /api/health times out (e.g. during indexing
    when SQLite locks block reads).
    """
    try:
        req = Request(API_URL, method="GET")
        with urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        pass  # Fall through to ping

    # Health timed out — try lightweight ping
    try:
        req = Request(PING_URL, method="GET")
        with urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            if data.get("pong"):
                return {"status": "ok", "indexing": True}
    except Exception:
        pass

    return None


def main():
    health = _check_health()

    # Case 1: Service unreachable
    if health is None:
        if _can_alert():
            sent = _send_feishu_alert(
                "Memory Service Unreachable",
                "**The memory-service is not responding.**\n\n"
                "Possible causes:\n"
                "- Service process crashed\n"
                "- Ollama is not running\n"
                "- Port 5370 is blocked\n\n"
                "Action: Check with `curl http://127.0.0.1:5370/api/health`",
            )
            if sent:
                _mark_alert_sent()
                log.warning("Alert sent: service unreachable")
        return

    status = health.get("status", "unknown")

    # Case 2: Everything OK — completely silent
    if status == "ok":
        return

    # Case 3: Degraded or Error
    if not _can_alert():
        return

    sent = False

    if status == "error":
        ollama = health.get("ollama", "?")
        details = (
            f"**Status:** error\n\n"
            f"**Ollama:** {'Connected' if ollama else 'DISCONNECTED'}\n"
            f"**Disk:** {health.get('disk', {}).get('used_pct', '?')}% used\n"
            f"**Uptime:** {health.get('uptime_seconds', 0) / 3600:.1f}h\n"
        )
        sent = _send_feishu_alert("Memory Service ERROR", details, "red")

    elif status == "degraded":
        # 2026-06-25: 优先用服务端权威的 degraded_reasons (type 分类), 把"磁盘满"和
        # "查询慢"拆开标注 — 否则运维会把磁盘满误读成查询慢 (6/24 教训). 旧逻辑用本地
        # 硬编码阈值重判, 与服务端判定可能不一致, 作为 fallback 保留.
        disk = health.get("disk", {})
        metrics = health.get("recent_latency_ms", {})
        server_reasons = health.get("degraded_reasons") or []
        reason_types = set()
        reasons = []
        for r in server_reasons:
            rtype = r.get("type", "?")
            reason_types.add(rtype)
            label = {"disk": "磁盘", "slow_query": "查询慢"}.get(rtype, rtype)
            reasons.append(f"- [{label}] {r.get('detail', '')}")
        if not reasons:
            # Fallback: 服务端没给 degraded_reasons 时按旧逻辑本地判
            if disk.get("used_pct", 0) > 90:
                reason_types.add("disk")
                reasons.append(
                    f"- [磁盘] Disk usage critical: {disk['used_pct']}% "
                    f"(free: {disk.get('free_gb', '?')} GB)"
                )
            if metrics.get("search_max_ms", 0) > 500:
                reason_types.add("slow_query")
                reasons.append(
                    f"- [查询慢] Slow search detected: {metrics['search_max_ms']:.0f}ms max"
                )
        detail_text = "\n".join(reasons) if reasons else "Unknown degradation"
        # 告警标题直接点明类别, 一眼看清是磁盘还是延迟
        if reason_types == {"disk"}:
            title = "Memory Service Degraded — 磁盘"
        elif reason_types == {"slow_query"}:
            title = "Memory Service Degraded — 查询慢"
        elif reason_types:
            title = "Memory Service Degraded — 磁盘+查询慢"
        else:
            title = "Memory Service Degraded"
        sent = _send_feishu_alert(
            title,
            f"**Status:** degraded\n\n{detail_text}",
            "orange",
        )

    if sent:
        _mark_alert_sent()
        log.warning(f"Alert sent: status={status}")


if __name__ == "__main__":
    main()
