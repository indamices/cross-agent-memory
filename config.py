"""Global configuration for memory-service."""

import os
import sys
import yaml
from pathlib import Path
from dataclasses import dataclass, field


SERVICE_DIR = Path(__file__).parent
SQLITE_DB = Path(os.environ.get("MEMSVC_DB", SERVICE_DIR / "memory.db"))
PROJECTS_DIR = SERVICE_DIR / "projects"
OPENCLAW_MEMORY_DIR = Path.home() / ".openclaw" / "memory"


def _env_num(name: str, default):
    """env 数值解析, 非法值回退默认并告警 (flash执行审D-A: 防导入期 ValueError 起不来)。"""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return type(default)(raw)
    except (TypeError, ValueError):
        print(f"[config] WARNING: env {name}={raw!r} 非法, 回退默认 {default!r}",
              file=sys.stderr)
        return default


@dataclass
class EmbeddingConfig:
    backend: str = "ollama"
    ollama_url: str = "http://localhost:11434"
    long_text_model: str = "bge-m3:latest"
    short_text_model: str = (
        "bge-m3:latest"  # Changed from shaw/dmeta-embedding-zh which is not available
    )
    local_model: str = "BAAI/bge-small-zh-v1.5"
    local_batch_size: int = 16
    long_threshold: int = 200
    batch_size: int = 8
    timeout: int = 60
    cache_max_size: int = 512


@dataclass
class ProjectConfig:
    name: str = ""
    root: str = ""
    watch_files: list = field(default_factory=list)
    watch_dirs: list = field(default_factory=list)
    exclude_patterns: list = field(
        default_factory=lambda: [
            "*.pyc",
            "__pycache__",
            "node_modules",
            ".git",
            "*.csv",
        ]
    )
    chunk_size: int = 800
    chunk_overlap: int = 100


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = _env_num("MEMSVC_PORT", 5370)  # env 仅供 E2E 测试实例换端口
    log_level: str = "INFO"


@dataclass
class SearchConfig:
    time_decay_half_life: float = 30.0
    mmr_lambda: float = 0.7
    feedback_file: str = "feedback_store.json"


@dataclass
class MonitorConfig:
    # 2026-05-26 maintainer decision: 500 → 8000ms. baseline P50 6.6s/P95 10.3s 是正常态,
    # 8000ms = 远超 baseline P95, 真出大事 (索引锁/IO 阻塞) 才报警.
    # 2026-08-15 T-MEMSVC-DEGRADED-FIX: slow 计数改滚动窗口, 根治粘性标志缺陷
    # (旧逻辑进程内只增不重置, 一次超阈值即永久 degraded 直到 scheduled_restart;
    # 8/15 一天 3 次误报复发). 判定 = 窗内 slow_count >= degraded_min_slow
    # 且 slow_ratio > degraded_slow_ratio, 且窗内总样本 >= degraded_min_samples
    # (防小样本误判). slow 事件滚出窗口后 health 自动恢复, 无需重启.
    # env 覆盖仅供 E2E 测试实例压小参数用, 生产不设 env 走默认.
    # Tradeoff 落档 (flash审D4): min_samples=10 意味着窗内样本不足时即使全 slow
    # 也不判 degraded — 这是有意的: scheduled_restart 后冷查询合法 14-30s
    # (2026-06-11 预热机制前科), 小样本全 slow 的常见成因是冷缓存非真劣化,
    # 任务书明令"min_samples 门控防小样本误判"。若夜间低流量期需更灵敏,
    # 调低此值即可 (无需改代码)。
    slow_threshold_ms: float = _env_num("MEMSVC_SLOW_THRESHOLD_MS", 8000.0)
    slow_window_seconds: float = _env_num("MEMSVC_SLOW_WINDOW_SECONDS", 3600.0)
    degraded_min_slow: int = _env_num("MEMSVC_DEGRADED_MIN_SLOW", 2)
    degraded_slow_ratio: float = _env_num("MEMSVC_DEGRADED_SLOW_RATIO", 0.05)
    degraded_min_samples: int = _env_num("MEMSVC_DEGRADED_MIN_SAMPLES", 10)


@dataclass
class BackupConfig:
    backup_dir: str = os.environ.get("MEMSVC_BACKUP_DIR", "backups")
    max_backups: int = 7
    archive_days: int = 30


@dataclass
class SecurityConfig:
    enabled: bool = False
    api_token: str = ""
    read_requires_token: bool = False


def _load_security_config() -> SecurityConfig:
    """Bearer-token auth from env.

    MEMSVC_TOKEN              — set it to require `Authorization: Bearer <token>`
    MEMSVC_READ_REQUIRES_TOKEN — "1"/"true"/"yes" also protects read endpoints
                                 (default: reads stay open, writes need the token)
    """
    token = os.environ.get("MEMSVC_TOKEN", "").strip()
    return SecurityConfig(
        enabled=bool(token),
        api_token=token,
        read_requires_token=os.environ.get("MEMSVC_READ_REQUIRES_TOKEN", "").strip().lower()
        in ("1", "true", "yes"),
    )


@dataclass
class AlertConfig:
    """Feishu alert configuration for health check cron.

    Credentials are read from a ``.alert.env`` file in SERVICE_DIR
    (one key=value per line). Falls back to empty strings if the file
    does not exist — alerts are silently skipped.
    """

    feishu_app_id: str = ""
    feishu_app_secret: str = ""
    feishu_chat_id: str = ""
    alert_cooldown_seconds: int = 1800  # 30 minutes
    alert_retry_count: int = 2


EMBEDDING = EmbeddingConfig()
SERVER = ServerConfig()
SEARCH = SearchConfig()
MONITOR = MonitorConfig()
BACKUP = BackupConfig()
SECURITY = _load_security_config()
ALERT = AlertConfig()
# F-1 (reviewer 20260828 裁决/maintainer-approved): SQLite 数据库内部文件按类型拒 — 8/25 事故真因
# (-wal/-shm/-journal 二进制被当记忆文档嵌入, 1.97万条垃圾向量进库); 与 B5 体积闸互相独立.
DEFAULT_EXCLUDE = [
    "*.pyc", "__pycache__", "node_modules", ".git", "*.csv",
    # SQLite 内部文件 (按类型拒, 不靠体积兜底)
    "*-wal", "*-shm", "*-journal",
    "*.sqlite-wal", "*.sqlite-shm", "*.sqlite-journal",
    "*.reindex-lock*", "*-tmp", "*.db-wal", "*.db-shm",
]


def _load_alert_config() -> AlertConfig:
    """Load Feishu credentials from ``.alert.env`` file."""
    env_file = SERVICE_DIR / ".alert.env"
    cfg = AlertConfig()
    if not env_file.exists():
        return cfg
    try:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            key = key.strip().lower()
            value = value.strip()
            if key == "feishu_app_id":
                cfg.feishu_app_id = value
            elif key == "feishu_app_secret":
                cfg.feishu_app_secret = value
            elif key == "feishu_chat_id":
                cfg.feishu_chat_id = value
            elif key == "alert_cooldown_seconds":
                cfg.alert_cooldown_seconds = int(value)
            elif key == "alert_retry_count":
                cfg.alert_retry_count = int(value)
    except Exception:
        pass
    return cfg


ALERT = _load_alert_config()


def load_project_config(project_root: str) -> ProjectConfig:
    """Load project config from a ``.memory-service.yaml`` in the given directory.

    Falls back to sensible defaults (directory name as project name) if the
    YAML file does not exist.

    Args:
        project_root: Path to the project directory.

    Returns:
        A ``ProjectConfig`` with resolved absolute ``root``.
    """
    project_root = str(Path(project_root).expanduser())
    cfg_path = Path(project_root) / ".memory-service.yaml"
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return ProjectConfig(
            name=data.get("name", Path(project_root).name),
            root=str(Path(project_root).resolve()),
            watch_files=data.get("watch_files", []),
            watch_dirs=data.get("watch_dirs", []),
            exclude_patterns=data.get("exclude_patterns", list(DEFAULT_EXCLUDE)),
            chunk_size=data.get("chunk_size", 800),
            chunk_overlap=data.get("chunk_overlap", 100),
        )
    return ProjectConfig(
        name=Path(project_root).name,
        root=str(Path(project_root).resolve()),
    )


def discover_projects() -> list[ProjectConfig]:
    """Scan the ``projects/`` directory for YAML configs and return all found.

    Returns:
        List of ``ProjectConfig`` instances (may be empty).
    """
    if not PROJECTS_DIR.exists():
        PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
        return []
    projects = []
    for f in PROJECTS_DIR.glob("*.yaml"):
        with open(f, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        projects.append(
            ProjectConfig(
                name=data.get("name", f.stem),
                root=data.get("root", ""),
                watch_files=data.get("watch_files", []),
                watch_dirs=data.get("watch_dirs", []),
                exclude_patterns=data.get("exclude_patterns", list(DEFAULT_EXCLUDE)),
                chunk_size=data.get("chunk_size", 800),
                chunk_overlap=data.get("chunk_overlap", 100),
            )
        )
    return projects
