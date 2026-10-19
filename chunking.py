"""chunking.py — 统一切块模块 (2026-06-11 审计 A3+B1, 单一真相源)。

背景: indexer.py(watcher) 与 index_*_embed.py(cron) 各自实现 _chunk_text,
overlap 记账差 ±1 字符 → 同一文件两条路径产出不同 chunk 边界 → chunk_hash
对不齐 → 去重失效产生真重复 (6/9 曾实测 dedup 后 2 分钟回灌 13179→17646)。

本模块是唯一切块实现, 两条路径必须 import 这里, 禁止再写本地副本。

CHUNKER_VERSION 平滑迁移 (会审 DeepSeek 修正, 防历史 hash 全变引发更大不一致):
- 新写入的 chunk metadata 带 chunker_version 字段
- 去重比较只在"同一文件重索引"时按 hash 对比 — 旧文件首次被 v2 重索引时
  hash 全不同 → 整文件重写一次, 之后稳定。不做跨版本 hash 对比。

prefix_hash / DEDUP_PREFIX_CHARS: 跨文件内容去重算法 (B1 审计修复)。
两条索引路径 (server.py watcher + cron) 必须使用此处同一实现,
否则去重后 2 分钟内容回灌 (6/9 事故机制)。
"""
import hashlib
import re

CHUNKER_VERSION = 2

# 跨文件内容去重: 取每个 chunk 前 N 字符的 SHA1 作指纹
DEDUP_PREFIX_CHARS = 200

# 单行超长守卫沿用 2026-05-11 教训: 超长单行(base64/JSON dump)直接喂 ollama
# 触发 context overflow + 内存爆炸 (实测 phys_footprint_peak 43GB), 强制按字符切。


def chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    """按行聚合切块, overlap 记账统一计入换行符 (+1)。"""
    lines = text.split("\n")
    chunks: list[str] = []
    current_lines: list[str] = []
    current_len = 0

    for line in lines:
        line_len = len(line) + 1
        if line_len > chunk_size:
            if current_lines:
                chunks.append("\n".join(current_lines))
                current_lines = []
                current_len = 0
            for i in range(0, len(line), chunk_size):
                chunks.append(line[i:i + chunk_size])
            continue
        if current_len + line_len > chunk_size and current_lines:
            chunks.append("\n".join(current_lines))
            overlap_lines: list[str] = []
            overlap_len = 0
            for l in reversed(current_lines):
                if overlap_len + len(l) + 1 > chunk_overlap:
                    break
                overlap_lines.insert(0, l)
                overlap_len += len(l) + 1
            current_lines = overlap_lines + [line]
            current_len = overlap_len + line_len
        else:
            current_lines.append(line)
            current_len += line_len

    if current_lines:
        chunks.append("\n".join(current_lines))
    return chunks


def chunk_hash(chunk: str) -> str:
    """统一指纹: MD5 前 12 位 (沿用 indexer.py 原口径)。"""
    return hashlib.md5(chunk.encode("utf-8", errors="replace")).hexdigest()[:12]


def prefix_hash(chunk: str) -> str:
    """跨文件内容去重指纹: 取前 DEDUP_PREFIX_CHARS 字符的 SHA1。

    两条索引路径 (watcher + cron) 必须调用此函数, 不得各自定义。
    """
    return hashlib.sha1(
        chunk[:DEDUP_PREFIX_CHARS].encode("utf-8", errors="replace")
    ).hexdigest()


def content_hash(chunk: str) -> str:
    """MEMIDX 试点(T-MEMIDX-PILOT-IMPL change#1)跨文件去重指纹: **全内容** SHA1。

    规范化 = 仅去首尾空白(chunk.strip()), **保留时间戳等**(保守, 防误合并不同时点内容)。
    相对 prefix_hash(前 200 字符)能抓 >200 字符的长模板(系统提示/JSON schema 回显), 治
    Y10 审计实测的 7% 冗余(最高 506× 系统提示回显因后段时间戳不同漏过 200-prefix 去重)。
    仅试点路径调用(pilot_config 门控); 非试点仍走 prefix_hash。
    """
    return hashlib.sha1(chunk.strip().encode("utf-8", errors="replace")).hexdigest()


# ── MEMIDX change#2 黑名单过滤 (T-MEMIDX-PILOT-IMPL; 单一真相源, watcher+cron 两路径共用) ──
def drop_base64_lines(text: str) -> str:
    """保守丢弃**明显 base64/编码长串行**(>200 字符、无空格、纯 base64 字符集) —— 只杀编码数据,
    不误伤代码(有空格/换行)或中英文散文。"""
    out = []
    for ln in text.split("\n"):
        s = ln.strip()
        if len(s) > 200 and " " not in s and re.fullmatch(r"[A-Za-z0-9+/=_-]+", s):
            continue
        out.append(ln)
    return "\n".join(out)


def pilot_blacklist_filter(text: str, bl: dict) -> str:
    """change#2 黑名单(逐条 config 开关): strip <system-reminder> 模板段 / 重复注入卡(config
    正则枚举, 行锚防 over-reach) / base64 编码行。对话轮真文本照留; 坏正则跳过不阻断主流程。
    watcher(indexer.py)与 cron(index_claude_code_embed.py)两路径**都调此函数**, 保证一致。"""
    if not text:
        return text
    if bl.get("skip_system_reminder"):
        text = re.sub(r"(?s)<system-reminder>.*?</system-reminder>", " ", text)
    for pat in (bl.get("injection_card_patterns") or []):
        try:
            text = re.sub(pat, " ", text)
        except re.error:
            pass  # 坏正则跳过(调用方可 logger.debug)
    if bl.get("skip_base64_or_nonnl"):
        text = drop_base64_lines(text)
    return text
