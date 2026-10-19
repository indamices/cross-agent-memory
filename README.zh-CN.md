<h1 align="center">Cross-Agent Memory</h1>

<p align="center">
  <b>给所有 AI 编程 Agent 共用的一份本地记忆。</b><br>
  Claude Code 的历史、Codex 的会话、Cursor 的工作、项目文档与决策——<br>
  只索引一次，每个 Agent 都能检索，永远属于你自己。
</p>

<p align="center">
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
  <img alt="Python 3.10+" src="https://img.shields.io/badge/python-3.10%2B-blue.svg">
  <a href="https://github.com/indamices/cross-agent-memory/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/indamices/cross-agent-memory/actions/workflows/ci.yml/badge.svg"></a>
  <img alt="Local-first" src="https://img.shields.io/badge/cloud-none%20%C2%B7%20data%20stays%20local-2ea44f.svg">
</p>

[English](README.md) | **中文**

<p align="center">
  <img src="docs/assets/demo.gif" alt="终端演示：安装、验证、写入与检索" width="860">
</p>
<p align="center"><i>真实运行：安装 → 验证 → 写入 → 检索，无摆拍。</i></p>

---

## 解决的问题

每一个 AI 工具都是一座孤岛。

你在 Claude Code 里花一周设计了一套架构，下周换到 Codex——它对此一无所知；
你重新粘贴上下文，重新解释项目。换到第三个工具时，同一场对话已经发生过五次，
而且任何一次都检索不到。

**cross-agent-memory 解决这个问题。** 它在本地运行一个小型记忆服务：

1. **汇入**——从任意 AI 工具（Claude Code / 各类 CLI / OpenCode / ZCode……）
   的会话历史，以及你指定的任意项目目录中摄取内容；
2. **索引**——用本地嵌入模型把一切写入同一个可检索的存储；
3. **供给**——通过 HTTP API 或直接以 **MCP 工具**提供给每个 Agent，
   任何支持 MCP 的 Agent 都能回忆起其他 Agent（和你）已经想清楚的事。

无云端、无账号，**数据永远不离开你的机器**。

## 功能特性

- **🔒 本地优先，隐私即设计**：嵌入与检索全部在本机完成
  （Ollama + SQLite + sqlite-vec）。断网也能正常工作。
- **🧠 跨工具共享**：内置 Claude Code、OpenCode、ZCode 会话库连接器，
  加上通用文件监控（任意项目/文档目录）。不管内容来自哪个工具，
  Agent 们共享**同一份**记忆。
- **🔌 一行接入 MCP**：通过 Model Context Protocol 暴露
  `memory_search` / `memory_write` / `memory_stats` 三个工具。
  Claude Code、Codex CLI、Cursor 等所有支持 MCP 的 Agent 即刻可用。
- **⚡ 真实规模下的性能**：为大数据量而建——sqlite-vec vec0 KNN +
  **1-bit 粗召回**预筛 + **热/温/冷三层存储**。参考部署实测
  **200 万+ chunks 检索耗时 0.36–1.20 秒**（见
  [实测数字](#实测数字)）。
- **🛠 自带运维能力**：健康检查与监控端点、滚动窗口降级判定、
  备份与恢复、快照归档、分层维护脚本。
- **📈 生产验证**：抽取自一套长期运行的多 Agent 系统，
  四款 AI 工具每天共用一个记忆库，数百万 chunk，每日增量索引。

## 架构

```
        Claude Code      Codex 类 CLI        OpenCode / ZCode      任意项目
             │                  │                  │                  │
             ▼                  ▼                  ▼                  ▼
      ┌────────────────────────────────────────────────────────────────────┐
      │                         连接器 / 索引器                            │
      │   增量水位 · 内容哈希去重 · 在写会话跳过 · 统一切块 ·               │
      │   本地嵌入 (Ollama, 如 bge-m3)                                     │
      └────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
      ┌────────────────────────────────────────────────────────────────────┐
      │                      SQLite 记忆库（单文件）                       │
      │   chunks (真相源)   ├─ vec0 KNN 索引 (sqlite-vec)                  │
      │   元数据             ├─ 1-bit 召回码 (Hamming 预筛)                │
      │                      └─ 热 / 温 / 冷 三层表                       │
      └────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴───────────────────┐
                    ▼                                     ▼
        HTTP API（端口 5370）                   MCP server（stdio）
        /api/search /api/memories               memory_search
        /api/stats  /api/health ...             memory_write / memory_stats
                    ▲                                     ▲
                    └─────────────────┬───────────────────┘
                                      │
                          你用的一切 Agent（任意工具/语言）
```

设计细节与完整检索流水线见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 🤖 让你的 AI 帮你装（一条指令）

最快的安装方式：在本仓库（或把仓库地址给 AI）里打开你的 AI 编程助手，
把下面这段话原样粘贴给它。它会在本地完成全部安装——虚拟环境、依赖、全新
隔离的数据库——并跑一次真实的写入/检索回路验证后向你汇报具体命令与结果。
所有数据只在本机，不发送到任何地方。

```text
Install this repository locally. Read AGENTS.md and INSTALL-FOR-AI.md first.
Use ./install.sh --dry-run, then ./install.sh. Keep all runtime data isolated
from existing databases and bind only 127.0.0.1. Verify initialization twice,
Ollama bge-m3, health, and a real write/search round trip using examples/run.py.
Add the client instructions from examples/client-instructions.md. Report exact
commands, results and blockers; do not claim success from health alone.
Do not import personal projects, restore backups or change system services.
```

（指令本身是英文，保证各类 AI 助手理解一致。）想手动安装就看下面的快速开始。
面向 AI 的完整协议见 [INSTALL-FOR-AI.md](INSTALL-FOR-AI.md) 与
[AGENTS.md](AGENTS.md)。

## 快速开始

### 0. 前置条件

- **macOS 或 Linux**（暂不支持 Windows——连接器使用 POSIX 文件锁）
- Python **3.10+**
- 本地运行 [Ollama](https://ollama.com)（提供嵌入模型）

```bash
ollama pull bge-m3            # 约 1.2 GB，中英文俱佳的嵌入模型
```

### 1. 安装

```bash
git clone https://github.com/indamices/cross-agent-memory.git
cd cross-agent-memory
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. 启动服务

```bash
python3 server.py
# Server listening on 127.0.0.1:5370
```

### 3. 喂给它第一批记忆

注册任意项目目录（文档、决策、笔记均可）：

```bash
curl -s -X POST http://127.0.0.1:5370/api/register \
  -H 'Content-Type: application/json' \
  -d '{"root": "/path/to/your/project"}'
```

> 目录下可以放 `.memory-service.yaml`（见 [examples/](examples/)）；
> 没有则使用合理默认值。注册后立即索引；之后的文件变化按防抖窗口批量入库
> （默认 **1 小时**，`MEMSVC_WATCH_DEBOUNCE` 可调）。需要改完立即可搜：
> `POST /api/reindex {"project": "..."}`，或以 `MEMSVC_WATCH_DEBOUNCE=0` 启动。

或者用连接器汇入 AI 工具的历史会话：

```bash
python3 connectors/index_claude_code.py     # ~/.claude/projects/**/*.jsonl
python3 connectors/index_opencode.py        # ~/.local/share/opencode/opencode.db
python3 connectors/index_zcode.py           # ~/.zcode/v2/tasks-index.sqlite
```

### 4. 随时检索

```bash
curl -s -X POST http://127.0.0.1:5370/api/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "当初为什么选择分层存储设计", "n_results": 5}'
```

也可以直接写入一条记忆（适合作为 Agent 的会话钩子）：

```bash
curl -s -X POST http://127.0.0.1:5370/api/memories \
  -H 'Content-Type: application/json' \
  -d '{"items": [{"type": "decision", "content": "为了本地优先，选择了 sqlite-vec 而不是独立向量数据库服务。"}]}'
```

## 接入你的 Agent（MCP）

`mcp_server.py` 是一个零依赖的 MCP（stdio）服务，把记忆 API 暴露为工具。
让 Agent 指向它即可：

**Claude Code**

```bash
claude mcp add memory -- python3 /ABS/PATH/cross-agent-memory/mcp_server.py
```

或写入 `.mcp.json`：

```json
{
  "mcpServers": {
    "memory": {
      "command": "python3",
      "args": ["/ABS/PATH/cross-agent-memory/mcp_server.py"],
      "env": { "MEMORY_SERVICE_URL": "http://127.0.0.1:5370" }
    }
  }
}
```

**Codex CLI**（`~/.codex/config.toml`）

```toml
[mcp_servers.memory]
command = "python3"
args = ["/ABS/PATH/cross-agent-memory/mcp_server.py"]
env = { MEMORY_SERVICE_URL = "http://127.0.0.1:5370" }
```

**Cursor / 其他 MCP Agent**——在其 MCP 设置中填同样的 command/args 即可。
若启动服务时设置了 `MEMSVC_TOKEN`，MCP server 侧也需设置 `MEMORY_SERVICE_TOKEN`。

接入后，Agent 拥有三个工具：

| 工具 | 作用 |
|------|------|
| `memory_search` | 在所有工具的全部会话/项目中做语义检索 |
| `memory_write`  | 写入一条决策 / 洞察 / 发现，供未来会话检索 |
| `memory_stats`  | 查看集合与规模 |

## 检索是如何工作的

每个 `/api/search` 请求执行分段流水线：

1. **查询嵌入**——本地计算（Ollama），按文本哈希缓存；
2. **粗召回**——每个 chunk 的 1-bit/维度编码，用 Hamming 距离预筛
   （小集合自动跳过）；
3. **精确 KNN**——sqlite-vec `vec0` 余弦 KNN（sqlite-vec 不可用时
   自动回退 numpy 暴力检索路径）；
4. **重排**——时间衰减（30 天半衰期）+ MMR 多样性 + `helpful` 反馈因子；
5. **返回**——文本 + 来源 + 距离 + 得分。

底层还处理好了那些枯燥但关键的工程：内容哈希去重、每连接器自带的增量水位、
跳过正在写入的会话、批量写限速、WAL 调参、陈旧索引自愈。

## 实测数字

参考部署（一台 Mac mini M4 Pro，7×24 运行、服务多 Agent 环境）：

| 指标 | 数值 |
|---|---|
| 索引规模 | **2,013,030 chunks** / 16 个集合 |
| 共用一份记忆的工具 | 4 个（Claude Code、OpenCode、ZCode、OpenClaw） |
| `/api/search` 实测延迟 | **0.36 s – 1.20 s**（5 条生产查询，热缓存） |
| 嵌入模型 | `bge-m3`（1024 维），本地 Ollama |
| 存储 | 单个 SQLite 文件 + 可随时重建的 vec0 派生索引 |

这是真实、庞杂、持续增长语料上的数字——不是玩具基准。
你的数字取决于语料规模与硬件；新库上一切检索都在百毫秒以内。

## API 一览

| 端点 | 方法 | 说明 |
|---|---|---|
| `/api/ping` | GET | 存活探测（不触碰 DB） |
| `/api/health` | GET | 健康状态：嵌入服务、磁盘、降级原因 |
| `/api/stats` | GET | 集合与 chunk 统计 |
| `/api/projects` | GET | 已配置项目 |
| `/api/monitor` | GET | 延迟 / 缓存 / 磁盘监控指标 |
| `/api/search` | POST | 语义检索（`query`、`n_results?`、`project?`） |
| `/api/memories` | POST | 直接写入记忆条目 |
| `/api/feedback` | POST | 标记 chunk `helpful` / `unhelpful`（影响排序） |
| `/api/register` | POST | 注册项目目录开始索引 |
| `/api/reindex` | POST | 强制重建索引（单项目或全部） |
| `/api/backup` / `/api/restore` | POST | 备份 / 恢复数据库 |
| `/api/archive-snapshots` | POST | 归档压缩旧快照 |

带请求/响应示例的完整参考见 [docs/API.md](docs/API.md)。

## 配置

全部使用合理默认值即可运行；需要时通过环境变量调整：

| 变量 | 默认 | 用途 |
|---|---|---|
| `MEMSVC_PORT` | `5370` | HTTP 端口 |
| `MEMSVC_DB` | `./memory.db` | SQLite 数据库路径 |
| `MEMSVC_SEARCH_MODE` | `vec` | `vec`（sqlite-vec）或 `legacy`（numpy） |
| `MEMSVC_BIT_RECALL` | `on` | 1-bit 粗召回预筛开关 |
| `MEMSVC_MAX_CONCURRENCY` | `8` | HTTP 并发上限（超出快速 503） |
| `MEMSVC_STALL_WATCHDOG` | `on` | 检索卡顿时抓取线程栈 |
| `MEMSVC_AUTH` | *(未设置)* | `1` 启用 Bearer 鉴权（写操作需 `Authorization: Bearer <token>`） |
| `MEMSVC_API_TOKEN` | *(未设置)* | Bearer 密钥；单独设置也会启用鉴权 |
| `MEMSVC_READ_REQUIRES_TOKEN` | *(未设置)* | `1` 时读操作也需 token |
| `MEMSVC_BACKUP_DIR` | `backups` | 数据库备份目录 |
| `MEMSVC_SLOW_THRESHOLD_MS` | `8000` | 慢查询阈值（健康判定用） |
| `MEMORY_MISS_LEDGER` | *关闭* | 零结果/低质结果账本（补写候选） |
| `MEMORY_HITS_LEDGER` | *关闭* | 每次查询命中采样账本 |
| `CLAUDE_PROJECTS_DIR` | `~/.claude/projects` | Claude Code 连接器数据源 |
| `OPENCODE_DB` / `OPENCODE_DBS` | `~/.local/share/opencode/…` | OpenCode 连接器数据源 |
| `ZCODE_DB` | `~/.zcode/v2/tasks-index.sqlite` | ZCode 连接器数据源 |
| `MEMORY_SERVICE_URL` | `http://127.0.0.1:5370` | MCP server 使用 |
| `MEMORY_SERVICE_TOKEN` | *(未设置)* | 开启鉴权时的 Bearer Token |

## 运维

- **健康检查**：`GET /api/health` 返回 `status`、磁盘、嵌入可用性与
  **降级原因**（慢查询、磁盘压力等）——为定时巡检设计，不产生误报。
- **备份**：`scripts/backup_silent.py`（适合 cron）或 `POST /api/backup`；
  恢复用 `POST /api/restore`。
- **分层**（可选）：`scripts/migrate_to_vec0.py` 构建 vec0 索引；
  `scripts/tier_rebuild.py` 构建热/温/冷分层表，让超大语料的热集保持精小。
- **快照归档**：`scripts/archive_silent.py`。
- **健康巡检 + 告警**：`scripts/health_check.py`（内置可选飞书 webhook，
  配置 `.alert.env` 即可）。

## 设计取舍（与非目标）

- **SQLite，而非向量数据库服务**：单文件、零外置服务、备份容易、离线可用；
  向量索引是派生物，随时可重建。
- **本地嵌入，而非云 API**：隐私 + 零边际成本；想换模型改
  `config.py` 即可。
- **文件是真相源**：所有入库内容都能从源头重新摄取，
  删掉数据库不会丢失磁盘上存在的任何东西。
- **不是云端同步产品**：这是单机本地服务（多 Agent、一份记忆）。
  多用户/服务端模式在路线图中。
- **不参与向量数据库跑分**：优先小依赖面与可运维性，而非极致 ANN 吞吐。

## 路线图

- [ ] Web 控制台（浏览 / 检索 / 管理记忆）
- [ ] 更多连接器：Codex CLI 会话库、Cursor、Aider、通用 JSONL
- [ ] 一等公民记忆类型（事实 / 经验 / 代码变更上下文）与自动提取管线
- [ ] 多设备同步（跨硬件工作的人群）
- [ ] Python 包 / Docker 镜像，一条命令完成安装

欢迎 Issue 与 PR——尤其是新连接器。见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 致谢

站在优秀工具的肩膀上：[SQLite](https://sqlite.org)、
[sqlite-vec](https://github.com/asg017/sqlite-vec)、
[Ollama](https://ollama.com)、[numpy](https://numpy.org)、
[watchdog](https://github.com/gorakhargosh/watchdog)。

## 许可

[MIT](LICENSE) © 2026 indamices
