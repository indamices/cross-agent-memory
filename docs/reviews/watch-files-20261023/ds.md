# Review artifact — deepseek-v4.1-flash

The following metadata is supplied by the review harness; the two model reports below are verbatim. Their scopes combine the unchanged five-file candidate and the subsequent server/count test fix. Raw packets, host scripts, manifests and JSONL remain private.

Reviewed files: `indexer.py`, `server.py`, `examples/client-instructions.md`, `INSTALL-FOR-AI.md`, `test_watch_files_feedback_1023.py`, `tests/test_backup.py`, `test_project_init_feedback_1023.py`.
Native run IDs: ses_e97a97a82ffe34vSQEtTGI3hFu, ses_e979d4316ffeNeOrdahT5aiHDr.

## Candidate source SHA256

```json
{
  "indexer.py": "9bf982086f162fa07a70720cc2b585e58d3795caa54e36bc76518c6de171376a",
  "server.py": "31ba24798966f03a028bcb445fa09ef4455331f7be50dba4c0b39dad73ede8da",
  "examples/client-instructions.md": "73726c97f48398a36991c2fa44542be83560611145577013c4d32e286a4c3337",
  "INSTALL-FOR-AI.md": "5a2234ebafdb7c6b63868d3c4cf2be3bd9f36e8db2afcacf8c6b2e820bc83663",
  "test_watch_files_feedback_1023.py": "24c4761c07278d5614a5543a0a3e3b3a01c05e6219e35263a506daa414dd83fd",
  "tests/test_backup.py": "e50aaa024a25cd3ca87ce8f5187510d079756eee84c4690480652669e4c42532",
  "test_project_init_feedback_1023.py": "86d3cd98adbf2d34dc9d7285b41bd022457f8e91b0bef422b0893c84a2954755"
}
```

## Verbatim initial public-candidate report

**VERDICT: PASS（仅材料层，结论有边界）**

- 范围：只依据所给两包正文的 diff、未改动接口摘录和取证说明判断；未调用工具，未亲跑，也不声称执行过。
- 这个 PASS 只说明材料内部一致、MF-1~4 在材料层关闭；不替代独立原生运行验证，也不等于最终发布认证。
- 未发现任何具体反例足以支持 HOLD 或 REJECT。

---

## MF-1 — 关闭

- `tests/test_backup.py` 的 `path_bindings` 明确断言 **10 个真实运行绑定**：`config.DATA_DIR`、`config.SQLITE_DB`、`db.SQLITE_DB`、`config.PROJECTS_DIR`、`db.COLD_DB_PATH`、`backup._BACKUP_ROOT`、`backup.get_api_snapshot_dir()`、`server.MEMORY_MISS_LEDGER`、`server.MEMORY_HITS_LEDGER`、`indexer._MEMIDX_METRIC_LOG`。
- 每项都做 `Path.resolve()` 精确相等，并要求 `is_relative_to(root)` 成立，两层断言都有。旧版只做 `startswith`、且只查 snapshot 相对包路径的弱断言已移除。
- `DIM = 1024` 未改动；`DIM` 行之后的功能体未见放松。材料与“后 `DIM=1024` 功能体字节级同 main、未放宽”一致。
- 结论：MF-1 已按要求关闭，不是表面修补。

## MF-2 — 关闭（材料层）

- 两个独立补充测试分别覆盖：
  - 空 watch 配置：`pytest.raises(ValueError)`，且断言无 `schedule` / `start`；
  - 全部目标不存在：`pytest.raises(ValueError)`，且断言无 `schedule` / `start`。
- `indexer.py` 的 `start_watching` 在 `watches` 为空时抛 `ValueError`，抛点位于 `observer.schedule/start` 之前，与测试断言一致。
- **待证点（非已证缺陷）**：`INSTALL-FOR-AI.md` 宣称“project watcher error, not whole server crash”，但所给材料只证明 `start_watching` 抛错，未包含 server 启动循环逐 project 捕获 `ValueError` 的代码。原生复跑应加一条“一坏一好两个 project，坏 project 不阻断好 project watcher”的用例。这是材料缺证据，不是具体反例。

## MF-3 — 按材料接受（documentary）

- 材料给出了 packet 侧规格、原生 run 原始输出，以及机械修正授权；最终 `test_watch_files_feedback_1023.py` 的 `test_symlink_source_identity_keeps_config_path` 断言“交付配置路径、目录软链映射回配置目录 + 相对路径”，断言未削弱。
- 按“不把未证作者猜测当证据”：不基于怀疑判 REJECT。作者身份只能按提供的 provenance 接受，材料本身不能密码学式独立证明。
- 非阻断观察：最终 `test12` 内仍有一行冗余 `import indexer`（模块顶部已导入），不影响语义，也与作者身份无关。

## MF-4 — 关闭

- `INSTALL-FOR-AI.md` 明确 `docs/API.md` 是注册 API 参考；capture 路径指向 `examples/client-instructions.md`。
- `examples/client-instructions.md` 分“文件 capture”和“MCP capture”两条路径，并说明 MCP 写需客户端显式暴露 `memory_write`、结果需按项目 readback 验证。
- 没有“`API.md` 同时描述两条 capture 路径”的宣称。MF-4 的歧义已消除。

---

## 新增 14/50 结果评估

- **14**：呈现的 `test_watch_files_feedback_1023.py` 正好 **14 个 test 函数**（12 原有 + 2 补充），与“watcher module 14 passed”结构一致。我逐条核对用例与实现逻辑，未发现冲突：空 watch/缺失目标、单文件父目录非递归、同父去重、目录递归语义、软链身份、moved 原子替换、E2E 均有对应用例。这是材料一致性，不是执行认证。
- **50**：所给公开片段无法复核全包 50，属自报，需原生独立运行确认。
- **关键门槛**：材料自述“current tests/docs have NEW SHA, must verify current”，且旧 Sol 原生 run 是旧 SHA。发布前必须在 **NEW SHA** 上独立原生复跑，并核对 part-2 §SHA256 列出的 5 个哈希。

## 非阻断边界

- 已声明边界与实现一致：只监听已存在树、未来缺失目录、删除清理、软链 rebind、重复别名、files-only 过滤非全局排除策略、E2E 有界等待。
- 重复别名：`_file_sources` 以 resolve 后路径为 key，两个配置别名解析到同一目标时只保留一个；这与“duplicate aliases 不新保证”一致，建议保持文档明示。
- 文档精度小点：`INSTALL-FOR-AI.md` 的“missing directories are not watched for future creation”指 `watch_dirs`；同一文档又允许“缺失文件 + 已存在父目录”的 future creation 监听。两处不矛盾，但读者可能混淆，可在原生验证后顺手收紧措辞。
- 公开/私有：我的本次审语未复制 packet 内容或内部 run/session 标识；若候选正文本身要公开，建议按既定策略剔除“private packet excerpts”段与内部标识，只保留 repo 相对路径的验证说明。

## PASS 保持有效的放行条件

独立原生运行在 NEW SHA 上复现：**watcher 14 + 全包 50 + bash syntax/install dry-run + 本地 Ollama E2E**，并附带确认“单个 project watchdog 失败不阻断其他 project”。任一失败，本 PASS 作废。

## Verbatim incremental counter report

## 增量终审结论

**VERDICT: PASS（仅材料层，增量范围）**

审理范围：仅本次新增的 `_init_project` 计数配对修复（public/server.py 与 private/server.py）与新增独立测试 `test_project_init_feedback_1023.py`。未变六文件沿用前轮 GLM/Sol 真实 native 证据、SHA 未变，本轮不复核其内容。本人仅材料审，未亲跑、未执行命令、未触生产、未联网、未委派；不声称 native。

## 逐项核验

**1. 红因 `-1 == 0` 真实**
旧序为 `start_watching`→`_initialized_projects.add`→`with lock: count += 1`，finally 无条件 `count -= 1`。坏项目（root 存在但 `watch_dirs` 目标不存在）在 `start_watching` 处抛 `ValueError`，增量未执行、减量执行 → -1。双仓红输出 traceback 均落在 `_observers.append(start_watching(proj))`，断言 `-1 == 0`，与代码序一致，是真实判别器而非构造。public(server.py:106)/private(server.py:111) 行号偏移 5 行与两仓文件整体差异自洽。

**2. 改动最小性**
仅把 `with _indexing_lock: _indexing_count += 1` 从 `start_watching` 之后移到 `try` 之前，其余不动。语义闭合：所有通过 `_validate_project` 的初始化 +1 一次，finally −1 一次 → 坏/好/索引抛错各路径净额均 0；未改 `start_watching`、异常兜底、`index_project`、`_initialized_projects` 语义。双仓 `_init_project` 体字节一致（材料层）。

**3. 隔离边界**
导入前设 `MEMSVC_*` 全量沙箱 env 并重载 config/indexer/server；`indexer.Observer` 以 `StubObserver` 替身，坏项目在构造 Observer 前即抛错（走真实生产路径），好项目走替身不启原生线程；根级独立文件独立进程，不合入整包回归进程。0.07–0.10s 的绿跑佐证无原生线程。隔离成立。

**4. 前轮双项目条件闭环**
该测试即“一坏一好”：坏项目不入 initialized、好项目入且 `index_project` 恰一次、仅一个 observer 被 start、`_indexing_count == 0`。前轮 DS MF-2 的待证点（坏 project 不阻断好 project + 计数配对）在当前具体测试与源码下**已闭环（材料层）**。

## 必须修

无具体缺陷。

## 非阻断观察

- `assert _indexing_count >= 0` 在 `== 0` 之后冗余，无害。
- 测试对模块级 `_indexing_count/_initialized_projects/_observers` 做白盒断言；作为计数回归可接受，建议明确这几项属被断言接口的一部分。
- manifest 正文写“five files”但实际列六项，属记录卫生，非缺陷。
- 新测试属根级独立进程，须在固定命令中单独跑，勿并入整包回归（材料已声明）。

## 边界与放行

本轮 PASS 不覆盖未来目录创建、删除清理、软链重绑、完整客户端 E2E 等未涉及能力——按约定不将其转为本轮既成反例。**放行前置条件**：由具备原生执行席在 NEW script/manifest SHA 上按固定命令亲跑 `public14watch / public50package / 真实 Ollama E2E / public+private 新 counter 测试（红→绿双仓）`，任一项失败或 SHA 不符则本 PASS 作废；部署重启仍按提案单独把关，材料 PASS ≠ 部署授权。
