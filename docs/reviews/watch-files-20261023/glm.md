# Review artifact — glm-5.3

The following metadata is supplied by the review harness; the two model reports below are verbatim. Their scopes combine the unchanged five-file candidate and the subsequent server/count test fix. Raw packets, host scripts, manifests and JSONL remain private.

Reviewed files: `indexer.py`, `server.py`, `examples/client-instructions.md`, `INSTALL-FOR-AI.md`, `test_watch_files_feedback_1023.py`, `tests/test_backup.py`, `test_project_init_feedback_1023.py`.
Native run IDs: ses_e97a97a94ffeKTxaU06IgJUqun, ses_e979d4316ffe0l3Hx08QZI96X6.

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

亲跑完成（native exit 0），实测结果与声明一致：14 watcher passed / 50 package passed / install dry-run 通过 / 真实本地 Ollama bge-m3 E2E PASS / decisions·debugging·cross-client 3 组真实 embedding 往返 PASS。

逐项核验：

- **MF-1（隔离断言）**: tests/test_backup.py 新增 10 项运行时绑定断言（Path.resolve 精确相等 + scratch 根包含：DB/data/cold/backup/snapshot/projects/miss/hits/index metrics），在 50-passed 内实际执行。处置成立。
- **MF-2（空/全缺失目标 fail-loud）**: 两个独立结果态测试断言 `ValueError` 且零 schedule/start，原 main 上会 DID NOT RAISE（diff 中 watches 为空即 raise 的新逻辑可复核）；INSTALL 已写明"项目级 watcher 报错，非整服务崩溃"。处置成立。
- **MF-3（test12 作者身份疑点）**: 附原始 native run（ses_e9863556…）prompt/output 摘录证实 test12 由测试作者在实现前依 source-identity 结果态规格产出，首份公开测试 commit 138cf17 早于任何公开 indexer 实现 commit；机械修正（重复 import/append 标记/误导性失败消息）有作者明示授权确认，断言未变。疑点被原始证据反驳，不采纳为阻断。
- **MF-4（API.md 定位）**: 已明确 docs/API.md 仅为注册 API 参考，capture 路径指向 examples/client-instructions.md，无"API.md 同时描述两路径"的声明。歧义消除。

边界声明（不监听未来创建目录、快照 best-effort 不由 SQLite capture 认证、packet 私有仅审语公开、无 push/PR/生产变更）与 diff 内容一致，无越界。未见误读或未证猜测被当作证据，未发现真实反例。

**VERDICT: PASS**

## Verbatim incremental counter report

**VERDICT: PASS**

亲跑结果（native exit 0）：public watcher 14 passed / 整包 50 passed / install dry-run 通过 / 真实本地 Ollama bge-m3 E2E PASS / decisions·debugging·cross-client 3 组真实 embedding 往返 PASS / public 新增 project-init 测试 1 passed / private 1 passed + 17 package passed。与 packet 声明计数完全一致。

增量审阅（仅本轮新范围）：

- **红因真实性**：双仓 traceback 均为 `start_watching` 抛 `ValueError`（坏项目 watch 目录不存在）→ `finally` 无条件 `-1` 而增量从未发生 → `-1 == 0` 断言失败。这是真实计数配对缺陷，非测试自造。
- **修法最小性与正确性**：把 increment 移到 `try` 之前（同锁内、验证通过后），保证"每个通过验证的初始化必配对一次 decrement"，任何退出路径（含 watcher 启动失败）均平衡；未动 `_validate_project`/`_init_projects`/错误隔离语义。public/private `_init_project` 补丁后逐字一致。坏项目隔离（异常捕获、好项目继续）行为不变，DS 既有放行条件在原生运行中已被新测试直接覆盖。
- **测试独立性与隔离**：测试由 GLM 在实现前依接口结果态写成并先行 commit（public 18292d1 / private 50c5a30）；全部资源 env 指向 temp 目录后才 import 模块；stub observer 无原生线程/DB/网络；置于 repo 根并以独立进程运行，fixture 重载 `sys.modules` 不污染整包回归进程——该隔离必要且已声明。membership 缺陷修正（改按 `set[str]` 查 name）合理，作者已留痕"原测试未测过产品路径"。
- **边界与部署**：五文件 SHA 与前轮候选一致（旧证据不作新执行复用，本轮已全部原生复跑）；部署提议（commit private 修复+同测试、验 SHA、重启既有托管服务、读 health；public 保持本地分支不 push/PR）边界清晰，属非交易时段受管重启，R7 范围内，留待后续放行执行。

必须修：无。非阻断建议：① `INSTALL-FOR-AI.md` 中 watch_dirs "missing→不监听" 与缺失文件+存在父目录可监听两处措辞可收紧；② 原测试 TypeError 教训已按作者建议留痕即可。本轮未覆盖功能（缺失目录未来创建、软链 rebind、删除清理、MCP 端到端等）不构成反例，沿用既有边界声明。
