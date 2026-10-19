#!/usr/bin/env python3
"""MEMEXEC-W3: 三层分表批量重建 (热/温/冷) — 派生索引重建, 真相层零改动.

设计: docs/W3-TIERING-NOTES.md + memsvc_tiering_design_20260816.md (补3 口径).
法理: vec0 表是派生索引 (8/4 "可重建" 第三次复用); 本脚本只建 W3 自己的三张
tier 表 + chunk_tier/tier_meta, **不触碰** chunks / vec_chunks (旧表 = 回滚锚).

分派规则 (与 db.tier_for_chunk 同口径, 保护项只有 rebuild 全量跑得到):
    age = now - source_ts   (source_ts = metadata.file_mtime, 缺则 updated_at)
    age <= hot_days         -> hot   (hot_days 按数据 derive, 见 --max-hot-rows)
    age <= warm_days(30)    -> warm
    >warm_days 且 (gold ∨ probe目标 ∨ 台账90d内被返回过) -> hot  (越龄置热, 补3①)
    else                    -> cold

部署序列 (生产): 合入+restart (legacy 行为) → 本脚本 rebuild (活库批提交, 服务
不中断) → --sync-stragglers 收敛中途写入 → restart (tiered 生效)。
回滚: checkout 旧 commit + 跑 scripts/migrate_to_vec0.py --rebuild + restart。

用法:
    python3 scripts/tier_rebuild.py --dry-run          # 只出分派统计 (只读)
    python3 scripts/tier_rebuild.py                    # 全量重建 + 自检
    python3 scripts/tier_rebuild.py --sync-stragglers  # 收敛 rebuild 窗口期的写入
    python3 scripts/tier_rebuild.py --verify           # 只跑对账
env: MEMSVC_DB (库路径) / MEMSVC_TIER_HOT_MAX_ROWS (默认 150000)
"""
import argparse
import ast
import hashlib
import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

TIER_TABLES = {
    "hot": "vec_chunks_hot",
    "warm": "vec_chunks_warm",
    "cold": "vec_chunks_cold",
}
REGISTRY = "chunk_tier"
META_TABLE = "tier_meta"
BATCH = 4096          # 批提交上限 (限锁窗: 与活库 indexer upsert 交错)
EMBED_BYTES = 4096    # bge-m3 float32[1024]

# Both optional: MEMSVC_PROBE_BASELINE (probe target set) and
# MEMSVC_HITS_LEDGER (recent-hit retention ledger). Missing/unreadable files
# degrade gracefully to empty protection sets.
DEFAULT_PROBE_BASELINE = os.environ.get("MEMSVC_PROBE_BASELINE", "")
DEFAULT_HITS_LEDGER = os.environ.get("MEMSVC_HITS_LEDGER", "")


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        import sqlite_vec
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
    except ImportError:
        print("[fatal] sqlite_vec 未安装 — vec0 表无法建/灌", file=sys.stderr)
        sys.exit(3)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def load_recent_hit_ids(ledger_path: str, days: int) -> set[str]:
    """台账 (query,caller 去重前的原始行) 近 N 天被返回过的 chunk id 集."""
    ids: set[str] = set()
    cutoff = time.time() - days * 86400
    try:
        with open(ledger_path, encoding="utf-8") as f:
            for line in f:
                try:
                    d = json.loads(line)
                    ts = datetime.strptime(d["ts"], "%Y-%m-%d %H:%M:%S").timestamp()
                    if ts < cutoff:
                        continue
                    top = d.get("top")
                    if isinstance(top, str):
                        top = ast.literal_eval(top)
                    for h in top or []:
                        if isinstance(h, dict) and h.get("chunk_id"):
                            ids.add(h["chunk_id"])
                except (json.JSONDecodeError, ValueError, KeyError, SyntaxError):
                    continue
    except OSError as e:
        print(f"[warn] hits ledger 不可读 ({e}) — recent-hit 保护置空")
    return ids


def load_probe_hashes(baseline_path: str) -> set[str]:
    """85 探针目标 = 基线 top5 的 content_hash 集 (sha1(text)[:16], memidx 口径)."""
    hashes: set[str] = set()
    try:
        d = json.load(open(baseline_path, encoding="utf-8"))
        for r in d.get("results", []):
            for h in r.get("top5") or []:
                if isinstance(h, dict) and h.get("content_hash"):
                    hashes.add(h["content_hash"])
    except (OSError, json.JSONDecodeError) as e:
        print(f"[warn] 探针基线不可读 ({e}) — probe 保护置空")
    return hashes


def _source_ts(mj, upd: float) -> float:
    """源时间戳 = metadata.file_mtime (缺/坏则 updated_at 兜底, 与 db 同口径)."""
    try:
        fmt = float(json.loads(mj).get("file_mtime") or 0) if mj else 0.0
    except (json.JSONDecodeError, TypeError, ValueError):
        fmt = 0.0
    return fmt if fmt > 0 else upd


def derive_hot_days(day_rows: dict[int, int], max_hot_rows: int, protected_old: int) -> int:
    """最大整数天 d ∈ [1, 7] 使 累计 age≤d 行数 + 越龄保护 <= max_hot_rows.

    设计书"边界不变量": float 预算 <=0.6GB(≈15万行) 是真约束, 天数是派生 UI。
    找不到 (1 天就超) 则返回 1 并如实报警 (预算被增速击穿, 需人工调 max 或模型侧减量)。
    """
    budget = max_hot_rows - protected_old
    cum = 0
    cum_at: dict[int, int] = {}   # 天数 d 处的累计行数 (含 d)
    for d in sorted(day_rows):
        cum += day_rows[d]
        cum_at[d] = cum

    def _cum_upto(d: int) -> int:
        # ≤d 的累计: 缺天沿用上一个有数天 (单调)
        best_cum = 0
        for k in sorted(cum_at):
            if k <= d:
                best_cum = cum_at[k]
            else:
                break
        return best_cum

    best = 1
    for d in range(1, 8):
        if _cum_upto(d) <= budget:
            best = d
    if _cum_upto(best) > budget:
        print(f"[WARN] hot 预算击穿: {best} 天累计 {_cum_upto(best)} > 预算 {budget} "
              f"— 按天数={best} 执行, 如实呈报")
    return best


def rebuild(db_path, hot_days_max, warm_days, max_hot_rows, probe_baseline,
            hits_ledger, recent_hit_days, dry_run):
    t0 = time.time()
    conn = get_conn(db_path)
    n_chunks = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    print(f"[1/6] chunks 总数 = {n_chunks}")

    probe_hashes = load_probe_hashes(probe_baseline)
    recent_hits = load_recent_hit_ids(hits_ledger, recent_hit_days)
    print(f"[2/6] 保护集: probe_hashes={len(probe_hashes)} recent_hit_ids={len(recent_hits)} "
          f"(窗口 {recent_hit_days}d)")

    # ── pass A: 年龄直方图 (derive hot_days) — rowid 分批 fetchall, 免开游标跨提交 ──
    now = time.time()
    day_rows: dict[int, int] = {}
    last_rowid = 0
    while True:
        rows = conn.execute(
            "SELECT rowid, metadata, updated_at FROM chunks WHERE rowid > ? ORDER BY rowid LIMIT ?",
            (last_rowid, BATCH)).fetchall()
        if not rows:
            break
        last_rowid = rows[-1][0]
        for _rid, mj, upd in rows:
            fmt = _source_ts(mj, upd)
            d = int(max(0, now - fmt) // 86400)
            day_rows[d] = day_rows.get(d, 0) + 1

    # 越龄保护量预估 (derive 预算扣减用): gold>30d — updated_at 近似, 仅预算估
    prot_old = conn.execute(
        "SELECT COUNT(*) FROM chunks WHERE json_extract(metadata,'$.layer')='gold' "
        "AND updated_at < ?", (now - warm_days * 86400,)).fetchone()[0]
    hot_days = derive_hot_days(day_rows, max_hot_rows, prot_old)
    print(f"[3/6] hot_days derive: {hot_days} (max {hot_days_max}d, 预算 {max_hot_rows} 行, "
          f"越龄保护估 {prot_old})")

    bands = {"hot": 0, "warm": 0, "cold": 0}
    reasons = {"age": 0, "gold": 0, "probe": 0, "hit": 0}
    probe_matched = 0
    if dry_run:
        for d, n in day_rows.items():
            if d <= hot_days:
                bands["hot"] += n
            elif d <= warm_days:
                bands["warm"] += n
            else:
                bands["cold"] += n
        print(f"[dry-run] 分派: {bands} (gold/probe/hit 越龄保护需全量跑, 见正式 rebuild)")
        conn.close()
        return

    # ── 建表 (幂等: 重建 = DROP 自己的 tier 表; chunks/vec_chunks 不碰) ──
    # ADR-029 (2026-09-06): DDL 收敛到 vec_ddl 单一真相源 (partition 开关
    # MEMSVC_VEC_PARTITION, 默认分区形态; 文件级回滚后重建设 =0 防静默翻回)。
    from vec_ddl import tier_vec_ddl, current_form
    print(f"[ddl] vec tier 表形态 = {current_form()} (MEMSVC_VEC_PARTITION)")
    with conn:
        for t in TIER_TABLES.values():
            conn.execute(f"DROP TABLE IF EXISTS {t}")
            conn.execute(tier_vec_ddl(t))
        conn.execute(f"DROP TABLE IF EXISTS {REGISTRY}")
        conn.execute(
            f"CREATE TABLE {REGISTRY}(chunk_id TEXT PRIMARY KEY, tier TEXT NOT NULL "
            f"CHECK(tier IN ('hot','warm','cold')), reason TEXT, assigned_at REAL)")
        conn.execute(f"DROP TABLE IF EXISTS {META_TABLE}")
        conn.execute(f"CREATE TABLE {META_TABLE}(key TEXT PRIMARY KEY, value TEXT)")

    # gold 判定复用 db.classify_layer (单一真相源; main 已把 MEMSVC_DB 指向目标库)
    import db as dbmod

    # ── pass B: 分派 + 批量灌入 (rowid 分批, 批间提交限活库锁窗) ──
    insert_sql = {
        tr: f"INSERT INTO {t}(chunk_id, embedding, collection, layer) VALUES (?,?,?,?)"
        for tr, t in TIER_TABLES.items()
    }
    reg_sql = f"INSERT OR REPLACE INTO {REGISTRY}(chunk_id, tier, reason, assigned_at) VALUES (?,?,?,?)"
    last_rowid = 0
    buffers = {"hot": [], "warm": [], "cold": []}
    reg_buf: list[tuple] = []
    total = 0
    while True:
        rows = conn.execute(
            "SELECT c.rowid, c.id, c.collection, c.metadata, c.updated_at, "
            "c.embedding, c.source, c.text FROM chunks c "
            "WHERE c.rowid > ? ORDER BY c.rowid LIMIT ?",
            (last_rowid, BATCH)).fetchall()
        if not rows:
            break
        last_rowid = rows[-1][0]
        for _rid, cid, coll, mj, upd, emb, source, text in rows:
            if emb is None or len(emb) != EMBED_BYTES:
                continue  # 坏行跳过 (与 migrate_to_vec0/双写同口径)
            layer = None
            try:
                m = json.loads(mj) if mj else {}
                fmt = float(m.get("file_mtime") or 0)
                layer = m.get("layer")
            except (json.JSONDecodeError, TypeError, ValueError):
                fmt = 0.0
            if fmt <= 0:
                fmt = upd
            age = now - fmt
            # 保护优先级 (高→低): probe > gold > recent-hit > 年龄
            # probe 目标 = 检索质量锚 (设计 §二.3 探针判"排得进吗") — 全年龄段置热:
            # 温层 bit 近似 (~96% 池召回) 对锚点目标会引入边际排位扰动 (实测 G10/G11
            # 0.80/0.60→0.60/0.20), 锚点必须走热层 float 精确路径 (~326 行, 成本≈0)。
            if probe_hashes and hashlib.sha1(
                    text.encode("utf-8", errors="replace")).hexdigest()[:16] in probe_hashes:
                tier, reason = "hot", "probe"
                probe_matched += 1
            elif age <= hot_days * 86400:
                tier, reason = "hot", "age"
            elif age <= warm_days * 86400:
                tier, reason = "warm", "age"
            elif layer == "gold" or dbmod.classify_layer(source or "") == "gold":
                # 越龄置热 (补3①): gold 判定含 90d 内未返回的老 gold — 保护类不坠冷
                tier, reason = "hot", "gold"
            elif cid in recent_hits:
                tier, reason = "hot", "hit"
            else:
                tier, reason = "cold", "age"
            buffers[tier].append((cid, emb, coll, layer or "session"))
            reg_buf.append((cid, tier, reason, now))
            bands[tier] += 1
            reasons[reason] += 1
            total += 1
        _flush(conn, insert_sql, reg_sql, buffers, reg_buf)

    with conn:
        conn.executemany(
            f"INSERT OR REPLACE INTO {META_TABLE}(key, value) VALUES (?,?)",
            [("hot_days_effective", str(hot_days)),
             ("warm_days", str(warm_days)),
             ("max_hot_rows", str(max_hot_rows)),
             ("built_at", str(now)),
             ("built_rows", str(total))])
    print(f"[4/6] 灌入完成: total={total} bands={bands} reasons={reasons} "
          f"probe_matched={probe_matched} elapsed={time.time()-t0:.0f}s")

    # ── 自检 ──
    ok, detail = verify(conn)
    print(f"[5/6] 对账: ok={ok} {detail}")
    conn.close()
    report = {
        "rebuilt_at": datetime.now().isoformat(timespec="seconds"),
        "db": db_path, "chunks_total": n_chunks, "assigned": total,
        "hot_days_effective": hot_days, "warm_days": warm_days,
        "bands": bands, "reasons": reasons, "probe_matched": probe_matched,
        "verify_ok": ok, "verify": detail, "elapsed_s": round(time.time() - t0, 1),
    }
    # CC 条件A (8/19): 输出路径参数化 — 默认带时间戳防覆盖证据链; --report PATH 可显式指定
    import argparse as _ap
    _rp = None
    if "--report" in sys.argv:
        _rp = sys.argv[sys.argv.index("--report") + 1]
    out = Path(_rp) if _rp else (Path(__file__).parent.parent / "data" / "research"
                                 / f"tier_rebuild_report_{datetime.now():%Y%m%d_%H%M%S}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"[6/6] 报告 → {out}")
    if not ok:
        sys.exit(2)


def _flush(conn, insert_sql, reg_sql, buffers, reg_buf):
    with conn:
        for tr, buf in buffers.items():
            if buf:
                conn.executemany(insert_sql[tr], buf)
                buf.clear()
        if reg_buf:
            conn.executemany(reg_sql, reg_buf)
            reg_buf.clear()


def verify(conn) -> tuple[bool, dict]:
    n_chunks = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    counts = {tr: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for tr, t in TIER_TABLES.items()}
    n_vec = sum(counts.values())
    d = {"chunks": n_chunks, "tiers": counts, "sum": n_vec}
    ok = n_vec == n_chunks
    # 跨表重复 / 覆盖差集 (物化临时表, PK 索引)
    conn.execute(f"CREATE TEMP TABLE tchk(chunk_id TEXT PRIMARY KEY, tier TEXT)")
    for tr, t in TIER_TABLES.items():
        conn.execute(f"INSERT OR IGNORE INTO tchk SELECT chunk_id, '{tr}' FROM {t}")
    n_dedup = conn.execute("SELECT COUNT(*) FROM tchk").fetchone()[0]
    d["cross_tier_dup"] = n_vec - n_dedup
    ok = ok and d["cross_tier_dup"] == 0
    miss = conn.execute(
        "SELECT COUNT(*) FROM chunks c WHERE NOT EXISTS (SELECT 1 FROM tchk v WHERE v.chunk_id=c.id)"
    ).fetchone()[0]
    orphan = conn.execute(
        "SELECT COUNT(*) FROM tchk v WHERE NOT EXISTS (SELECT 1 FROM chunks c WHERE c.id=v.chunk_id)"
    ).fetchone()[0]
    d["id_missing"], d["id_orphan"] = miss, orphan
    ok = ok and miss == 0 and orphan == 0
    n_reg = conn.execute(f"SELECT COUNT(*) FROM {REGISTRY}").fetchone()[0]
    reg_mm = conn.execute(
        f"SELECT COUNT(*) FROM tchk v WHERE NOT EXISTS (SELECT 1 FROM {REGISTRY} r "
        f"WHERE r.chunk_id=v.chunk_id AND r.tier=v.tier)").fetchone()[0]
    d["registry"], d["registry_mismatch"] = n_reg, reg_mm
    ok = ok and n_reg == n_dedup and reg_mm == 0
    # 保护不变量: gold 不得坠冷层 (layered 读路径 gold 段依赖 + 补3 保护)
    gold_cold = conn.execute(
        f"SELECT COUNT(*) FROM {TIER_TABLES['cold']} WHERE layer='gold'").fetchone()[0]
    d["gold_in_cold"] = gold_cold
    ok = ok and gold_cold == 0
    d["reasons"] = dict(conn.execute(
        f"SELECT reason, COUNT(*) FROM {REGISTRY} GROUP BY reason").fetchall())
    # 距离抽检: 20 条 self-MATCH 最近邻应是自己且距离 ≈ 0
    # (vec0 0.1.9 PK 过滤+MATCH 组合要求 k=? 约束 — 改纯 KNN 取 top1 对照 id)
    spot = 0
    bad_spot = 0
    for (cid, emb) in conn.execute(
            f"SELECT chunk_id, embedding FROM {TIER_TABLES['hot']} LIMIT 20").fetchall():
        r = conn.execute(
            f"SELECT chunk_id, distance FROM {TIER_TABLES['hot']} "
            f"WHERE embedding MATCH ? ORDER BY distance LIMIT 1",
            (emb,)).fetchone()
        spot += 1
        if not r or r[0] != cid or r[1] > 1e-3:
            bad_spot += 1
    d["spot_checked"], d["spot_bad"] = spot, bad_spot
    ok = ok and bad_spot == 0
    conn.execute("DROP TABLE IF EXISTS tchk")
    return ok, d


def sync_stragglers(db_path, wait_stable: bool) -> None:
    """收敛 rebuild 窗口期写入: chunks 有而 tier 表无 → hot; tier 表有而 chunks 无 → 删."""
    conn = get_conn(db_path)
    stable_rounds = 0
    rounds = 0
    while True:
        rounds += 1
        with conn:
            added = 0
            last_rowid = 0
            while True:
                rows = conn.execute(
                    "SELECT c.rowid, c.id, c.collection, c.embedding, "
                    "json_extract(c.metadata,'$.layer') FROM chunks c "
                    f"WHERE c.rowid > ? AND NOT EXISTS "
                    f"(SELECT 1 FROM {REGISTRY} r WHERE r.chunk_id = c.id) "
                    "ORDER BY c.rowid LIMIT ?", (last_rowid, BATCH)).fetchall()
                if not rows:
                    break
                last_rowid = rows[-1][0]
                conn.executemany(
                    f"INSERT OR REPLACE INTO {TIER_TABLES['hot']}"
                    "(chunk_id, embedding, collection, layer) VALUES (?,?,?,?)",
                    [(r[1], r[3], r[2], r[4] or "session") for r in rows if
                     r[3] is not None and len(r[3]) == EMBED_BYTES])
                conn.executemany(
                    f"INSERT OR REPLACE INTO {REGISTRY}(chunk_id, tier, reason, assigned_at) "
                    f"VALUES (?, 'hot', 'straggler', ?)", [(r[1], time.time()) for r in rows])
                added += len(rows)
            removed = 0
            for t in TIER_TABLES.values():
                gone = [r[0] for r in conn.execute(
                    f"SELECT chunk_id FROM {t} WHERE NOT EXISTS "
                    f"(SELECT 1 FROM chunks c WHERE c.id = {t}.chunk_id)").fetchall()]
                for i in range(0, len(gone), 900):
                    b = gone[i:i + 900]
                    ph = ",".join("?" * len(b))
                    conn.execute(f"DELETE FROM {t} WHERE chunk_id IN ({ph})", b)
                    conn.execute(f"DELETE FROM {REGISTRY} WHERE chunk_id IN ({ph})", b)
                removed += len(gone)
        print(f"[sync] round {rounds}: +{added} (窗口期写入→hot) -{removed} (已删孤儿)")
        if added == 0 and removed == 0:
            stable_rounds += 1
            if not wait_stable or stable_rounds >= 2:
                break
        else:
            stable_rounds = 0
        if wait_stable:
            time.sleep(35)  # 跨过运行服务的 60s 布局 TTL + 缓冲
        if rounds > 20:
            print("[sync] 20 轮未收敛, 退出 (人工检查)")
            break
    conn.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default=os.environ.get(
        "MEMSVC_DB", str(Path(__file__).resolve().parent.parent / "memory.db")))
    p.add_argument("--hot-days", type=int, default=7, help="热层天数上限 (derive 只降不升)")
    p.add_argument("--warm-days", type=int, default=30)
    p.add_argument("--max-hot-rows", type=int,
                   default=int(os.environ.get("MEMSVC_TIER_HOT_MAX_ROWS", "150000")),
                   help="热层行数预算 (≈0.6GB float, 设计书边界不变量)")
    p.add_argument("--probe-baseline", default=DEFAULT_PROBE_BASELINE)
    p.add_argument("--hits-ledger", default=DEFAULT_HITS_LEDGER)
    p.add_argument("--recent-hit-days", type=int, default=90)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verify-only", action="store_true")
    p.add_argument("--sync-stragglers", action="store_true")
    p.add_argument("--wait-stable", action="store_true",
                   help="sync 模式: 循环到 2 轮零增量 (活库部署用)")
    args = p.parse_args()

    # 先钉死 MEMSVC_DB 再触发任何 db 模块加载 (classify_layer 复用, 连接必须指向目标库)
    os.environ["MEMSVC_DB"] = args.db

    # T-MEMDB-COLD-SPLIT 守闸: 分库 active 时禁跑 — rebuild 会把 cold 行重新物化回
    # 主库三表 (违反"冷行归冷库"归属, 检索冷层走冷库=行不可见)。回退流程:
    # 先 scripts/cold_split.py rollback (state→off) 再 rebuild, 或重建后重跑 migrate。
    try:
        _chk = sqlite3.connect(args.db, timeout=30)
        _st = _chk.execute(
            "SELECT value FROM cold_split_meta WHERE key='state'").fetchone()
        _chk.close()
        if _st and str(_st[0]) == "active":
            print("[tier_rebuild] 拒跑: cold 分库 state=active — rebuild 会把冷行物化回主库 "
                  "(违反冷库归属)。需先 scripts/cold_split.py rollback 再 rebuild。",
                  file=sys.stderr)
            sys.exit(2)
    except sqlite3.OperationalError:  # cold_split_meta 表不存在 = 未分库 (命名: 吞的是无表)
        pass

    if args.sync_stragglers:
        sync_stragglers(args.db, args.wait_stable)
        return
    conn = get_conn(args.db)
    if args.verify_only:
        ok, d = verify(conn)
        print(json.dumps({"ok": ok, **d}, ensure_ascii=False))
        sys.exit(0 if ok else 2)
    conn.close()
    rebuild(args.db, args.hot_days, args.warm_days, args.max_hot_rows,
            args.probe_baseline, args.hits_ledger, args.recent_hit_days, args.dry_run)


if __name__ == "__main__":
    main()
