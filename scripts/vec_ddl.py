#!/usr/bin/env python3
"""vec_ddl.py — ADR-029: vec0 虚表 DDL 单一真相源 + partition 开关 (2026-09-06).

背景: ADR-029 (decisions/ADR-029-vec0-partition-key-rebuild.md, CC终审 ACCEPT c137b2db)
以 collection 作 partition key 重建三张 tier vec0 虚表, 使 `WHERE collection=? AND
embedding MATCH` 从全表遍历后滤变为分区剪枝后扫 (staging 实测巨头 1.67-1.92×,
中小 8.4-362×)。

DDL 三处调用点 (tier_rebuild.py / cold_split.py / migrate_to_vec0.py) 统一 import
本模块, 消灭三份手写 DDL 漂移 (对侧审 D13: 文件级回滚后 "下一次任何人跑重建工具
就静默翻回分区形态" — 现在回滚只需设 env, 无需回退代码)。

开关 (回滚层级②配套, D13 修法):
    MEMSVC_VEC_PARTITION=1 (默认)  → partition key 形态 (ADR-029 目标 DDL)
    MEMSVC_VEC_PARTITION=0/off     → 旧非分区形态 (np, 文件级回滚后重建用)

T11 约束 (staging micro_tests.log 实锤): vec0 0.1.9 RENAME 两形态均破 (影子表不随
改名) — 本模块所有路径禁 RENAME, 切换只走 DROP+CREATE+回灌 (ADR 切换机制)。
"""
import os

# 与 db.py:3062-3064 / tier_rebuild.py TIER_TABLES 同源的三张 tier 表名
TIER_TABLES = ("vec_chunks_hot", "vec_chunks_warm", "vec_chunks_cold")


def partition_enabled() -> bool:
    """MEMSVC_VEC_PARTITION 开关 (默认开: ADR-029 落地后分区形态是基线)."""
    v = os.environ.get("MEMSVC_VEC_PARTITION", "1").strip().lower()
    return v not in ("0", "off", "false", "no")


def tier_vec_ddl(table: str) -> str:
    """建一张 tier vec0 虚表的 CREATE VIRTUAL TABLE 语句 (按开关出形态).

    注意 vec0 0.1.9 语法: partition key 列声明为 `collection TEXT partition key`
    (sqlite-vec 0.1.9 原生支持, 本机 staging T1 实测可建可查)。
    """
    coll = "collection TEXT partition key" if partition_enabled() else "collection TEXT"
    return (f"CREATE VIRTUAL TABLE IF NOT EXISTS {table} USING vec0("
            f"chunk_id TEXT PRIMARY KEY, "
            f"embedding float[1024] distance_metric=cosine, "
            f"{coll}, layer TEXT)")


def legacy_vec_ddl() -> str:
    """旧单层布局 vec_chunks 表 DDL (migrate_to_vec0 重建用, 同开关)."""
    coll = "collection TEXT partition key" if partition_enabled() else "collection TEXT"
    return (f"CREATE VIRTUAL TABLE IF NOT EXISTS vec_chunks USING vec0("
            f"chunk_id TEXT PRIMARY KEY, "
            f"embedding float[1024] distance_metric=cosine, "
            f"{coll}, layer TEXT)")


def current_form() -> str:
    return "partition" if partition_enabled() else "non-partition"
