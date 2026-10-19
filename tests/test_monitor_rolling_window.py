"""T-MEMSVC-DEGRADED-FIX 单测: 滚动窗口 slow 计数 + degraded 判定。

运行 (venv 无 pytest, 用 unittest; flash执行审D-C):
    venv/bin/python test_monitor_rolling_window.py -v
E2E (判据2): bash test_e2e_rolling_window.sh

覆盖 (任务书判据1, >=8 例):
 1. 窗内计数: 窗内事件计入 slow_count
 2. 窗外滚出: 过期事件被剪枝, slow_count 下降 (粘性根治)
 3. min_slow 门: 窗内 1 次 slow 不触发 (旧逻辑会永久 degraded)
 4. ratio 门: 2 slow / 100 总 (2%<=5%) 不触发
 5. min_samples 门: 2 slow / 5 总 (样本<10) 不触发
 6. 组合触发: 2 slow / 20 总 (10%>5%) 触发 degraded
 7. 阈值边界: ==8000ms 不算 slow (严格大于)
 8. 逐条日志: slow 事件产生 [slow-query] warning (含 project/query)
 9. slow_recent 字段: 最近 slow 事件带 ts/duration_ms/project
10. 自动恢复: 触发 degraded 后窗口滚出 → 判定转阴 (无需重启)
"""

import logging
import sys
import time
import unittest
from unittest import mock

sys.path.insert(0, ".")

import monitor  # noqa: E402
from config import MONITOR  # noqa: E402


class RollingWindowTest(unittest.TestCase):
    def setUp(self):
        """每个用例独立事件流, 免互相污染; 保留默认参数 (8000ms/3600s/2/5%/10)。"""
        self._saved = list(monitor._search_events)
        monitor._search_events.clear()
        self.addCleanup(self._restore)

    def _restore(self):
        monitor._search_events.clear()
        monitor._search_events.extend(self._saved)

    def _record(self, ms, project="demo", ts=None):
        """直接注入事件 (绕过 time.time 便于造窗外旧事件)。"""
        with monitor._metrics_lock:
            if ts is None:
                ts = time.time()
            monitor._search_events.append((ts, ms, project))

    def _degraded(self, metrics=None):
        """复刻 server.py _handle_health 的窗口判定 (单一语义源对齐)。"""
        m = metrics or monitor.get_metrics()
        s = m["search"]
        return (
            s["window_count"] >= MONITOR.degraded_min_samples
            and s["slow_count"] >= MONITOR.degraded_min_slow
            and s["slow_ratio"] > MONITOR.degraded_slow_ratio
        )

    # 1. 窗内计数
    def test_window_counts_in_window_events(self):
        for _ in range(12):
            self._record(9000)
        m = monitor.get_metrics()
        self.assertEqual(m["search"]["slow_count"], 12)
        self.assertEqual(m["search"]["window_count"], 12)

    # 2. 窗外滚出 (粘性根治)
    def test_expired_events_pruned(self):
        now = time.time()
        self._record(9000, ts=now - MONITOR.slow_window_seconds - 1)  # 窗外
        self._record(100, ts=now - MONITOR.slow_window_seconds - 1)   # 窗外
        for _ in range(10):
            self._record(100)  # 窗内, 全快
        m = monitor.get_metrics()
        self.assertEqual(m["search"]["slow_count"], 0)  # 旧代码这里是 1 (永久粘住)
        self.assertEqual(m["search"]["window_count"], 10)

    # 3. min_slow 门: 单次 slow 不触发
    def test_single_slow_no_degrade(self):
        self._record(9000)
        for _ in range(49):
            self._record(100)
        self.assertFalse(self._degraded())  # 1/50, 旧逻辑 slow_count>0 即 degraded

    # 4. ratio 门: 2/100 = 2% <= 5% 不触发
    def test_ratio_gate_blocks_low_ratio(self):
        self._record(9000)
        self._record(9000)
        for _ in range(98):
            self._record(100)
        self.assertFalse(self._degraded())

    # 5. min_samples 门: 2/5 样本不足不触发 (防小样本误判)
    def test_min_samples_gate_blocks_sparse_window(self):
        self._record(9000)
        self._record(9000)
        for _ in range(3):
            self._record(100)
        m = monitor.get_metrics()
        self.assertEqual(m["search"]["slow_ratio"], 0.4)  # 比率确实高
        self.assertFalse(self._degraded())  # 但样本 <10 不判

    # 6. 组合触发: 2/20 = 10% > 5%, 样本足
    def test_combined_condition_triggers(self):
        self._record(9000)
        self._record(12000)
        for _ in range(18):
            self._record(100)
        self.assertTrue(self._degraded())

    # 7. 阈值边界: ==8000 不算 slow
    def test_threshold_boundary_exclusive(self):
        for _ in range(10):
            self._record(MONITOR.slow_threshold_ms)  # 恰好等于
        m = monitor.get_metrics()
        self.assertEqual(m["search"]["slow_count"], 0)

    # 8. 逐条 slow 日志
    def test_slow_query_logged_with_context(self):
        with self.assertLogs("memory-service.monitor", level="WARNING") as cm:
            monitor.record_request(
                "/api/search", 9500.0, True,
                {"project": "opencode_sessions", "query": "测试查询ABC"},
            )
        self.assertTrue(any(
            "[slow-query]" in line and "opencode_sessions" in line and "测试查询ABC" in line
            for line in cm.output
        ))

    # 9. slow_recent 字段
    def test_slow_recent_fields(self):
        monitor.record_request("/api/search", 8100.0, True, {"project": "demo", "query": "q"})
        monitor.record_request("/api/search", 200.0, True, {"project": "demo", "query": "q"})
        m = monitor.get_metrics()
        self.assertEqual(len(m["search"]["slow_recent"]), 1)
        entry = m["search"]["slow_recent"][0]
        self.assertEqual(entry["project"], "demo")
        self.assertEqual(entry["duration_ms"], 8100.0)
        self.assertIn("ts", entry)

    # 10. 自动恢复: degraded → 窗口滚出 → 转阴
    def test_auto_recovery_after_window_rolls(self):
        self._record(9000)
        self._record(9000)
        for _ in range(18):
            self._record(100)
        self.assertTrue(self._degraded())  # 先触发
        now = time.time()
        # 全部事件滚出窗外 (用新窗口事件顶替: 10 条窗内快查)
        monitor._search_events.clear()
        for _ in range(10):
            self._record(100, ts=now)
        self.assertFalse(self._degraded())  # 无需重启自愈


if __name__ == "__main__":
    unittest.main(verbosity=2)
