#!/usr/bin/env python3
"""选点开销基准：对比优化前后的队首/回填选点成本。

对每种机架规模，先把机架占满 99.9%（成片占用），再分别测量：

- 优化前（整架扫描 + 全量排序，即旧 ``_earliest_on_rack`` 的算法）；
- 优化后（空闲堆 + 释放堆，即当前 ``Scheduler`` 提交小作业的完整路径）。

用法：
    python scripts/bench_selection.py
仅依赖项目自身代码与标准库。
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import Settings
from app.scheduler import Scheduler

SCALES = [10_000, 100_000, 1_000_000]
SMALL_JOBS = 200          # 优化后每种规模提交的小作业数
NAVE_REPS = 3             # 优化前算法每种规模重复次数（太慢，只测几次取平均）


def naive_earliest_on_rack(owners: dict[int, int], jobs: dict,
                           cap: int, now: int, need: int) -> int:
    """优化前的选点算法：对整架 cap 个节点生成列表并排序。"""
    def free_at(n: int) -> int:
        owner_id = owners.get(n)
        if owner_id is None:
            return now
        owner = jobs[owner_id]
        return owner.actual_start + owner.duration

    chosen = sorted((free_at(n), n) for n in range(cap))[:need]
    return max(now, max(t for t, _ in chosen))


def bench(scale: int) -> None:
    free_target = SMALL_JOBS + 10
    s = Scheduler(Settings(rack_capacities=(scale,)))
    s.submit(scale - free_target, 10_000)   # 成片占用，只留 free_target 个空位

    # ---- 优化前：旧算法单次选点成本（随机架容量线性增长）
    t0 = time.perf_counter()
    for _ in range(NAVE_REPS):
        naive_earliest_on_rack(s.owners["r1"], s.jobs, scale, s.now, 1)
    old_ms = (time.perf_counter() - t0) / NAVE_REPS * 1000

    # ---- 优化后：完整提交路径（队首选架 + 选点 + 开工）
    t0 = time.perf_counter()
    for _ in range(SMALL_JOBS):
        s.submit(1, 5)
    new_ms = (time.perf_counter() - t0) / SMALL_JOBS * 1000

    # ---- 填满剩余空位后：队首预约（释放堆查询）
    for _ in range(free_target - SMALL_JOBS):
        s.submit(1, 5)
    assert s._free_count("r1") == 0
    t0 = time.perf_counter()
    waiting, _ = s.submit(1, 10)
    reserve_ms = (time.perf_counter() - t0) * 1000

    print(f"机架 {scale:>9,} 节点 | 旧算法 {old_ms:9.2f} ms/次 | "
          f"小作业提交 {new_ms:7.3f} ms/次 | 满架预约 {reserve_ms:6.3f} ms")
    assert waiting.scheduled_start == 5


def main() -> None:
    print("选点开销基准（占用 99.9% 的机架上提交 1 节点小作业）")
    for scale in SCALES:
        bench(scale)


if __name__ == "__main__":
    main()
