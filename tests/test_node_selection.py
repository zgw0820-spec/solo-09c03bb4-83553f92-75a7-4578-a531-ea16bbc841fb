"""选点优化专项测试。

1. 差分模糊测试：把优化前的原始选点实现（整架扫描 + 全量排序）完整保留为
   ``ReferenceScheduler``，与优化后的 ``Scheduler`` 跑同一随机操作序列
   （提交/推进/完工/重置，覆盖跨时刻释放与空位变化），逐步比对全部可观测
   状态——作业字段、队列、预约、节点归属与事件流必须完全一致。
2. 大机架性能测试：百万节点机架、成片占用的场景下，提交小作业的选点开销
   不再随空闲容量增长。
"""

from __future__ import annotations

import heapq
import itertools
import random
import time
from dataclasses import dataclass, field

import pytest

from app.config import Settings
from app.scheduler import Event, Job, Scheduler, SchedulerError

# ---------------------------------------------------------------- 原始实现基准


class ReferenceScheduler:
    """优化前的调度器（逐节点扫描选点），作为语义基准逐字保留。"""

    MAX_EVENTS = 200

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.rack_names: list[str] = list(settings.rack_names)
        self.capacities: dict[str, int] = dict(
            zip(settings.rack_names, settings.rack_capacities, strict=True)
        )
        self.now: int = 0
        self.jobs: dict[int, Job] = {}
        self.queue: list[int] = []
        self.owners: dict[str, dict[int, int]] = {
            rack: {} for rack in self.rack_names
        }
        self._timeout_heap: list[tuple[int, int, int]] = []
        self._seq = itertools.count()
        self._next_id = 1
        self._events: list[Event] = []

    def _log(self, type_: str, detail: str, job_id=None, rack=None) -> Event:
        event = Event(time=self.now, type=type_, detail=detail,
                      job_id=job_id, rack=rack)
        self._events.append(event)
        if len(self._events) > self.MAX_EVENTS:
            del self._events[: -self.MAX_EVENTS]
        return event

    def _job(self, job_id: int) -> Job:
        job = self.jobs.get(job_id)
        if job is None:
            raise SchedulerError(404, f"作业 {job_id} 不存在")
        return job

    def _node_free_at(self, rack: str, node: int) -> int:
        owner_id = self.owners[rack].get(node)
        if owner_id is None:
            return self.now
        owner = self.jobs[owner_id]
        assert owner.actual_start is not None
        return owner.actual_start + owner.duration

    def _earliest_on_rack(self, rack: str, need: int) -> tuple[int, list[int]]:
        cap = self.capacities[rack]
        free_at = sorted(
            (self._node_free_at(rack, n), n) for n in range(cap)
        )
        chosen = free_at[:need]
        earliest = max(self.now, max(t for t, _ in chosen))
        return earliest, [n for _, n in chosen]

    def _free_nodes_now(self, rack: str) -> list[int]:
        return [n for n in range(self.capacities[rack])
                if n not in self.owners[rack]]

    def _start(self, job: Job, rack: str, nodes: list[int], kind: str) -> Event:
        assert job.status == "queued"
        owner_map = self.owners[rack]
        for n in nodes:
            if n in owner_map:
                raise SchedulerError(409, f"节点 {rack}[{n}] 已被占用")
        for n in nodes:
            owner_map[n] = job.job_id
        job.status = "running"
        job.rack = rack
        job.assigned_nodes = sorted(nodes)
        job.actual_start = self.now
        self.queue.remove(job.job_id)
        heapq.heappush(
            self._timeout_heap,
            (self.now + job.duration, next(self._seq), job.job_id),
        )
        labels = {
            "direct": "直接开工",
            "backfill": "回填开工",
            "reservation": "按预约开工",
            "early": "提前于预约开工",
        }
        return self._log("job_started",
                         f"作业 {job.job_id} 在机架 {rack} 节点 {sorted(nodes)} "
                         f"{labels[kind]}，时限 {job.duration}，"
                         f"预计释放时刻 {self.now + job.duration}",
                         job.job_id, rack)

    def reschedule(self) -> list[Event]:
        events: list[Event] = []
        while self.queue:
            head = self.jobs[self.queue[0]]
            best = self._earliest_start(head)
            earliest, rack, nodes = best
            if earliest > self.now:
                self._publish_reservation(head, earliest, rack, nodes, events)
                events.extend(self._backfill_pass(head))
                break
            kind = "direct" if head.scheduled_start is None else (
                "early" if head.scheduled_start > self.now else "reservation"
            )
            events.append(self._start(head, rack, nodes, kind))
        return events

    def _earliest_start(self, job: Job) -> tuple[int, str, list[int]]:
        candidates: list[tuple[int, int, str, list[int]]] = []
        for idx, rack in enumerate(self.rack_names):
            if self.capacities[rack] < job.nodes:
                continue
            earliest, nodes = self._earliest_on_rack(rack, job.nodes)
            candidates.append((earliest, idx, rack, nodes))
        if not candidates:
            biggest = max(self.capacities.values())
            raise SchedulerError(
                422,
                f"作业需要 {job.nodes} 个同机架节点，"
                f"最大机架仅 {biggest} 个节点，无法调度",
            )
        earliest, _, rack, nodes = min(candidates, key=lambda c: (c[0], c[1]))
        return earliest, rack, nodes

    def _publish_reservation(self, head: Job, earliest: int, rack: str,
                             nodes: list[int], events: list[Event]) -> None:
        old = head.scheduled_start
        new_time = earliest if old is None else min(old, earliest)
        head.scheduled_start = new_time
        head.rack = rack
        head.assigned_nodes = sorted(nodes)
        if old is None:
            events.append(self._log(
                "reservation_published",
                f"队首作业 {head.job_id} 需等待，公布最早开工时刻 {new_time}，"
                f"预约机架 {rack} 节点 {sorted(nodes)}，"
                f"预计释放时刻 {new_time + head.duration}",
                head.job_id, rack,
            ))
        elif new_time < old:
            events.append(self._log(
                "reservation_updated",
                f"作业 {head.job_id} 预约开工时刻提前：{old} -> {new_time}",
                head.job_id, rack,
            ))

    def _backfill_pass(self, head: Job) -> list[Event]:
        events: list[Event] = []
        assert head.scheduled_start is not None
        reserved_rack = head.rack
        reserved_nodes = set(head.assigned_nodes)
        for job_id in list(self.queue[1:]):
            job = self.jobs[job_id]
            if job.status != "queued":
                continue
            if self.now + job.duration > head.scheduled_start:
                continue
            for rack in self.rack_names:
                if self.capacities[rack] < job.nodes:
                    continue
                free_now = self._free_nodes_now(rack)
                if rack == reserved_rack:
                    free_now = [n for n in free_now
                                if n not in reserved_nodes]
                if len(free_now) >= job.nodes:
                    picked = sorted(free_now[: job.nodes])
                    events.append(self._start(job, rack, picked, "backfill"))
                    break
        return events

    def submit(self, nodes: int, duration: int) -> tuple[Job, list[Event]]:
        max_cap = max(self.capacities.values())
        if nodes > max_cap:
            raise SchedulerError(
                422,
                f"作业需要 {nodes} 个同机架节点，最大机架仅 {max_cap} 个",
            )
        job = Job(
            job_id=self._next_id,
            nodes=nodes,
            duration=duration,
            submit_time=self.now,
        )
        self._next_id += 1
        self.jobs[job.job_id] = job
        self.queue.append(job.job_id)
        events = [self._log(
            "job_submitted",
            f"提交作业 {job.job_id}：需 {nodes} 节点、时限 {duration}",
            job.job_id,
        )]
        events.extend(self.reschedule())
        return job, events

    def _release(self, job: Job, reason: str, at: int) -> Event:
        assert job.status == "running"
        rack = job.rack
        assert rack is not None
        owner_map = self.owners[rack]
        for n in job.assigned_nodes:
            if owner_map.get(n) == job.job_id:
                del owner_map[n]
        job.status = "finished" if reason == "completed" else "killed"
        job.finish_time = at
        job.finish_reason = reason
        etype = "job_completed" if reason == "completed" else "job_timeout"
        label = "完工释放" if reason == "completed" else "达到时限被杀并释放"
        return self._log(
            etype,
            f"作业 {job.job_id} 于时刻 {at} {label}机架 {rack} 节点 "
            f"{job.assigned_nodes}",
            job.job_id, rack,
        )

    def report_completion(self, job_id: int, at: int) -> tuple[Job, list[Event]]:
        job = self._job(job_id)
        if job.status != "running":
            raise SchedulerError(
                409,
                f"作业 {job_id} 当前状态为 {job.status}，完工回报无效；"
                "迟到的回报不会释放已转配的节点",
            )
        if at > self.now:
            raise SchedulerError(
                409,
                f"当前模拟时刻为 {self.now}，完工回报时刻 {at} 不能晚于当前时刻",
            )
        assert job.actual_start is not None
        if at < job.actual_start:
            raise SchedulerError(409, "完工时刻早于实际开工时刻")
        if at > job.actual_start + job.duration:
            raise SchedulerError(
                409,
                f"完工时刻 {at} 已超过运行时限 "
                f"{job.actual_start + job.duration}，作业应被时限杀掉",
            )
        events = [self._release(job, "completed", at)]
        events.extend(self.reschedule())
        return job, events

    def _process_timeouts(self, until: int) -> list[Event]:
        events: list[Event] = []
        while self._timeout_heap and self._timeout_heap[0][0] <= until:
            t = self._timeout_heap[0][0]
            due_ids: list[int] = []
            while self._timeout_heap and self._timeout_heap[0][0] == t:
                _, _, jid = heapq.heappop(self._timeout_heap)
                due_ids.append(jid)
            self.now = t
            for jid in due_ids:
                job = self.jobs[jid]
                if job.status != "running":
                    continue
                if job.actual_start + job.duration != t:
                    continue
                events.append(self._release(job, "killed", t))
            events.extend(self.reschedule())
        return events

    def advance(self, steps: int) -> tuple[int, int, list[Event]]:
        if steps <= 0:
            raise SchedulerError(422, "steps 必须为正整数")
        start = self.now
        target = start + steps
        events = self._process_timeouts(target)
        if self.now < target:
            self.now = target
            events.extend(self.reschedule())
        return start, target, events

    def reset(self) -> None:
        self.now = 0
        self.jobs.clear()
        self.queue.clear()
        for m in self.owners.values():
            m.clear()
        self._timeout_heap.clear()
        self._seq = itertools.count()
        self._next_id = 1
        self._events.clear()
        self._log("sim_reset", "模拟环境已重置")

    def reservations(self) -> list[dict]:
        out = []
        for jid in self.queue:
            job = self.jobs[jid]
            if job.scheduled_start is not None:
                out.append({
                    "job_id": job.job_id,
                    "rack": job.rack,
                    "nodes": list(job.assigned_nodes),
                    "scheduled_start": job.scheduled_start,
                    "deadline": job.scheduled_start + job.duration,
                })
        return out


# ------------------------------------------------------------------ 差分比对


def _snapshot(s) -> tuple:
    """提取调度器全部可观测状态，用于逐操作比对。"""
    jobs = {
        jid: (
            j.job_id, j.nodes, j.duration, j.submit_time, j.status, j.rack,
            tuple(j.assigned_nodes), j.scheduled_start, j.actual_start,
            j.finish_time, j.finish_reason,
        )
        for jid, j in s.jobs.items()
    }
    events = [
        (e.time, e.type, e.detail, e.job_id, e.rack) for e in s._events
    ]
    owners = {rack: dict(m) for rack, m in s.owners.items()}
    return (s.now, tuple(s.queue), jobs, owners,
            tuple(map(tuple, s.reservations())), tuple(events))


def _run_ops(s, ops: list[tuple]) -> None:
    for op in ops:
        kind = op[0]
        if kind == "submit":
            s.submit(op[1], op[2])
        elif kind == "advance":
            s.advance(op[1])
        elif kind == "complete":
            s.report_completion(op[1], op[2])
        elif kind == "reset":
            s.reset()
        else:  # pragma: no cover
            raise AssertionError(f"未知操作 {op}")


def _gen_ops(rng: random.Random, racks: tuple[int, ...],
             steps: int) -> list[tuple]:
    """生成随机操作序列；完工回报的时刻在合法区间内选取。"""
    probe = Scheduler(Settings(rack_capacities=racks))
    ops: list[tuple] = []
    max_nodes = max(racks)
    for _ in range(steps):
        kind = rng.choices(
            ["submit", "advance", "complete", "reset"],
            weights=[55, 25, 18, 2],
        )[0]
        if kind == "submit":
            op = ("submit", rng.randint(1, max_nodes), rng.randint(1, 12))
        elif kind == "advance":
            op = ("advance", rng.randint(1, 8))
        elif kind == "complete":
            running = [j for j in probe.jobs.values() if j.status == "running"]
            if not running:
                op = ("advance", rng.randint(1, 4))
            else:
                j = rng.choice(running)
                assert j.actual_start is not None
                hi = min(probe.now, j.actual_start + j.duration)
                op = ("complete", j.job_id, rng.randint(j.actual_start, hi))
        else:
            op = ("reset",)
        ops.append(op)
        _run_ops(probe, [op])  # 探针同步演进，保证后续完工时刻合法
    return ops


@pytest.mark.parametrize("racks", [(4, 4), (3, 5, 2), (6,), (1, 1, 1, 1),
                                   (2, 3)])
@pytest.mark.parametrize("seed", [1, 7, 42, 2026, 31337, 999])
def test_differential_against_reference(racks: tuple[int, ...], seed: int):
    rng = random.Random(seed)
    ops = _gen_ops(rng, racks, steps=300)
    new = Scheduler(Settings(rack_capacities=racks))
    ref = ReferenceScheduler(Settings(rack_capacities=racks))
    for i, op in enumerate(ops):
        _run_ops(new, [op])
        _run_ops(ref, [op])
        assert _snapshot(new) == _snapshot(ref), (
            f"第 {i} 步操作 {op} 后状态分叉（racks={racks}, seed={seed}）"
        )


def test_differential_covers_interesting_paths():
    """确认模糊测试确实覆盖了跨时刻释放、预约更新、回填与重置。"""
    racks = (2, 3)
    rng = random.Random(1)
    ops = _gen_ops(rng, racks, steps=300)
    new = Scheduler(Settings(rack_capacities=racks))
    _run_ops(new, ops)
    types = {e.type for e in new._events}
    assert "job_timeout" in types                 # 跨时刻时限释放
    assert "job_completed" in types               # 完工回报改变空位
    assert "reservation_published" in types       # 队首预约
    assert "reservation_updated" in types         # 预约时刻提前
    assert any(e.type == "job_started" and "回填" in e.detail
               for e in new._events)              # 回填开工
    assert "sim_reset" in types                   # 重置后重新选点


# ------------------------------------------------------------------ 大机架性能

BIG_RACK = 1_000_000


def test_million_node_rack_small_job_selection_is_fast():
    """百万节点机架、99.9% 成片占用时，小作业选点应近乎常数时间。"""
    s = Scheduler(Settings(rack_capacities=(BIG_RACK,)))
    big, _ = s.submit(BIG_RACK - 1000, 10_000)   # 占满前 999000 节点
    assert big.status == "running"

    t0 = time.perf_counter()
    ids = [s.submit(1, 5)[0].job_id for _ in range(1000)]
    elapsed = time.perf_counter() - t0
    # 优化前每次提交要对 100 万节点排序（实测约 600ms/次）；
    # 这里放宽到 2 秒总量，仍有三个数量级余量
    assert elapsed < 2.0, f"1000 次小作业提交耗时 {elapsed:.2f}s，超出预算"
    assert all(s.jobs[i].status == "running" for i in ids)
    # 恰好占满剩余空位：节点号最小的 1000 个空位
    assert s.jobs[ids[0]].assigned_nodes == [BIG_RACK - 1000]
    assert s.jobs[ids[-1]].assigned_nodes == [BIG_RACK - 1]

    # 机架已满：下一个小作业成为等待队首，预约应指向最早释放时刻 t=5
    t0 = time.perf_counter()
    waiting, _ = s.submit(1, 10)
    reserve_elapsed = time.perf_counter() - t0
    assert reserve_elapsed < 0.5
    assert waiting.status == "queued"
    assert waiting.scheduled_start == 5
    assert waiting.assigned_nodes == [BIG_RACK - 1000]

    # 回填：跑满时限不晚于 t=5 的作业可立即占空位……但当前无空位，排队
    t0 = time.perf_counter()
    s.advance(5)                                  # 1000 个小作业到点释放
    adv_elapsed = time.perf_counter() - t0
    assert adv_elapsed < 2.0
    assert s.jobs[waiting.job_id].status == "running"
    assert s.jobs[waiting.job_id].actual_start == 5


def test_free_selection_skips_contiguous_occupied_blocks():
    """只找少量空位时跳过成片占用，选中节点号最小的空位。"""
    s = Scheduler(Settings(rack_capacities=(BIG_RACK,)))
    s.submit(BIG_RACK - 3, 1000)                  # 只剩末尾 3 个空位
    job, _ = s.submit(2, 5)
    assert job.assigned_nodes == [BIG_RACK - 3, BIG_RACK - 2]
    job2, _ = s.submit(1, 5)
    assert job2.assigned_nodes == [BIG_RACK - 1]
    # 空位耗尽：下一个是等待队首，预约指向最早释放时刻 t=5
    head, _ = s.submit(1, 2000)
    assert head.scheduled_start == 5
    assert head.assigned_nodes == [BIG_RACK - 3]
    # 其后的作业不是队首，不公布预约；跑满时限会超过 t=5 也不能回填
    tail, _ = s.submit(4, 10)
    assert tail.status == "queued" and tail.scheduled_start is None


def test_reservation_lists_all_free_plus_earliest_releasing():
    """空位不足时，预约节点 = 全部空位 + 预计释放最早的占用节点。"""
    s = Scheduler(Settings(rack_capacities=(BIG_RACK,)))
    s.submit(BIG_RACK - 3, 1000)                  # 占用 0..BIG-4，末尾 3 个空位
    head, _ = s.submit(4, 10)                     # 需 4 节点 > 3 空位
    assert head.scheduled_start == 1000
    assert head.assigned_nodes == [0, BIG_RACK - 3, BIG_RACK - 2,
                                   BIG_RACK - 1]


def test_release_heap_stale_entries_do_not_change_selection():
    """同一节点反复释放/转配后，释放堆中的陈旧项不影响选点结果。"""
    s = Scheduler(Settings(rack_capacities=(4,)))
    first, _ = s.submit(4, 10)                    # 节点 0-3，t=10 释放
    s.advance(3)
    s.report_completion(first.job_id, 3)          # 提前完工：堆中留下陈旧项
    second, _ = s.submit(4, 7)                    # 同样 t=10 释放（巧合同时刻）
    assert second.actual_start == 3
    head, _ = s.submit(4, 5)                      # 需等待：预约 t=10
    assert head.scheduled_start == 10
    assert head.assigned_nodes == [0, 1, 2, 3]
    s.advance(7)                                  # second 到点被杀，队首开工
    assert s.jobs[head.job_id].actual_start == 10
