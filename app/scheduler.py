"""回填调度核心。

模拟时钟为整数，只通过 ``advance`` 向前推进。所有状态修改集中在本模块，
FastAPI 层用一把异步锁保证并发提交不会把同一节点分给两个作业。

关键规则：

1. 作业只使用同一机架内的节点，不跨机架；队首在重新预约时可以换机架。
2. 队首需要等待时公布最早开工时刻（预约）；已公布的时刻只会提前、不会推后。
3. 队首等待期间，按提交顺序检查后续作业：能用当前空位、跑满自己的时限且
   不耽误队首预约（now + duration <= 预约时刻，且不占用预约节点）才放行。
4. 作业完工回报或达到时限时释放节点并立即重排。
5. 节点释放带归属校验：迟到的完工回报面对已结束/已转配的节点不会再次释放。

选点实现（面向百万节点机架）：

- 每机架一个**空闲节点最小堆**，精确存放当前空闲节点号。取最小的 k 个
  空位只需弹 k 次，天然跳过成片占用；预约等纯查询场景"弹出后放回"，不消耗。
- 每机架一个**释放时刻最小堆**，元素为 ``(预计释放时刻, 节点号, 作业 id)``。
  完工/被杀产生的陈旧项在查询时按归属校验惰性丢弃；陈旧项积累过多时重建，
  堆规模保持与当前占用数同阶。
- 队首选架先只比较各机架最早开工时刻（同刻按原机架顺序），仅对胜出机架
  枚举具体节点。因此提交小作业的选点开销为 O(机架数 + k log n)，
  不再随机架容量/空闲容量线性增长，也不生成与整架等大的临时数据。
"""

from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass, field
from typing import Literal

from .config import Settings

JobStatus = Literal["queued", "running", "finished", "killed"]


class SchedulerError(Exception):
    """调度业务错误，HTTP 层映射为 4xx。"""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


@dataclass
class Job:
    job_id: int
    nodes: int
    duration: int
    submit_time: int
    status: JobStatus = "queued"
    rack: str | None = None
    assigned_nodes: list[int] = field(default_factory=list)
    scheduled_start: int | None = None
    actual_start: int | None = None
    finish_time: int | None = None
    finish_reason: str | None = None


@dataclass
class Event:
    time: int
    type: str
    detail: str
    job_id: int | None = None
    rack: str | None = None


class Scheduler:
    """单机模拟调度器（非线程安全，外层加锁）。"""

    MAX_EVENTS = 200

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.rack_names: list[str] = list(settings.rack_names)
        self.capacities: dict[str, int] = dict(
            zip(settings.rack_names, settings.rack_capacities, strict=True)
        )
        self.now: int = 0
        self.jobs: dict[int, Job] = {}
        self.queue: list[int] = []  # 提交顺序的待调度作业 id
        # rack -> 节点号 -> 当前占用作业 id
        self.owners: dict[str, dict[int, int]] = {
            rack: {} for rack in self.rack_names
        }
        # rack -> 空闲节点号最小堆（精确等于当前空闲集合，无陈旧项；
        # range 生成的升序列表天然满足堆序，无需 heapify）
        self._free_heaps: dict[str, list[int]] = {
            rack: list(range(self.capacities[rack]))
            for rack in self.rack_names
        }
        # rack -> (预计释放时刻, 节点号, 作业 id) 最小堆；
        # 完工/被杀留下陈旧项，查询时按 owners 归属校验惰性丢弃
        self._release_heaps: dict[str, list[tuple[int, int, int]]] = {
            rack: [] for rack in self.rack_names
        }
        # (释放时刻, 序号, 作业 id)
        self._timeout_heap: list[tuple[int, int, int]] = []
        self._seq = itertools.count()
        self._next_id = 1
        self._events: list[Event] = []

    # ------------------------------------------------------------------ 工具

    def _log(self, type_: str, detail: str, job_id: int | None = None,
             rack: str | None = None) -> Event:
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

    # --------------------------------------------------------------- 选点原语

    def _free_count(self, rack: str) -> int:
        """机架当前空闲节点数（O(1)，不扫描整架）。"""
        return len(self._free_heaps[rack])

    def _consume_free(self, rack: str, count: int,
                      exclude: set[int] | None = None) -> list[int]:
        """弹出节点号最小的 count 个空闲节点（升序返回）。

        ``exclude`` 中的节点（如队首预约节点）被跳过并放回堆中。
        调用前需保证可用空闲数足够，代价 O((count + 跳过数) log n)。
        """
        heap = self._free_heaps[rack]
        skip = exclude or ()
        held: list[int] = []   # 跳过、待放回
        taken: list[int] = []
        while len(taken) < count:
            if not heap:
                # 理论不可达：调用方已按空闲计数校验
                for n in itertools.chain(held, taken):
                    heapq.heappush(heap, n)
                raise SchedulerError(409, f"机架 {rack} 空闲节点不足")
            n = heapq.heappop(heap)
            if n in skip:
                held.append(n)
            else:
                taken.append(n)
        for n in held:
            heapq.heappush(heap, n)
        return sorted(taken)

    def _peek_earliest_releases(self, rack: str,
                                count: int) -> list[tuple[int, int]]:
        """占用节点中预计释放时刻最早的 count 个 ``(释放时刻, 节点号)``。

        纯查询：有效项弹出后放回；陈旧项（作业已完工/被杀）顺手丢弃。
        代价 O((count + 碰到的陈旧数) log n)。
        """
        heap = self._release_heaps[rack]
        owners = self.owners[rack]
        held: list[tuple[int, int, int]] = []
        while len(held) < count:
            # 理论不可达：调用方保证 count <= 当前占用数，而每个占用节点
            # 在堆中恰有一条有效项
            assert heap, "释放堆与占用状态不一致"
            entry = heapq.heappop(heap)
            if owners.get(entry[1]) == entry[2]:
                held.append(entry)
            # 否则为陈旧项：节点已释放或转配，丢弃
        for entry in held:
            heapq.heappush(heap, entry)
        return [(t, n) for t, n, _ in held]

    def _earliest_time_on_rack(self, rack: str, need: int) -> int:
        """某机架上凑齐 need 个节点的最早时刻（不枚举具体节点）。"""
        free = self._free_count(rack)
        if free >= need:
            return self.now
        releases = self._peek_earliest_releases(rack, need - free)
        return max(self.now, max(t for t, _ in releases))

    def _select_nodes(self, rack: str, need: int) -> list[int]:
        """按（预计释放时刻, 节点号）顺序选出 need 个节点（纯查询，不占用）。

        空闲节点的释放时刻视为当前时刻，因此空位足够时取节点号最小的
        need 个空位；不足时为全部空位 + 预计释放最早的一批占用节点。
        """
        free_heap = self._free_heaps[rack]
        free = len(free_heap)
        if free >= need:
            taken = [heapq.heappop(free_heap) for _ in range(need)]
            for n in taken:
                heapq.heappush(free_heap, n)
            return sorted(taken)
        releases = self._peek_earliest_releases(rack, need - free)
        return sorted(free_heap + [n for _, n in releases])

    # --------------------------------------------------------------- 作业启动

    def _start(self, job: Job, rack: str, nodes: list[int],
               kind: Literal["direct", "backfill", "reservation",
                             "early"]) -> Event:
        assert job.status == "queued"
        owner_map = self.owners[rack]
        for n in nodes:
            if n in owner_map:
                # 理论不可达：上层已校验，作为并发/逻辑的最后防线
                raise SchedulerError(409, f"节点 {rack}[{n}] 已被占用")
        release_heap = self._release_heaps[rack]
        release_at = self.now + job.duration
        for n in nodes:
            owner_map[n] = job.job_id
            heapq.heappush(release_heap, (release_at, n, job.job_id))
        # 惰性清理的兜底：陈旧项积累过多时重建，堆规模保持与占用数同阶
        if len(release_heap) > 4 * len(owner_map) + 64:
            release_heap[:] = [
                e for e in release_heap if owner_map.get(e[1]) == e[2]
            ]
            heapq.heapify(release_heap)
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

    # --------------------------------------------------------------- 主重排逻辑

    def reschedule(self) -> list[Event]:
        """核心回填重排，返回本次新产生的事件。"""
        events: list[Event] = []
        # 队首能立即开工就持续开工，直到出现需要等待的队首
        while self.queue:
            head = self.jobs[self.queue[0]]
            earliest, rack = self._earliest_start(head)
            if earliest > self.now:
                # 队首需要等待：公布/刷新预约（时刻只可提前）
                nodes = self._select_nodes(rack, head.nodes)
                self._publish_reservation(head, earliest, rack, nodes, events)
                # 回填：按提交顺序检查后续作业
                events.extend(self._backfill_pass(head))
                break
            kind = "direct" if head.scheduled_start is None else (
                "early" if head.scheduled_start > self.now else "reservation"
            )
            nodes = self._consume_free(rack, head.nodes)
            events.append(self._start(head, rack, nodes, kind))
        return events

    def _earliest_start(self, job: Job) -> tuple[int, str]:
        """跨所有机架求最早开工时刻与机架（队首可换机架）。

        只比较各机架的最早时刻，不为落选机架枚举节点；同刻按原机架顺序。
        """
        best_time: int | None = None
        best_rack: str | None = None
        for rack in self.rack_names:
            if self.capacities[rack] < job.nodes:
                continue
            t = self._earliest_time_on_rack(rack, job.nodes)
            if best_time is None or t < best_time:
                best_time, best_rack = t, rack
        if best_rack is None or best_time is None:
            biggest = max(self.capacities.values())
            raise SchedulerError(
                422,
                f"作业需要 {job.nodes} 个同机架节点，"
                f"最大机架仅 {biggest} 个节点，无法调度",
            )
        return best_time, best_rack

    def _publish_reservation(self, head: Job, earliest: int, rack: str,
                             nodes: list[int], events: list[Event]) -> None:
        old = head.scheduled_start
        # 防御性保证：已公布的开工时刻只可提前
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
        """队首等待时，按提交顺序为后续作业找当前空位回填。"""
        events: list[Event] = []
        assert head.scheduled_start is not None
        reserved_rack = head.rack
        reserved_nodes = set(head.assigned_nodes)
        # 预约节点中当前空闲的部分：回填一律避开。本趟回填只占用不释放，
        # 且不会触碰预约节点，因此该集合在一趟扫描内不变
        reserved_free: set[int] = set()
        if reserved_rack is not None:
            owners = self.owners[reserved_rack]
            reserved_free = {n for n in reserved_nodes if n not in owners}
        for job_id in list(self.queue[1:]):
            job = self.jobs[job_id]
            if job.status != "queued":
                continue
            # 跑满时限也不耽误预约
            if self.now + job.duration > head.scheduled_start:
                continue
            for rack in self.rack_names:
                if self.capacities[rack] < job.nodes:
                    continue
                exclude = reserved_free if rack == reserved_rack else None
                skipped = len(exclude) if exclude else 0
                if self._free_count(rack) - skipped < job.nodes:
                    continue
                picked = self._consume_free(rack, job.nodes, exclude=exclude)
                events.append(self._start(job, rack, picked, "backfill"))
                break
        return events

    # ------------------------------------------------------------------ 提交

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

    # ------------------------------------------------------------- 完工/超时释放

    def _release(self, job: Job, reason: Literal["completed", "killed"],
                 at: int) -> Event:
        assert job.status == "running"
        rack = job.rack
        assert rack is not None
        owner_map = self.owners[rack]
        free_heap = self._free_heaps[rack]
        # 归属校验：只释放仍属于本作业的节点，防止陈旧回报误释放转配节点
        for n in job.assigned_nodes:
            if owner_map.get(n) == job.job_id:
                del owner_map[n]
                heapq.heappush(free_heap, n)
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
        """处理完工回报。

        回报时刻 ``at`` 不得早于作业实际开工时刻；可以在时钟推进到某时刻后
        补报该时刻的完工（``at`` 不得晚于当前模拟时刻），也不得晚于运行时限
        （超时的作业已被杀掉，回报按迟到处理）。
        """
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
        """处理释放时刻 <= until 的所有超时。

        按释放时刻逐批弹出，并在每个时刻立即重排；重排中新启动的回填作业
        若时限也落在 ``until`` 之前，会在后续循环中被继续处理。堆中条目
        可能已陈旧（作业提前完工），通过作业状态与实际释放时刻过滤。
        """
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
                # 陈旧条目：已完工或实际释放时刻已变化，跳过
                if job.status != "running":
                    continue
                if job.actual_start + job.duration != t:
                    continue
                events.append(self._release(job, "killed", t))
            # 每个释放时刻都立即重排，回填作业可能在该时刻入场
            events.extend(self.reschedule())
        return events

    # ------------------------------------------------------------------ 时钟

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
        for rack in self.rack_names:
            self.owners[rack].clear()
            self._free_heaps[rack] = list(range(self.capacities[rack]))
            self._release_heaps[rack].clear()
        self._timeout_heap.clear()
        self._seq = itertools.count()
        self._next_id = 1
        self._events.clear()
        self._log("sim_reset", "模拟环境已重置")

    # ------------------------------------------------------------------ 查询视图

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

    def rack_occupancy(self) -> list[dict]:
        result = []
        for rack in self.rack_names:
            owner_map = self.owners[rack]
            nodes = {n: owner_map.get(n)
                     for n in range(self.capacities[rack])}
            used = len(owner_map)
            result.append({
                "rack": rack,
                "capacity": self.capacities[rack],
                "used": used,
                "free": self.capacities[rack] - used,
                "nodes": nodes,
            })
        return result

    def recent_events(self, limit: int = 20) -> list[Event]:
        return list(self._events[-limit:])
