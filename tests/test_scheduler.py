"""调度器单元测试：回填、预约、跨时刻推进、陈旧回报。"""

from __future__ import annotations

import pytest

from app.config import Settings
from app.scheduler import Scheduler, SchedulerError


def make_scheduler(racks: tuple[int, ...] = (4, 4)) -> Scheduler:
    return Scheduler(Settings(rack_capacities=racks))


def submit(s: Scheduler, nodes: int, duration: int) -> int:
    job, _ = s.submit(nodes, duration)
    return job.job_id


# --------------------------------------------------------------- 基础提交与占用


def test_submit_jobs_fill_racks_and_head_publishes_reservation():
    s = make_scheduler((4, 4))
    j1 = submit(s, 4, 10)
    j2 = submit(s, 4, 10)
    j3 = submit(s, 4, 5)

    j = s.jobs
    assert j[j1].status == "running" and j[j1].rack == "r1"
    assert j[j2].status == "running" and j[j2].rack == "r2"
    # 两个机架都满，队首等待并公布最早开工时刻
    assert j[j3].status == "queued"
    assert j[j3].scheduled_start == 10
    assert j[j3].rack in ("r1", "r2")
    assert len(j[j3].assigned_nodes) == 4
    occ = {r["rack"]: r for r in s.rack_occupancy()}
    assert occ["r1"]["used"] == occ["r2"]["used"] == 4
    assert {r["job_id"] for r in s.reservations()} == {j3}


def test_oversized_job_rejected():
    s = make_scheduler((3, 2))
    with pytest.raises(SchedulerError) as ei:
        s.submit(4, 1)
    assert ei.value.status_code == 422


# ----------------------------------------------------------------- 回填逻辑


def test_backfill_uses_free_space_without_delaying_reservation():
    s = make_scheduler((4, 4))
    submit(s, 4, 10)                  # 占满 r1
    submit(s, 3, 10)                  # r2 占用 0,1,2，节点 3 空闲
    waiting = submit(s, 4, 10)        # 队首：r1/r2 均在 t=10 才可凑齐
    assert s.jobs[waiting].scheduled_start == 10
    # 短作业：现在空位（r2 节点 3）上跑满时限也不晚于预约 → 回填
    short_ok = submit(s, 1, 3)
    # 跑满时限会超过预约时刻 → 不允许回填
    too_long = submit(s, 1, 12)

    j = s.jobs
    assert j[short_ok].status == "running"
    assert j[short_ok].rack == "r2"
    assert j[short_ok].assigned_nodes == [3]
    assert j[too_long].status == "queued"
    assert j[too_long].scheduled_start is None
    # 预约仍为 10，未被推迟
    assert j[waiting].scheduled_start == 10


def test_backfill_boundary_duration_equals_reservation_time_allowed():
    s = make_scheduler((4, 4))
    submit(s, 4, 10)                  # r1 满
    submit(s, 3, 10)                  # r2 节点 0,1,2
    waiting = submit(s, 4, 10)        # 预约 t=10
    # now=0，duration=10 => 完工时刻 10 <= 预约 10，边界放行
    exact = submit(s, 1, 10)
    assert s.jobs[exact].status == "running"
    assert s.jobs[exact].rack == "r2" and s.jobs[exact].assigned_nodes == [3]
    assert s.jobs[waiting].scheduled_start == 10


def test_backfill_cannot_take_reserved_nodes():
    s = make_scheduler((4,))
    submit(s, 3, 10)                  # r1 节点 0,1,2 被占
    head = submit(s, 4, 10)           # 需 4 节点，预约 t=10，含节点 3
    head_job = s.jobs[head]
    assert head_job.scheduled_start == 10
    assert set(head_job.assigned_nodes) == {0, 1, 2, 3}
    # 节点 3 当前空闲但已被预约，短作业不得占用
    thief = submit(s, 1, 5)
    assert s.jobs[thief].status == "queued"
    assert s.rack_occupancy()[0]["used"] == 3


# ---------------------------------------------------- 已公布时刻只可提前 + 换机架


def test_head_can_switch_rack_and_start_earlier_than_published():
    s = make_scheduler((3, 3))
    j1 = submit(s, 3, 10)             # r1 全占，t=10 释放
    submit(s, 2, 4)                   # r2 节点 0,1，t=4 释放
    head = submit(s, 3, 10)           # 队首：r2 t=4 可凑齐
    h = s.jobs[head]
    assert h.scheduled_start == 4 and h.rack == "r2"
    # r1 作业在 t=2 提前完工：r1 此刻即可凑齐 3 节点 → 立即开工、换到 r1
    s.advance(2)
    s.report_completion(j1, 2)
    assert h.status == "running"
    assert h.actual_start == 2
    assert h.rack == "r1"
    # 实际开工（2）早于此前公布的时刻（4），公布值本身从不推后
    assert h.actual_start <= h.scheduled_start == 4


def test_head_starts_immediately_when_space_frees():
    s = make_scheduler((4, 4))
    j1 = submit(s, 4, 10)
    submit(s, 4, 10)
    j3 = submit(s, 4, 10)
    assert s.jobs[j3].scheduled_start == 10
    # t=5 完工释放 r1，队首立即开工（早于预约 10）
    s.advance(5)
    s.report_completion(j1, 5)
    h = s.jobs[j3]
    assert h.status == "running"
    assert h.actual_start == 5 and h.rack == "r1"
    assert h.scheduled_start == 10      # 历史预约保留


# ------------------------------------------------------- 跨时刻推进与时限释放


def test_advance_crossing_multiple_release_points_reschedules_each():
    s = make_scheduler((4, 4))
    a = submit(s, 4, 2)                # r1，t=2 时限
    b = submit(s, 4, 4)                # r2，t=4 时限
    c = submit(s, 4, 10)               # 预约 r1 t=2
    d = submit(s, 1, 2)                # 暂无空位，排队
    assert s.jobs[c].scheduled_start == 2
    assert s.jobs[d].status == "queued"

    # 一次推进直接跨过 t=2、t=4、t=6 三个释放时刻
    _, target, events = s.advance(6)
    assert target == 6
    types = [e.type for e in events]
    # t=2 a 超时、c 开工；t=4 b 超时、d 开工；t=6 d 超时
    assert types.count("job_timeout") == 3
    assert types.count("job_started") == 2
    jc, jd = s.jobs[c], s.jobs[d]
    assert jc.status == "running" and jc.actual_start == 2   # 时限 t=12 未到
    assert jd.actual_start == 4
    assert jd.status == "killed" and jd.finish_time == 6
    assert s.rack_occupancy()[1]["used"] == 0


def test_timeout_kills_and_releases_then_reschedules():
    s = make_scheduler((4,))
    j = submit(s, 2, 3)
    s.advance(3)
    assert s.jobs[j].status == "killed"
    assert s.jobs[j].finish_reason == "killed"
    assert s.jobs[j].finish_time == 3
    assert s.rack_occupancy()[0]["used"] == 0


def test_backfill_started_during_advance_also_times_out_within_window():
    # 重排中新启动的作业，其时限也落在推进窗口内时应被继续处理
    s = make_scheduler((2, 2))
    submit(s, 2, 2)                    # r1，t=2 释放
    submit(s, 2, 8)                    # r2，t=8 释放
    submit(s, 2, 5)                    # 预约 r1 t=2，t=7 到期
    short = submit(s, 1, 2)            # 无空位，排队
    s.advance(6)
    # t=2 队首占 r1，r2 仍满 → short 继续等待
    assert s.jobs[short].status == "queued"
    s.advance(3)                       # 目标 t=9：t=7、t=8 各有释放
    assert s.jobs[short].actual_start == 7
    assert s.jobs[short].status == "killed"
    assert s.jobs[short].finish_time == 9


# ----------------------------------------------------------- 迟到完工回报处理


def test_late_completion_after_timeout_does_not_touch_reassigned_nodes():
    s = make_scheduler((4,))
    old = submit(s, 2, 3)
    s.advance(3)                        # 时限杀掉，节点释放
    assert s.jobs[old].status == "killed"
    new = submit(s, 2, 5)              # 节点转配给新作业
    assert s.jobs[new].status == "running"
    with pytest.raises(SchedulerError) as ei:
        s.report_completion(old, 3)    # 迟到回报被拒
    assert ei.value.status_code == 409
    # 新作业的节点占用原样保留
    assert s.jobs[new].status == "running"
    assert s.rack_occupancy()[0]["used"] == 2
    assert all(v == new for v in s.owners["r1"].values())


def test_duplicate_completion_is_stale():
    s = make_scheduler((4,))
    j = submit(s, 2, 5)
    s.advance(2)
    s.report_completion(j, 2)
    assert s.jobs[j].status == "finished"
    with pytest.raises(SchedulerError):
        s.report_completion(j, 2)


def test_completion_at_cannot_be_in_future_or_before_start():
    s = make_scheduler((4,))
    j = submit(s, 2, 5)
    with pytest.raises(SchedulerError):
        s.report_completion(j, 3)       # 当前时刻为 0，回报未来时刻
    s.advance(4)
    job, _ = s.report_completion(j, 4)  # 与时钟一致且未超时限，放行
    assert job.status == "finished"


def test_completion_beyond_deadline_rejected():
    s = make_scheduler((4,))
    j = submit(s, 2, 3)
    s.advance(3)                        # 已超时
    with pytest.raises(SchedulerError):
        s.report_completion(j, 3)


# ---------------------------------------------------------------- 杂项


def test_reset_clears_everything():
    s = make_scheduler((4,))
    submit(s, 4, 10)
    submit(s, 1, 2)
    s.advance(2)
    s.reset()
    assert s.now == 0
    assert s.jobs == {} and s.queue == []
    assert all(m == {} for m in s.owners.values())


def test_event_log_is_bounded():
    s = make_scheduler((4,))
    for _ in range(Scheduler.MAX_EVENTS + 50):
        s.reset()
        submit(s, 1, 1)
    assert len(s._events) <= Scheduler.MAX_EVENTS
