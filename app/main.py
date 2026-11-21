"""FastAPI 路由：MPI 集群回填调度模拟。"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import settings
from .models import (
    AdvanceRequest,
    AdvanceResult,
    ClusterStatus,
    CompletionRequest,
    CompletionResult,
    EventView,
    JobSubmitRequest,
    JobView,
    RackOccupancy,
    ScheduledReservation,
)
from .scheduler import Event, Job, Scheduler, SchedulerError

app = FastAPI(
    title="MPI 集群回填调度模拟器",
    version="1.0.0",
    description=(
        "模拟时钟驱动的 MPI 计算集群回填（backfill）调度。\n\n"
        "- 作业需指定同一机架内的节点数与运行时限；\n"
        "- 队首需要等待时公布最早开工时刻，已公布时刻只可提前；\n"
        "- 队首等待期间，后续作业能用当前空位跑满时限且不耽误预约即可回填；\n"
        "- 完工回报或达到时限释放节点并立即重排；\n"
        "- 迟到的完工回报不会释放已转配的节点。"
    ),
)
scheduler = Scheduler(settings)
_lock = asyncio.Lock()


@app.exception_handler(SchedulerError)
async def scheduler_error_handler(_: Request, exc: SchedulerError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})


# ----------------------------------------------------------------- 序列化辅助


def _job_view(job: Job) -> JobView:
    return JobView(
        job_id=job.job_id,
        nodes=job.nodes,
        duration=job.duration,
        submit_time=job.submit_time,
        status=job.status,
        rack=job.rack,
        assigned_nodes=list(job.assigned_nodes),
        scheduled_start=job.scheduled_start,
        actual_start=job.actual_start,
        finish_time=job.finish_time,
        finish_reason=job.finish_reason,
    )


def _event_view(event: Event) -> EventView:
    return EventView(
        time=event.time,
        type=event.type,
        job_id=event.job_id,
        rack=event.rack,
        detail=event.detail,
    )


def _cluster_status() -> ClusterStatus:
    running = [_job_view(scheduler.jobs[i])
               for i in scheduler.jobs
               if scheduler.jobs[i].status == "running"]
    queued = [_job_view(scheduler.jobs[i]) for i in scheduler.queue]
    racks = [RackOccupancy(**r) for r in scheduler.rack_occupancy()]
    return ClusterStatus(
        now=scheduler.now,
        racks=racks,
        running_jobs=running,
        queued_jobs=queued,
        recent_events=[_event_view(e)
                       for e in scheduler.recent_events(limit=20)],
    )


# --------------------------------------------------------------------- 作业接口


@app.post("/jobs/submit", response_model=JobView, status_code=201,
          tags=["jobs"], summary="提交作业并立即触发回填重排")
async def submit_job(payload: JobSubmitRequest) -> JobView:
    async with _lock:
        job, _ = scheduler.submit(payload.nodes, payload.duration)
        return _job_view(job)


@app.post("/jobs/{job_id}/completion", response_model=CompletionResult,
          tags=["jobs"], summary="报告作业完工（at 必须等于当前模拟时刻）")
async def report_completion(job_id: int, payload: CompletionRequest) -> CompletionResult:
    async with _lock:
        job, events = scheduler.report_completion(job_id, payload.at)
        return CompletionResult(
            accepted=True,
            job=_job_view(job),
            events=[_event_view(e) for e in events],
        )


@app.get("/jobs", response_model=list[JobView], tags=["jobs"],
         summary="查询全部作业（含预约、实际开工时刻等）")
async def list_jobs() -> list[JobView]:
    async with _lock:
        return [_job_view(j) for j in scheduler.jobs.values()]


@app.get("/jobs/{job_id}", response_model=JobView, tags=["jobs"],
         summary="查询单个作业")
async def get_job(job_id: int) -> JobView:
    async with _lock:
        return _job_view(scheduler._job(job_id))


@app.get("/reservations", response_model=list[ScheduledReservation],
         tags=["jobs"], summary="查询当前公布的预约（最早开工时刻）")
async def get_reservations() -> list[ScheduledReservation]:
    async with _lock:
        return [ScheduledReservation(**r) for r in scheduler.reservations()]


# ----------------------------------------------------------------- 模拟时钟接口


@app.post("/simulation/advance", response_model=AdvanceResult,
          tags=["simulation"],
          summary="推进模拟时钟；一次跨过多个释放时刻时会在各时刻分别重排")
async def advance(payload: AdvanceRequest) -> AdvanceResult:
    async with _lock:
        start, target, events = scheduler.advance(payload.steps)
        return AdvanceResult(
            from_time=start,
            to_time=target,
            events=[_event_view(e) for e in events],
            status=_cluster_status(),
        )


@app.get("/simulation/status", response_model=ClusterStatus,
         tags=["simulation"], summary="查询时钟、节点占用与队列全貌")
async def get_status() -> ClusterStatus:
    async with _lock:
        return _cluster_status()


@app.post("/simulation/reset", tags=["simulation"],
          summary="重置模拟环境（清空作业、节点占用与时钟）")
async def reset_simulation() -> dict[str, Any]:
    async with _lock:
        scheduler.reset()
        return {"detail": "已重置", "now": scheduler.now}


@app.get("/events", response_model=list[EventView], tags=["simulation"],
         summary="查询最近的调度事件")
async def list_events(limit: int = 20) -> list[EventView]:
    async with _lock:
        limit = max(1, min(limit, Scheduler.MAX_EVENTS))
        return [_event_view(e) for e in scheduler.recent_events(limit)]


@app.get("/racks/occupancy", response_model=list[RackOccupancy],
         tags=["simulation"], summary="查询各机架节点占用")
async def get_occupancy() -> list[RackOccupancy]:
    async with _lock:
        return [RackOccupancy(**r) for r in scheduler.rack_occupancy()]


@app.get("/", tags=["meta"], summary="服务信息")
async def root() -> dict[str, str]:
    return {
        "service": "mpi-backfill-scheduler",
        "docs": "/docs",
        "racks": ",".join(
            f"{name}:{cap}" for name, cap in
            zip(scheduler.rack_names, settings.rack_capacities, strict=True)
        ),
    }
