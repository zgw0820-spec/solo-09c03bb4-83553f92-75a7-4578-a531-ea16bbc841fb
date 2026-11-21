"""API 层测试：Swagger 对应端点、错误码与并发提交。"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client() -> TestClient:
    c = TestClient(app)
    c.post("/simulation/reset")
    return c


def test_root_and_openapi(client: TestClient):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["service"] == "mpi-backfill-scheduler"
    spec = client.get("/openapi.json").json()
    assert "/jobs/submit" in spec["paths"]
    assert "/simulation/advance" in spec["paths"]


def test_full_backfill_flow_via_api(client: TestClient):
    # 占满两个机架
    r = client.post("/jobs/submit", json={"nodes": 4, "duration": 10})
    j1 = r.json()["job_id"]
    client.post("/jobs/submit", json={"nodes": 3, "duration": 10})
    # 队首等待 → 预约
    j3 = client.post("/jobs/submit", json={"nodes": 4, "duration": 10}).json()
    assert j3["scheduled_start"] == 10
    reservations = client.get("/reservations").json()
    assert reservations[0]["job_id"] == j3["job_id"]
    assert reservations[0]["deadline"] == 20
    # 回填
    j4 = client.post("/jobs/submit", json={"nodes": 1, "duration": 3}).json()
    assert j4["status"] == "running" and j4["rack"] == "r2"
    j5 = client.post("/jobs/submit", json={"nodes": 1, "duration": 99}).json()
    assert j5["status"] == "queued"

    status = client.get("/simulation/status").json()
    assert status["now"] == 0
    assert {r["rack"]: r["used"] for r in status["racks"]} == {"r1": 4, "r2": 4}

    # 时钟还在 0，回报未来时刻 t=3 → 409
    bad = client.post(f"/jobs/{j1}/completion", json={"at": 3})
    assert bad.status_code == 409
    client.post("/simulation/advance", json={"steps": 3})
    ok = client.post(f"/jobs/{j1}/completion", json={"at": 3})
    assert ok.status_code == 200
    # 队首提前于预约开工
    j3_after = client.get(f"/jobs/{j3['job_id']}").json()
    assert j3_after["status"] == "running"
    assert j3_after["actual_start"] == 3

    # 迟到回报：j1 已完工，再报一次 → 409
    stale = client.post(f"/jobs/{j1}/completion", json={"at": 3})
    assert stale.status_code == 409


def test_advance_and_occupancy_endpoint(client: TestClient):
    client.post("/jobs/submit", json={"nodes": 4, "duration": 2})
    client.post("/jobs/submit", json={"nodes": 4, "duration": 4})
    client.post("/jobs/submit", json={"nodes": 4, "duration": 10})
    r = client.post("/simulation/advance", json={"steps": 6})
    assert r.status_code == 200
    body = r.json()
    assert body["from_time"] == 0 and body["to_time"] == 6
    event_types = [e["type"] for e in body["events"]]
    assert event_types.count("job_timeout") >= 2
    occ = client.get("/racks/occupancy").json()
    assert {x["rack"]: x["free"] for x in occ} == {"r1": 0, "r2": 4}


def test_error_codes(client: TestClient):
    assert client.post("/jobs/submit", json={"nodes": 0, "duration": 1}).status_code == 422
    assert client.post("/jobs/submit", json={"nodes": 99, "duration": 1}).status_code == 422
    assert client.get("/jobs/9999").status_code == 404
    assert client.post("/simulation/advance", json={"steps": 0}).status_code == 422


def test_concurrent_submissions_never_share_nodes(client: TestClient):
    async def scenario() -> None:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as ac:
            # 16 个 1 节点作业并发提交，集群只有 8 个节点
            coros = [
                ac.post("/jobs/submit", json={"nodes": 1, "duration": 10})
                for _ in range(16)
            ]
            responses = await asyncio.gather(*coros)
            assert all(r.status_code == 201 for r in responses)

    asyncio.run(scenario())

    body = client.get("/simulation/status").json()
    owners = [owner for rack in body["racks"]
              for owner in rack["nodes"].values() if owner is not None]
    assert len(owners) == 8                       # 恰好 8 个节点被占
    assert len(body["running_jobs"]) == 8
    assert len(body["queued_jobs"]) == 8
    assert len(owners) == len(set(owners))        # 节点归属唯一
