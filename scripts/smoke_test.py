#!/usr/bin/env python3
"""端到端冒烟演示：通过 HTTP 验证回填、跨时刻推进、预约与迟到回报。

用法：
    python scripts/smoke_test.py [base_url]
默认地址 http://127.0.0.1:8000（docker compose up 后的本地地址）。
仅使用标准库，不依赖第三方包。
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
PASS = 0
FAIL = 0


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def main() -> int:
    print(f"== 目标服务 {BASE} ==")
    status, body = call("GET", "/")
    check("服务可达", status == 200, str(body))
    call("POST", "/simulation/reset")

    print("\n-- 1. 提交作业制造等待与预约 --")
    _, j1 = call("POST", "/jobs/submit", {"nodes": 4, "duration": 10})
    _, j2 = call("POST", "/jobs/submit", {"nodes": 3, "duration": 10})
    _, j3 = call("POST", "/jobs/submit", {"nodes": 4, "duration": 10})
    check("j1 立即运行", j1["status"] == "running")
    check("j2 立即运行", j2["status"] == "running")
    check("队首 j3 排队并公布预约 t=10",
          j3["status"] == "queued" and j3["scheduled_start"] == 10,
          str(j3))
    _, reservations = call("GET", "/reservations")
    check("预约可查询且含机架/节点/释放时刻",
          reservations and reservations[0]["job_id"] == j3["job_id"]
          and reservations[0]["deadline"] == 20, str(reservations))

    print("\n-- 2. 回填：短作业用空位，长作业被拒于队列 --")
    _, j4 = call("POST", "/jobs/submit", {"nodes": 1, "duration": 3})
    _, j5 = call("POST", "/jobs/submit", {"nodes": 1, "duration": 99})
    check("短作业 j4 回填到 r2 空闲节点 3",
          j4["status"] == "running" and j4["rack"] == "r2"
          and j4["assigned_nodes"] == [3], str(j4))
    check("长作业 j5 保持排队且无预约",
          j5["status"] == "queued" and j5["scheduled_start"] is None, str(j5))
    check("j3 预约时刻仍为 10（不被回填耽误）",
          j3["scheduled_start"] == 10)

    print("\n-- 3. 节点占用查询 --")
    _, occ = call("GET", "/racks/occupancy")
    used = {r["rack"]: r["used"] for r in occ}
    check("两个机架均满占", used == {"r1": 4, "r2": 4}, str(used))

    print("\n-- 4. 完工回报时序校验与提前开工 --")
    status, _ = call("POST", f"/jobs/{j1['job_id']}/completion", {"at": 3})
    check("时钟为 0 时回报未来时刻被 409 拒绝", status == 409)
    call("POST", "/simulation/advance", {"steps": 3})
    status, comp = call("POST", f"/jobs/{j1['job_id']}/completion", {"at": 3})
    check("t=3 完工回报被接受", status == 200, str(comp))
    _, j3_after = call("GET", f"/jobs/{j3['job_id']}")
    check("队首 j3 提前于预约（t=10）在 t=3 开工",
          j3_after["status"] == "running"
          and j3_after["actual_start"] == 3, str(j3_after))
    status, _ = call("POST", f"/jobs/{j1['job_id']}/completion", {"at": 3})
    check("重复（迟到）完工回报被 409 拒绝", status == 409)

    print("\n-- 5. 一次推进跨过多个释放时刻，逐时刻重排 --")
    call("POST", "/simulation/reset")
    _, a = call("POST", "/jobs/submit", {"nodes": 4, "duration": 2})
    _, b = call("POST", "/jobs/submit", {"nodes": 4, "duration": 4})
    _, c = call("POST", "/jobs/submit", {"nodes": 4, "duration": 10})
    _, d = call("POST", "/jobs/submit", {"nodes": 1, "duration": 2})
    check("c 预约 t=2", c["scheduled_start"] == 2)
    check("d 无空位排队", d["status"] == "queued")
    _, adv = call("POST", "/simulation/advance", {"steps": 6})
    kinds = [e["type"] for e in adv["events"]]
    check("窗口内出现 3 次时限释放（t=2/t=4/t=6）",
          kinds.count("job_timeout") == 3, str(kinds))
    check("窗口内出现 2 次开工（c@2、d@4）",
          kinds.count("job_started") == 2, str(kinds))
    _, c_view = call("GET", f"/jobs/{c['job_id']}")
    _, d_view = call("GET", f"/jobs/{d['job_id']}")
    check("c 在 t=2 开工且仍在运行（时限 t=12）",
          c_view["actual_start"] == 2 and c_view["status"] == "running",
          str(c_view))
    check("d 在 t=4 回填开工、t=6 时限被杀",
          d_view["actual_start"] == 4 and d_view["status"] == "killed"
          and d_view["finish_time"] == 6, str(d_view))

    print("\n-- 6. 迟到回报不得释放已转配节点 --")
    call("POST", "/simulation/reset")
    _, old = call("POST", "/jobs/submit", {"nodes": 2, "duration": 3})
    call("POST", "/simulation/advance", {"steps": 3})
    _, new = call("POST", "/jobs/submit", {"nodes": 2, "duration": 5})
    check("旧作业超时被杀、节点已转配新作业",
          new["status"] == "running", str(new))
    status, _ = call("POST", f"/jobs/{old['job_id']}/completion", {"at": 3})
    check("迟到完工回报被 409 拒绝", status == 409)
    _, new_after = call("GET", f"/jobs/{new['job_id']}")
    check("新作业节点占用不受影响",
          new_after["status"] == "running"
          and len(new_after["assigned_nodes"]) == 2, str(new_after))

    print(f"\n== 结果：{PASS} 通过，{FAIL} 失败 ==")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
