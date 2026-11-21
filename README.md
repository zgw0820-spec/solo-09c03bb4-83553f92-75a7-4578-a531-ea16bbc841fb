# MPI 计算集群回填调度模拟器（FastAPI）

模拟时钟驱动的 MPI 集群**回填（backfill）调度**服务。机架与节点为内存状态，
通过 HTTP API 提交作业、推进时钟、回报完工并查询预约与占用。

## 调度规则

1. **同机架分配**：提交作业时声明同一机架所需节点数与运行时限，作业不跨机架；
   超过最大机架容量的作业直接拒绝（422）。
2. **队首优先 + 最早开工时刻**：每次重排先看队首，跨所有机架求最早可凑齐节点的
   时刻（队首**可以换机架**）。需要等待时公布预约（机架、节点、最早开工时刻、
   预计释放时刻）。**已公布的开工时刻只会提前、不会推后**。
3. **回填**：队首等待期间，按提交顺序检查后续作业——当前机架有空位、跑满自己
   的时限（`now + duration <= 队首预约时刻`）且不占用预约节点，就放行；
   否则继续排队。
4. **释放与重排**：收到完工回报，或时钟推进到达运行时限，立即释放节点并重排；
   一次推进跨过多个释放时刻时，在**每个释放时刻**都执行一次重排。
5. **迟到回报安全**：节点释放带作业归属校验。作业已完工/已被杀掉后，迟到的
   完工回报返回 409，不会释放已经转配给其他作业的节点。
6. **并发安全**：所有写操作由一把异步锁串行化，并发提交不会把同一节点分给两个
   作业。

## 选点实现与复杂度（百万节点）

机架扩到百万节点后，选点不再扫描整架，也不生成与容量等大的临时数据。
每个机架维护两个堆（`app/scheduler.py`）：

- **空闲节点最小堆**：精确存放当前空闲节点号。取最小的 k 个空位只弹 k 次，
  成片占用的节点不在堆中、天然被跳过；预约等纯查询"弹出后放回"，不消耗。
- **释放时刻最小堆**：`(预计释放时刻, 节点号, 作业 id)`，按释放时刻再按节点号
  排序。完工/被杀留下的陈旧项在查询时按 `owners` 归属校验惰性丢弃；积累到
  超过占用数 4 倍时重建，堆规模始终与当前占用数同阶。

队首选架先只比较各机架最早开工时刻（同刻按原机架顺序），仅对胜出机架枚举
具体节点；机架内仍是"先看预计释放时刻、再看节点号"。回填的准入条件与扫描
顺序不变，照旧避开队首预约节点。开工、完工、超时、重置后，两个堆随占用
同步更新，选点永远基于最新占用与释放时刻。

| 操作 | 优化前 | 优化后 |
| --- | --- | --- |
| 队首选架 + 选点（k 个空位） | O(机架数 × 容量 log 容量) | O(机架数 + k log n) |
| 满架队首预约（查 m 个最早释放） | 同上 | O(m log n) |
| 回填一个作业 | O(机架数 × 容量) | O((k + 跳过数) log n) |
| 开工 / 释放（k 个节点） | O(k) | O(k log n) |

实测（占用 99.9% 的单机架，提交 1 节点小作业）：容量 1 万 / 10 万 / 100 万时，
旧算法 3.7 / 43 / 504 ms 每次，优化后稳定在约 0.006 ms，满架预约约 0.03 ms。
唯一仍与容量成正比的情形是预约本身需要列出全部空位（空位数 < 需求数时），
属于 API 响应本身的输出规模。

## 目录结构

```
app/
  config.py      # 机架拓扑配置（环境变量 CLUSTER_RACKS）
  models.py      # Pydantic 请求/响应模型
  scheduler.py   # 调度核心：预约、回填、超时堆、归属校验、空闲/释放双堆选点
  main.py        # FastAPI 路由与异常映射
tests/           # pytest 单元 + API + 选点差分/性能测试（56 项）
scripts/
  smoke_test.py      # 仅用标准库的端到端 HTTP 冒烟脚本（22 项检查）
  bench_selection.py # 选点开销基准（新旧算法对照，最大 100 万节点）
Dockerfile
docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 服务与 Swagger：
#   API 根路径    http://localhost:8000/
#   Swagger UI   http://localhost:8000/docs
#   OpenAPI      http://localhost:8000/openapi.json
```

机架拓扑通过环境变量 `CLUSTER_RACKS` 配置（compose 中默认 `4,4`，即两个机架、
每个 4 节点）。支持两种写法，可混用：

- `"4,4"`：机架 r1=4 节点、r2=4 节点；
- `"3x4,8"`：4 个各 3 节点的机架，再加一个 8 节点机架。

百万节点示例：`CLUSTER_RACKS=1000000`（单机架）或 `CLUSTER_RACKS=1000x1000`
（1000 个机架各 1000 节点）。此时提交、预约、回填的选点开销仍为亚毫秒级；
但 `/simulation/status`、`/racks/occupancy` 等逐节点全量导出的接口响应体会
随机架容量增大（见"已知限制"）。

## 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv            # 如系统缺 ensurepip，可用 uv：uv venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/uvicorn app.main:app --reload --port 8000
.venv/bin/pytest -q              # 56 passed
.venv/bin/python scripts/smoke_test.py   # 需先启动服务，22 项检查
.venv/bin/python scripts/bench_selection.py   # 选点开销基准（新旧对照）
```

## API 一览（详见 Swagger）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/jobs/submit` | 提交作业 `{nodes, duration}`，立即触发重排 |
| POST | `/jobs/{job_id}/completion` | 完工回报 `{at}`，`at` 不得晚于当前时钟、早于开工或超过时限 |
| GET | `/jobs` / `/jobs/{id}` | 查询作业（预约、实际开工、完工时刻与原因） |
| GET | `/reservations` | 当前公布的预约（最早开工时刻与预计释放时刻） |
| POST | `/simulation/advance` | 推进模拟时钟 `{steps}`，跨多个释放时刻逐时刻重排 |
| GET | `/simulation/status` | 时钟、各机架节点占用、运行/排队作业、近期事件 |
| GET | `/racks/occupancy` | 各机架逐节点占用（节点号 → 作业 id） |
| GET | `/events` | 调度事件流（提交/预约/开工/完工/超时） |
| POST | `/simulation/reset` | 重置模拟环境 |

## 典型操作序列（Swagger 或 curl）

```bash
# 1) 占满两个机架（r1 4 节点、r2 先占 3 节点留 1 个空位）
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":4,"duration":10}'
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":3,"duration":10}'
# 2) 队首需要等待 -> 公布预约 t=10
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":4,"duration":10}'
curl -s localhost:8000/reservations
# 3) 短作业回填空位（跑满 3 个时间单位，不耽误 t=10 的预约）
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":1,"duration":3}'
# 4) 推进时钟；到达时限未回报完工的作业自动被杀并释放
curl -s -X POST localhost:8000/simulation/advance -H 'Content-Type: application/json' \
  -d '{"steps":6}'
# 5) 完工回报（at 必须 <= 当前时钟且未超时限）
curl -s -X POST localhost:8000/jobs/1/completion -H 'Content-Type: application/json' \
  -d '{"at":3}'
```

## 自测结果

- `pytest`：**56 passed**。其中 21 项为原有回归（回填准入/边界、预约节点保护、
  预约只提前与换机架、跨时刻逐点重排、时限杀作业、迟到/重复回报、错误码、
  并发不共享节点）；新增 35 项选点专项：
  - **差分模糊测试**（30 项）：把优化前的整架扫描实现完整保留为基准调度器，
    与优化后在 5 种机架拓扑 × 6 个随机种子下跑同一随机操作序列（提交/推进/
    完工/重置，覆盖跨时刻释放与空位变化），逐步比对作业字段、队列、预约、
    节点归属与事件流，完全一致；
  - **大机架性能测试**：百万节点机架 99.9% 成片占用时，1000 次小作业提交
    总耗时 < 2s（优化前单次约 600ms），满架预约 < 0.5s；并验证选点跳过成片
    占用、释放堆陈旧项不影响结果。
- `scripts/smoke_test.py`（真实 HTTP，uvicorn 进程）：**22 项检查全部通过**，
  含一次推进跨越 t=2/t=4/t=6 三个释放时刻的场景。
- `scripts/bench_selection.py`：容量 1 万/10 万/100 万时，旧算法选点
  3.7/43/504 ms 每次，优化后约 0.006 ms 持平，满架预约约 0.03 ms。
- 百万节点 API 实测（`CLUSTER_RACKS=1000000`）：小作业提交 HTTP 往返约
  1.3ms，满架预约与 `/reservations` 查询约 1.2ms。
- 16 个并发提交压测：8 个作业运行、8 个排队，8 个节点归属互不重复。

## 已知限制 / 遗留事项

- 状态全部在内存，`/simulation/reset` 或进程重启即清空，未做持久化。
- 模拟时钟为整数单位；完工回报允许补报历史时刻（`at <= now`），不允许未来时刻。
- 回填判定为保守策略：严格不触碰队首预约节点，未做更激进的预约漂移类优化。
- 单进程实例；横向多实例部署需引入共享存储与分布式锁。
- `/simulation/status`、`/racks/occupancy` 逐节点全量导出，响应体仍随机架
  容量线性增长（沿用既有 API 响应格式，未做分页/摘要）；`advance` 的响应内嵌
  全量状态，大机架下建议用 `/jobs`、`/reservations` 等轻量接口观察。
- 空位数 < 需求数时，预约节点列表包含全部空位，开销与输出规模成正比
  （属 API 响应本身的大小）；空闲堆初始化/重置为 O(容量) 一次性开销。
- 占用约百万节点的大作业，其提交/释放开销与响应体（逐节点列表）本身成正比，
  属既有 API 契约，未做特殊优化。
