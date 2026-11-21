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

## 目录结构

```
app/
  config.py      # 机架拓扑配置（环境变量 CLUSTER_RACKS）
  models.py      # Pydantic 请求/响应模型
  scheduler.py   # 调度核心：预约、回填、超时堆、归属校验
  main.py        # FastAPI 路由与异常映射
tests/           # pytest 单元 + API 测试（21 项）
scripts/
  smoke_test.py  # 仅用标准库的端到端 HTTP 冒烟脚本（22 项检查）
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

## 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv            # 如系统缺 ensurepip，可用 uv：uv venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/uvicorn app.main:app --reload --port 8000
.venv/bin/pytest -q              # 21 passed
.venv/bin/python scripts/smoke_test.py   # 需先启动服务，22 项检查
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

- `pytest`：**21 passed**，覆盖回填准入/边界、预约节点保护、预约只提前与换机架、
  跨时刻逐点重排、时限杀作业、迟到/重复回报、错误码、并发不共享节点。
- `scripts/smoke_test.py`（真实 HTTP，uvicorn 进程）：**22 项检查全部通过**，
  含一次推进跨越 t=2/t=4/t=6 三个释放时刻的场景。
- 16 个并发提交压测：8 个作业运行、8 个排队，8 个节点归属互不重复。

## 已知限制 / 遗留事项

- 状态全部在内存，`/simulation/reset` 或进程重启即清空，未做持久化。
- 模拟时钟为整数单位；完工回报允许补报历史时刻（`at <= now`），不允许未来时刻。
- 回填判定为保守策略：严格不触碰队首预约节点，未做更激进的预约漂移类优化。
- 单进程实例；横向多实例部署需引入共享存储与分布式锁。
