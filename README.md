# FastAPI HPC 作业调度服务

一个可本地运行的 HPC 作业调度 API：提交作业、查询状态、取消、失败重试，
调度器按优先级把作业分配到模拟集群节点上。无外部服务依赖，
状态持久化在本地 SQLite，服务重启后已接受的作业与节点占用不丢。

## 架构分层

```
app/
  models.py     领域模型：Job/Node/Resources、状态机（唯一流转定义处）
  errors.py     稳定错误码分类（SchedulerError + ErrorCode）
  store.py      持久化层：SQLite 读写，不含业务逻辑
  scheduler.py  调度核心：纯内存资源账本 + 选节点策略，不做 I/O
  service.py    应用服务：幂等、并发锁、状态流转、模拟执行与恢复
  api.py        HTTP 层：参数解析、错误码 -> HTTP 状态映射
  bootstrap.py  组装：建库、播种默认节点、恢复
main.py         入口：create_app()
```

层间单向依赖：`api -> service -> (store, scheduler) -> models`，
状态、调度、HTTP 互不耦合，可独立测试。

## 调度规则

- **选作业**：每次资源释放或新作业到达后，扫描全部 `pending` 作业，
  按 **优先级降序、到达时间升序**（`created_at`，job_id 兜底）依次尝试调度。
- **选节点**：在健康且剩余资源足够的节点中做 **best-fit**
  （剩余 CPU 最小者优先，其次内存，再按节点 id 字典序），结果确定。
- **排队原因区分**：无健康节点 -> `queued_nodes_unavailable`；
  资源不足 -> `queued_no_resources`（见服务日志）。
- **节点健康**：`POST /nodes/{id}/health?healthy=false` 下线节点后不再分配；
  恢复上线会触发一次队列扫描。

## 状态机

```
pending  -> running | cancelled
running  -> succeeded | failed | cancelled
failed   -> pending            （重试）
succeeded, cancelled           （终态）
```

合法流转唯一定义在 `app/models.py: ALLOWED_TRANSITIONS`。
非法流转（如取消已成功作业、重试未失败作业）返回
`invalid_state_transition`；重复取消返回 `already_cancelled`，两者可稳定区分。

## 并发与幂等语义

- **一把 `RLock`** 保护所有状态变更与资源账本；调度、取消、执行回调
  都在锁内完成，因此并发提交不会超分配，并发取消不会重复释放
  （释放本身幂等：账本按 job_id 去重）。
- **幂等提交**：`idempotency_key` 唯一。同键同参数 -> 返回原作业（201，同一 job_id）；
  同键不同参数 -> 409 `idempotency_conflict`。重复提交不会生成两条作业或占两份资源。
- **执行一致性**：作业由后台线程模拟执行（`duration_seconds`，
  `should_fail` 可注入失败）。回调携带 `attempt` 令牌，
  乱序/过期回调（如取消后迟到的完成通知）被识别并丢弃；
  回调内任何异常只记日志，资源释放在锁内完成，底层异常不透传给调用方。
- **重启恢复**：作业与节点存于 SQLite（WAL）。启动时重建资源账本：
  `running` 作业补记占用并重新启动执行线程，`pending` 作业重新入队调度。

## 错误分类

| code | HTTP | 含义 |
|---|---|---|
| `job_not_found` | 404 | 作业/节点不存在 |
| `invalid_state_transition` | 409 | 当前状态不允许该操作 |
| `already_cancelled` | 409 | 重复取消 |
| `invalid_resource_request` | 422 | 请求超过所有节点总量，永远无法调度 |
| `idempotency_conflict` | 409 | 同幂等键、不同参数 |
| `internal` | 500 | 未预期错误（不透传底层异常） |

错误响应体固定为 `{"code": ..., "message": ...}`。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/jobs` | 提交作业（幂等键在请求体内） |
| GET | `/jobs` / `/jobs/{id}` | 列表 / 查询 |
| POST | `/jobs/{id}/cancel` | 取消 |
| POST | `/jobs/{id}/retry` | 失败重试 |
| GET | `/nodes` | 节点总量/可用量/健康 |
| POST | `/nodes/{id}/health?healthy=bool` | 上/下线节点 |

## 本地验证

```bash
# 跑测试（覆盖优先级调度、并发提交、取消释放、失败重试、
# 幂等重复提交、非法状态流转、重启恢复；日志含关键输入与调度决策）
uv run pytest -v -s

# 启动服务（默认 SQLite 文件 scheduler.db，可用 SCHEDULER_DB 覆盖）
uv run python main.py

# 提交一个 4 CPU 作业
curl -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "idempotency_key": "demo-1", "name": "train",
  "priority": 5, "cpu": 4, "memory_mb": 4096, "duration_seconds": 5}'

# 查看节点占用 / 作业状态
curl localhost:8000/nodes
curl localhost:8000/jobs

# 交互式文档
open http://localhost:8000/docs
```
