# HPC 作业调度 API（FastAPI，本地可运行）

一个**零外部依赖**的 HPC 作业调度服务：提交作业、查询状态、取消、失败重试，
调度器按优先级把作业分配到集群节点。持久化只用本地 JSON 文件（原子写），
不需要数据库、消息队列或任何真实凭据。

## 快速开始

```bash
uv sync                      # 安装依赖（仅 fastapi + pytest）
uv run uvicorn app.main:app --reload   # 启动服务，默认 http://127.0.0.1:8000
# 或：uv run python main.py
```

打开交互式文档：<http://127.0.0.1:8000/docs>

提交一个作业：

```bash
curl -s -X POST http://127.0.0.1:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","cpus":2,"memory_mb":2048,"priority":1,"run_time_ms":500}'
```

运行测试（日志会打印关键输入、调度决策和拒绝原因）：

```bash
uv run pytest                                   # 安静模式
uv run pytest --log-cli-level=INFO              # 带完整调度日志
uv run pytest tests/test_priority_scheduling.py # 只跑某一组
```

## 配置（环境变量，均有默认值）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HPC_STATE_PATH` | `scheduler_state.json` | 持久化状态文件路径 |
| `HPC_NODES` | `node-1:4:8192:0,node-2:8:16384:1` | 节点清单 `名称:CPU:内存MB:GPU`，逗号分隔；仅首次启动（无状态文件）时生效 |

## API

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/v1/jobs` | 提交作业；可选 `idempotency_key`。返回 `{job, replayed}` |
| `GET  /api/v1/jobs` | 列出全部作业 |
| `GET  /api/v1/jobs/{id}` | 查询单个作业 |
| `POST /api/v1/jobs/{id}/cancel` | 取消待调度/运行中的作业 |
| `POST /api/v1/jobs/{id}/retry` | 重试失败作业（受 `max_attempts` 限制） |
| `GET  /api/v1/nodes` | 节点容量、已用量、可用量、运行中的作业 |
| `POST /api/v1/nodes/{id}/state` | `{"drained": true/false}` 排水/恢复节点 |
| `GET  /health` | 健康检查 |

作业提交字段：`name`、`cpus>=1`、`memory_mb>=1`、`gpus>=0`、`priority>=0`、
`run_time_ms`（模拟执行时长）、`fail`（模拟失败）、`timeout_ms`
（小于运行时长即判定超时）、`max_attempts`（1–10）、`idempotency_key`。

## 状态流转

```
pending  ──调度成功──▶ running ──执行完成──▶ succeeded   （终态）
   │                    │
   │                    ├──失败/超时──▶ failed ──retry──▶ pending
   │                    │
   └──────取消──────────┴────取消──────▶ cancelled       （终态）
```

* 合法流转：`pending→running/cancelled`、`running→succeeded/failed/cancelled`、
  `failed→pending`（仅显式 retry）；`succeeded/cancelled` 为终态。
* 任何非法流转返回 `409 invalid_state_transition`。
* 重复取消与"取消已结束作业"用不同错误码区分（见下表）。

## 调度规则

1. **选作业**：所有 `pending` 作业按 `priority 降序` 排序，同优先级按
   到达时间 `created_at 升序`，再以 job id 兜底保证结果确定。
2. **选节点**：在 `healthy` 节点中按节点 id 稳定顺序做 first-fit 装箱；
   `drained` 节点不接受新分配，但已有作业不受影响。
3. 一次 tick 在**同一把状态锁**内完成，并基于 tick 开始时的可用资源快照分配，
   因此并发提交/取消不会超分配、重复占用。
4. 调度器对"暂时放不下"不抛异常，而是逐作业给出可区分的拒绝原因：
   `node_unavailable`（无节点/全部排水）、`insufficient_resources`
   （总量够但当前没空位）、`invalid_resources`（请求超过任一节点的物理容量）。
5. 作业成功/失败/取消后释放资源并立即触发新一轮调度，排队作业自动补位。

## 并发与幂等语义

* 所有作业/节点变更都在 `SchedulerState` 的可重入锁内完成，并且**在锁内立即
  落盘**后才释放锁，因此"状态变更 + 资源变化 + 持久化"是一个原子步骤，崩溃
  不会留下"已取消但仍占用"之类的窗口。
* tick 由异步事件循环 + wake 事件触发，重叠的 tick 通过 `asyncio.Lock` 折叠，
  并发请求不会导致同一作业被调度两次。
* **幂等键**：相同 `idempotency_key` 的重复提交返回同一条作业（`replayed=true`），
  只占一份资源；相同 key 但请求体不同 → `409 idempotency_conflict`。
  幂等记录随状态持久化，重启后依然有效。
* **乱序更新防护**：执行协程"醒来"后在锁内重新确认作业仍是 `running` 才写结果；
  若期间已被取消，迟到的完成结果被丢弃，资源由取消路径负责，不会重复释放。
* **异常/超时收敛**：执行期任何底层异常都在执行器边界被捕获并归一化为
  `failed`（或超时失败），只记录日志，绝不把底层异常/堆栈透传给调用方；
  HTTP 层兜底所有未处理异常为 `500 internal_error`。

## 重启恢复

* 已接受作业、幂等键、节点与占用状态持久化在本地 JSON 文件（临时文件 +
  `os.replace` 原子替换，崩溃不会写坏文件）。
* 重启时，中断前处于 `running` 的作业被视为"执行现场丢失"：占用全部清零，
  作业重新置为 `pending` 等待重新调度；终态作业保持不变。

## 错误分类（稳定错误码）

所有错误响应统一为：

```json
{"error": {"code": "job_not_found", "message": "job 'x' not found"}}
```

| code | HTTP | 触发场景 |
| --- | --- | --- |
| `validation_error` | 422 | 请求体字段类型/范围不合法 |
| `invalid_resources` | 422 | 资源请求超过所有节点容量 |
| `job_not_found` / `node_not_found` | 404 | id 不存在 |
| `invalid_state_transition` | 409 | 状态机不允许的流转 |
| `job_already_cancelled` | 409 | 重复取消（作业已是 cancelled） |
| `job_not_cancellable` | 409 | 取消 succeeded/failed 的作业 |
| `job_not_retryable` | 409 | 对非 failed 作业 retry，或重试次数用尽 |
| `idempotency_conflict` | 409 | 幂等键被不同请求体复用 |
| `node_unavailable` | 503/调度拒绝 | 无可用节点（提交时 503；tick 中为拒绝原因） |
| `insufficient_resources` | 调度拒绝 | 资源暂时不足，作业保持 pending |
| `job_state_changed` | —— | 保留：状态已被并发改动（内部判定用） |
| `internal_error` | 500 | 兜底，不泄漏内部细节 |

## 分层结构（便于维护）

```
app/
  models.py         领域模型与 API schema（纯数据）
  errors.py         稳定错误码、异常、统一错误响应
  state_machine.py  合法状态流转表（唯一事实来源）
  state.py          加锁的内存状态、快照/恢复
  storage.py        原子写 JSON 持久化
  scheduler.py      优先级/FIFO 调度策略、装箱、释放
  executor.py       模拟执行：失败/超时/异常收敛、乱序防护
  idempotency.py    请求指纹
  service.py        编排：状态+持久化+调度+执行，并发控制
  routes.py         薄 HTTP 层
  main.py           应用工厂、生命周期、异常处理
tests/              27 个自动化测试
```

状态层（state/storage）、调度层（scheduler/executor）与 HTTP 层（routes）
通过 service 协作，互不直接依赖；不启动 HTTP 也能单独测试调度策略。
