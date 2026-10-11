# AI Agent Execution Service

## 项目实现蓝图

**用途：** 为 GitHub 个人项目提供可实施、可验证的后端设计。

**范围：** 面向 0–2 年经验的 software / backend engineer。

**说明：** 以下目录、函数和数据结构是目标设计；实际实现进度以 `README.md` 为准。
测试指标必须来自本次实现，不沿用历史成绩。

**当前实现：** 调用方通过 `DecisionProvider` 提供决策；fake provider 仅模拟调用方。
已实现异步执行、lease/heartbeat、有限重试、action 持久化，以及真实数据库库存查询和幂等预留。
库存、tool_effects 和 reservations 在同一个 PostgreSQL 事务内提交，已验证扣库存后终止 Worker 的恢复。
已实现每轮决策尝试次数持久化与 Run 总期限（默认从创建起 300 秒，包含排队）。
已实现 Steps 查询接口，支持按轮次排序、游标分页、Run 错误和已知决策尝试次数查询。
已整理 pytest 集成测试、端到端演示与 GitHub Actions 配置；本地 14 个测试组通过，云端 CI 尚待首次运行。
独立 checkpoint 尚未实现；其他未实现部分仍是后续目标。

---

## 阅读导航

- [1. 项目做什么](#overview)
- [2. 项目目录与数据库分支](#structure)
- [3. Python 文件职责与内部接口](#modules)
- [4. HTTP 接口与请求实例](#http)
- [5. 数据库分层与表关系](#database)
- [6. 执行、事务与恢复实例](#execution)
- [7. 可靠性规则](#reliability)
- [8. 实现顺序与验收](#delivery)

阅读建议：先看 1、2、4 节理解全貌，再看 3、5、6 节理解代码与数据。

<a id="overview"></a>

## 1. 项目做什么

用户提交任务后，API 立即返回任务 ID。
独立 Worker 执行多轮模型决策和工具调用，把状态与结果保存到 PostgreSQL。

核心目标：消息重复或 Worker 崩溃之后，任务可以继续，已提交的本地业务动作不会重复发生。

### 示例业务

以库存预留为例：

```text
提交预留请求
  -> 查询库存
  -> 预留 2 件商品
  -> 返回预留结果
```

第一版使用 fake LLM 选择动作，工具操作真实的数据库业务表。
模型替身用于稳定测试，不代表已经验证真实模型能力。

### 第一版边界

- 实现：异步执行、状态查询、取消、重试、并发领取、checkpoint、恢复与本地幂等。
- 不实现：多 Agent、动态 DAG、人工审批、任意代码执行、完整监控平台。
- 不承诺：任意外部 API 的 exactly-once、生产级多租户认证或未经测试的性能。

假设：每轮最多调用一个工具或返回 FINAL；本地开发使用固定 caller scope。

### 服务关系

```text
Caller -> API -> PostgreSQL
            -> RabbitMQ -> Worker -> Engine
                                      -> Fake LLM
                                      -> Tools -> PostgreSQL
```

PostgreSQL 保存可信状态；RabbitMQ 只投递 `run_id`。
一条消息对应一个 run，step 不单独入队。

<a id="structure"></a>

## 2. 项目目录与数据库分支

```text
ai_agent_execution_service/
|-- app/
|   |-- __init__.py
|   |-- main.py
|   |-- api.py
|   |-- schemas.py
|   |-- config.py
|   |-- broker.py
|   |-- worker.py
|   |-- engine.py
|   |-- fake_llm.py
|   |-- tools.py
|   `-- db/
|       |-- __init__.py
|       |-- session.py
|       |-- models.py
|       |-- run_store.py
|       |-- execution_store.py
|       `-- inventory_store.py
|-- migrations/
|   |-- env.py
|   `-- versions/
|       `-- 0001_initial_schema.py
|-- tests/
|-- scripts/
|-- docker-compose.yml
|-- alembic.ini
|-- pyproject.toml
|-- .env.example
`-- README.md
```

### 数据库为什么有两个目录？

**`app/db/`：运行时访问数据库。**
包含连接管理、ORM 表定义、任务读写、执行记录和库存事务。

**`migrations/`：维护数据库结构的版本。**
负责建表、索引和结构变更，不负责业务执行。

上一版把数据库逻辑集中在 `store.py`，目录树不够直观。
本版将其拆成明确的 `db/` 分支，并区分 HTTP schema 与数据库 model。

注意：PostgreSQL 是 Docker Compose 启动的服务，不是放在项目里的 `.py` 文件或本地数据库文件。

<a id="modules"></a>

## 3. Python 文件职责与内部接口

下面是建议的函数契约，不是已存在的实现。
HTTP endpoint 对外提供服务；这些 Python 接口用于项目内部调用。

### 3.1 API 与配置

#### `main.py` — API 进程入口

创建 FastAPI 应用，注册路由，通过 lifespan 初始化和关闭 DB / MQ 连接。
不启动 Worker，不执行 Agent Loop。

```python
def create_app() -> FastAPI
async def lifespan(app: FastAPI)  # 启动与关闭资源
```

#### `api.py` — HTTP 路由

接收已校验请求，调用数据库和 Broker，把结果转换成 HTTP 响应。

```python
async def create_run(request, idempotency_key) -> RunCreated
async def get_run(run_id) -> RunDetail
async def list_steps(run_id, limit, cursor) -> StepPage
async def cancel_run(run_id) -> RunDetail
async def dispatch_run(run_id) -> DispatchResult
```

#### `schemas.py` — Pydantic 请求、响应与动作类型

定义数据格式和参数校验。它不建表、不连接数据库。

```python
class CreateRunRequest: ...  # task、input、max_iterations
class RunCreated: ...       # run_id、status、dispatch_status
class RunDetail: ...        # 进度、结果、错误、取消标记
class StepPage: ...         # items、next_cursor
class DispatchResult: ...   # run_id、dispatch_status
class ToolAction: ...       # tool_name、arguments
class FinalAction: ...      # result
class Claim: ...            # run_id、claim_id、lease_expires_at
```

#### `config.py` — 环境配置

集中读取连接地址、lease、timeout、并发和预算配置。
敏感值来自环境变量，不写进仓库。

```python
class Settings: ...
def get_settings() -> Settings
```

### 3.2 消息与执行

#### `broker.py` — RabbitMQ 通信

管理连接、队列、发布确认和 delivery 封装。
不判断数据库 ownership，不决定任务结果。

```python
async def connect() -> None
async def publish_run(run_id) -> DispatchResult
async def consume(handler) -> None
async def close() -> None
```

delivery 对象提供 `ack()`、`reject(requeue=False)` 等操作。
`handler` 由 Worker 提供；发布时验证消息是否路由成功及 Broker 是否确认。

#### `worker.py` — Worker 进程入口与任务领取

消费消息、获取 claim、启动 heartbeat、调用 Engine，再处理消息确认。
使用 semaphore 限制实际执行并发，prefetch 单独配置。

```python
async def main() -> None
async def handle_delivery(delivery) -> None
async def wait_and_claim(run_id) -> Claim | None
async def heartbeat_loop(claim, stop_event) -> None
```

终态任务可直接 ACK；非终态 claim 失败不能直接 ACK。
Worker 需要等待并重新检查，或保留可靠的后续恢复机会。

#### `engine.py` — 多轮执行与恢复

加载 checkpoint 和未完成 action，控制模型调用、工具执行、取消和预算。
消息 ACK 留给 Worker；数据库事务留给对应 store。

```python
async def resume_and_execute(claim) -> RunDetail
async def execute_iteration(claim, state) -> None
def parse_action(response) -> ToolAction | FinalAction
def retry_delay(attempt) -> float
```

已有持久化 action 时直接恢复它，不重新向模型请求该轮决策。

#### `fake_llm.py` — 模型测试替身

按上下文返回查询库存、预留库存或 FINAL。
支持可控 timeout、非法响应和重放不同动作，用于故障测试。

```python
async def decide(context, scenario) -> dict
```

#### `tools.py` — 受控工具入口

校验工具名称和参数，分发到库存查询或事务幂等预留。
只提供固定工具集合，不执行模型生成的任意代码。

```python
async def execute_tool(claim, action, operation_id) -> dict
```

库存事务在 `inventory_store.py` 内实现，不能先扣库存、再单独写 effect。

### 3.3 数据库运行模块

#### `db/session.py` — 数据库连接与 session

管理 SQLAlchemy async engine、连接池和 session 生命周期。

```python
async def init_db() -> None
async def close_db() -> None
async def session_scope()  # async context manager
```

session 只管理资源；每个 store 明确控制自己的短事务。
初始化连接不代替 migration，不在启动时自动修改生产表结构。

#### `db/models.py` — ORM 表结构

定义表、字段、外键、唯一约束和索引，不包含 HTTP 响应模型。

```python
class Run: ...
class Step: ...
class Checkpoint: ...
class ToolEffect: ...
class Inventory: ...
class Reservation: ...
```

#### `db/run_store.py` — 任务状态与执行资格

负责提交幂等、状态查询、取消、claim、续租和终态提交。

```python
async def create_or_get_run(request, scope, key) -> tuple[Run, bool]
async def get_run(run_id) -> Run
async def request_cancel(run_id) -> Run
async def try_claim(run_id, worker_id) -> Claim | None
async def renew_lease(claim) -> bool
async def finish_run(claim, status, result=None, error=None) -> Run
async def require_claim(session, claim) -> Run
```

`require_claim()` 在调用者已有事务内锁定 run 行并验证有效 claim。
其他写入模块复用它；它本身不独立提交，也不提前释放锁。

`finish_run()` 原子处理终态与取消竞争；终态不会被后续请求覆盖。

#### `db/execution_store.py` — Steps、action 与 checkpoint

负责持久化执行计划、尝试次数、恢复上下文和轮次完成状态。

```python
async def load_resume_state(run_id) -> ResumeState
async def begin_attempt(claim, iteration, step_type) -> int
async def save_action(claim, iteration, action) -> PersistedAction
async def complete_iteration(claim, iteration, output, context) -> None
async def list_steps(run_id, limit, cursor) -> StepPage
```

`save_action()` 保存固定 operation_id；`complete_iteration()` 在同一事务更新工具输出、checkpoint 和完成轮数。

#### `db/inventory_store.py` — 示例业务与副作用幂等

查询库存，或原子提交预留业务和 effect。

```python
async def get_inventory(sku) -> dict
async def reserve_once(claim, operation_id, sku, quantity) -> dict
```

`reserve_once()` 校验 claim 和取消意图，核对已有 effect，并锁定/条件更新库存。
库存不足时返回明确的业务失败；并发不同 run 也不能把库存扣成负数。

### 3.4 Migration 文件

**`migrations/env.py`：** 配置 Alembic 连接及 ORM metadata，供迁移工具调用。

**`migrations/versions/0001_initial_schema.py`：** 创建第一版表、约束和索引。

```python
def upgrade() -> None  # 应用该版本结构
def downgrade() -> None  # 回退该版本；可能删除数据
```

`__init__.py` 标识 Python 包，第一版保持为空。
测试文件按验收场景编写，演示脚本按需增加，不预先堆积模块。

### 3.5 调用关系实例

```text
api.create_run()
  -> run_store.create_or_get_run()
  -> broker.publish_run()

worker.handle_delivery()
  -> run_store.try_claim()
  -> engine.resume_and_execute()
     -> execution_store.load_resume_state()
     -> fake_llm.decide()
     -> execution_store.save_action()
     -> tools.execute_tool()
        -> inventory_store.reserve_once()
     -> execution_store.complete_iteration()
     -> run_store.finish_run()
  -> delivery.ack()
```

<a id="http"></a>

## 4. HTTP 接口与请求实例

### `POST /runs` — 提交任务

新建返回 `201`，相同幂等请求返回 `200`；不等待执行完成。
同一 key 对应不同规范化 payload，返回 `409`。

```http
POST /runs
Idempotency-Key: reserve-demo-001
Content-Type: application/json

{
  "task": "reserve_inventory",
  "input": {
    "sku": "BOOK-001",
    "quantity": 2
  },
  "max_iterations": 5
}
```

```json
{
  "run_id": "7c8d6624-8665-4405-b30a-52d37db132d0",
  "status": "PENDING",
  "dispatch_status": "CONFIRMED"
}
```

`CONFIRMED` 表示此次发布获 Broker 确认；`UNCONFIRMED` 表示失败或结果未知。
DB 已创建而发布未确认时，仍返回已创建的 run。
相同 key 重试会返回原 run，并对非终态任务再次尝试派发。

### `GET /runs/{id}` — 查询任务

返回 `200`，包含状态、完成轮数、取消标记、结果和错误。
不存在则 `404`。

```json
{
  "run_id": "7c8d6624-8665-4405-b30a-52d37db132d0",
  "status": "COMPLETED",
  "iteration_count": 3,
  "cancel_requested": false,
  "result": {"sku": "BOOK-001", "reserved": 2},
  "error": null
}
```

### `GET /runs/{id}/steps` — 查询执行轨迹

返回 `200`，按轮次展示已完成的调用方决策与工具操作。
`limit` 默认 50、最大 100；`cursor` 用于分页，响应包含 `items` 和 `next_cursor`。
响应同时包含 `run_id`、`status`、Run 级别 `error_payload` 和各轮 `decision_attempts`。
当前失败或执行中的调用不会生成 Step；通过 Run 错误与尝试次数排查，不提供逐次失败历史。
同一轮先返回 `LLM_CALL`（调用方 DecisionProvider），再返回 `TOOL_CALL`。
不存在的 Run 返回 `404`；非法 UUID、分页参数或其他 Run 的 cursor 返回 `422`。

示例：`GET /runs/{id}/steps?limit=20`。

### `POST /runs/{id}/cancel` — 请求取消

返回 `200`。非终态写入取消意图，由 Worker 在安全位置处理。
已经终态则返回现状，不修改结果。

尚未派发的任务需要先补派发，才能由 Worker 完成取消处理。

### `POST /runs/{id}/dispatch` — 手动补派发

非终态允许再次发送 run 消息，但不重置进度。
Broker 确认后返回 `202`；未确认返回 `503`，可安全重试。
终态返回 `409`，不存在返回 `404`。

这是手动恢复入口，不代表系统自动找回所有未派发任务。
请求参数非法统一返回 `422`。

<a id="database"></a>

## 5. 数据库分层与表关系

四层共用一个 PostgreSQL 数据库，不是四套数据库。

### 第一层：任务与调度

**表：`runs`**

- 身份与输入：id、caller_scope、request_key、request_hash、input_payload。
- 状态与预算：status、cancel_requested、iteration_count、max_iterations、deadline_at。
- 执行资格：worker_id、claim_id、lease_expires_at。
- 结果与定位：latest_checkpoint_id、result、error、时间戳。

唯一约束：`(caller_scope, request_key)`。没有 key 时可以创建多个独立 run。
worker_id 用于排障，claim_id 标识本次领取，二者不能混用。

### 第二层：执行与恢复

**表：`steps`**

保存轮次、step 类型、action、固定 operation_id、输出、状态、attempt_count 和耗时。
唯一约束：`(run_id, iteration_index, step_type)`。
第一版每轮最多一个 LLM_CALL 和一个 TOOL_CALL。

**表：`checkpoints`**

保存 completed_iteration 和 state_snapshot，包括下一轮所需的 context / observations。
唯一约束：`(run_id, completed_iteration)`。

### 第三层：副作用幂等

**表：`tool_effects`**

保存 operation_id、run_id、tool_name、arguments_hash 和 result。
operation_id 为主键，恢复时用它取回已经提交的业务结果。

相同 ID 的工具或参数不一致时，报告一致性错误，不返回错配的旧结果。

### 第四层：示例业务

**表：`inventory`** — sku、available；库存数量不得为负。

**表：`reservations`** — reservation_id、run_id、operation_id、sku、quantity。
operation_id 唯一并关联 effect。

预留 2 件时：库存从 10 变成 8，新增一条 reservation 和一条 effect。
重复执行同一 operation_id 时，库存仍为 8，返回同一个预留结果。

### 表关系

```text
runs
  |-- steps          (1:N)
  |-- checkpoints    (1:N)
  `-- tool_effects   (1:N)
        `-- reservations (本例中每个预留 effect 对应一条记录)

inventory <- reservations.sku
```

通过 ORM 定义关系、通过 migration 创建真实约束。
仅检查 effect 没有重复行不够，还必须检查库存变化和预留记录。

<a id="execution"></a>

## 6. 执行、事务与恢复实例

### 正常执行

```text
第 1 轮：LLM 选择 get_inventory
         -> 查询库存 -> 保存输出与 checkpoint 1

第 2 轮：LLM 选择 reserve_inventory
         -> 预留库存 -> 保存输出与 checkpoint 2

第 3 轮：LLM 返回 FINAL
         -> 提交结果 -> COMPLETED -> ACK
```

iteration 是一次模型决策，step 是追踪记录。
iteration_count 计算已完成决策轮数，包含 FINAL 轮。
下一轮开始前检查 max_iterations，不以数据库记录条数计算预算。

### 三个短事务

**事务 A：保存决策。**
校验 claim，保存本轮 action 和参数；工具动作同时生成并保存 operation_id。
对应 `execution_store.save_action()`。

**事务 B：提交本地副作用。**
校验 claim 与取消意图，查询已有 effect；若尚未提交，则原子完成库存更新、reservation 和 effect。
对应 `inventory_store.reserve_once()`。

**事务 C：完成轮次。**
校验 claim，原子保存工具输出、context、checkpoint 和完成轮数。
对应 `execution_store.complete_iteration()`。

模型调用和长等待在事务之外，不长期持有行锁。
require_claim 的校验与关键写入必须处于同一个事务内。

### 崩溃实例 A：checkpoint 后退出

```text
A 提交 checkpoint 1
  -> A 被终止，消息未 ACK
  -> B 收到重投，等待旧 lease 过期
  -> B 成功 claim，从第 2 轮继续
```

### 崩溃实例 B：预留已提交，checkpoint 未提交

```text
A 保存第 2 轮 action / operation_id
  -> 事务 B 提交：库存 10 -> 8
  -> A 被终止，事务 C 未提交
  -> B 接管，读取已保存 action
  -> 复用已有 effect，不再扣库存
  -> 补齐 checkpoint 2，继续第 3 轮
```

模型响应尚未持久化就退出，可以重新调用模型，因为工具尚未开始。
action 已持久化则恢复该 action，即使模型重放可能产生不同动作。

<a id="reliability"></a>

## 7. 可靠性规则

### Ownership 与消息处理

- 原子 claim 仅允许领取非终态、无有效 lease 的 run。
- 每次 claim 生成新 claim_id；续租与关键写入均验证 claim_id 和 DB 时间。
- 过期 claim 不可直接续租复活；heartbeat 独立推进。
- 丢失资格后停止新增业务动作；旧协程可能仍运行，但不允许提交关键写入。
- 终态 delivery 可 ACK；其他 owner 有效时保留 delivery，异步等待并带 jitter 再检查。
- DB 无法恢复时关闭 channel，使未 ACK 消息可重投；非法消息进入 dead-letter queue。

等待方案会占用 prefetch 槽位，是第一版的吞吐限制。
不能把“只有一个有效 owner”描述成“绝无两个请求在物理执行”。

### RabbitMQ 与双写缺口

使用持久队列、持久消息、manual ACK、publisher confirms，并检测无法路由的发布。
先提交终态，再 ACK；发布确认不明确时容忍重复发送。

固定 Broker 版本和队列类型，检查 ACK timeout 能覆盖任务与等待时长。
数据库 heartbeat 不会自动延长 Broker 的 ACK timeout。

DB 提交与 MQ 发布不是同一事务。
第一版依赖幂等重试和手动 dispatch；transactional outbox 属于后续改进。

### 重试与预算

- 暂时性 timeout、连接错误、429 / 可恢复 5xx：有限指数退避与 jitter。
- 非法工具、错误参数、权限问题：默认 FAILED。
- 单次调用有 timeout，run 有 deadline，step 有跨恢复保留的 attempt_count。
- 调用前持久化尝试计数；崩溃可能消耗一次预算，但重启不能重置预算。
- 预算耗尽前先核对已提交 effect，避免把已成功动作误认为尚未执行。

### 状态与取消

```text
PENDING -> RUNNING -> COMPLETED / FAILED / CANCELLED
```

终态不可重开；需要重新执行时创建新 run。
在调用前、工具提交前后、重试等待和最终提交时检查取消。

完成与取消竞争：最终提交事务已看到取消标记则 CANCELLED，否则 COMPLETED。
终态先提交后再收到取消请求，保持终态。

取消不会回滚已提交库存预留。CANCELLED 的 run 仍可能有有效业务记录。

<a id="delivery"></a>

## 8. 实现顺序与验收

### 实现顺序

1. API、数据库表、migration、提交幂等。
2. fake LLM、库存工具、单 Worker 正常执行。
3. RabbitMQ、claim、heartbeat、delivery 处理。
4. 持久化 action、checkpoint、事务幂等与恢复。
5. 取消、重试预算、deadline、终态竞争。
6. 故障测试、演示脚本、批量结果与 README。

### 必须通过的测试

- 重复提交：同 key 只创建一个 run；不同 payload 返回 409。
- 重复消息：只有一个有效 claim，已完成任务不重跑。
- Worker kill：lease 未过期时新 Worker 不提前 ACK 丢失恢复机会。
- effect 后 kill：库存只减少一次，最终可以补齐 checkpoint。
- 模型重放不同动作：仍恢复已持久化 action。
- 旧 owner 恢复：写入和过期续租被拒绝。
- 两个 run 同时预留：不会超卖或产生负库存。
- 取消/完成竞争、多次崩溃：遵循终态规则与持久化预算。
- 发布失败、Broker 重启：分别验证对应恢复路径。

使用可控暂停点定位崩溃窗口，不只依赖随机 sleep。

### GitHub 展示内容

README 保留用途、启动方法、请求实例、三种演示和限制。
本蓝图保留函数契约、数据分层、事务边界与恢复解释。

三种演示：正常任务、Worker 崩溃恢复、已提交副作用去重。
批量报告记录机器配置、执行并发、模拟延迟、完成/失败数、p95 延迟和恢复时间。

### 技术参考

- [RabbitMQ ACK 与 Publisher Confirms](https://www.rabbitmq.com/docs/confirms)
- [RabbitMQ 可靠性](https://www.rabbitmq.com/docs/reliability)
- [RabbitMQ Consumer 与 ACK timeout](https://www.rabbitmq.com/docs/consumers)
- [PostgreSQL 事务隔离](https://www.postgresql.org/docs/current/transaction-iso.html)
