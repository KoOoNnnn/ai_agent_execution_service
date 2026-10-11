# AI Agent Execution Service — 当前实现与项目记忆

核对日期：2026-10-11。代码基线：本地提交 `059f68c`，结合当前工作目录核对。
本文用于把项目背景、代码职责与设计取舍交给后续 ChatGPT；描述当前实现，不把旧蓝图或后续设想当作完成项。
依据：`app/`、`migrations/`、`tests/`、`scripts/`、README 与本地测试记录。本文不包含实际密码或 token。

## 0. 一页项目背景

**项目定位：** 面向 0–2 年 backend/software engineer 的个人作品集，一个可持久化、可恢复的多轮 Agent 执行框架。
关注任务状态、异步执行、并发所有权、重试预算与业务副作用幂等；模型能力不是本项目的验证目标。

**核心关系：** 调用方负责决定下一步，框架负责可靠地推进和记录执行。
服务没有训练、托管或集成自己的 LLM。`DecisionProvider` 是调用方决策适配器接口，不是 LLM 模型本身。
当前 Worker 显式注入 `FakeCallerDecisionProvider`，它模拟调用方 Agent 的决策；不是假装已经接入真实模型。

**当前能力：** 提交/查询/取消/补派发；RabbitMQ 与独立 Worker；多轮工具决策；claim、lease、heartbeat；
已保存 action 的恢复；持久化决策尝试次数与 deadline；真实数据库库存查询/幂等预留；Steps 分页；自动集成测试。

**明确未完成：** 真实 HTTP DecisionProvider、独立 checkpoint 表、outbox、认证/多租户、动态 DAG、任意工具接入、
工具重试循环、预留释放/补偿、自动到期扫描、Worker supervisor、完整容器部署和性能基准。

**证据状态：** 本地 Windows/Python 3.14，14 个测试组通过。CI 已配置 Python 3.12/3.14，但云端结果尚未验证。
本地已提交代码；最近核对未配置 Git remote，GitHub 上传仍待完成登录和推送。计划使用公开仓库，但不能写成已发布。

## 阅读导航

- [1. 框架与边界](#boundary)
- [2. 文件与方法职责](#modules)
- [3. HTTP 接口](#api)
- [4. 数据库分层](#database)
- [5. 一次任务如何推进](#execution)
- [6. 崩溃、重复与取消示例](#recovery)
- [7. 预算与错误处理](#budgets)
- [8. 设计思考与 trade-off](#tradeoffs)
- [9. 运行、测试和交付证据](#evidence)
- [10. 后续讨论的准确表述](#discussion)

<a id="boundary"></a>

## 1. 框架与边界

```text
调用方 -> FastAPI -> PostgreSQL：先提交 Run
                  -> RabbitMQ：再发送 run_id
                         |
                    独立 Worker：领取资格、维持 heartbeat、处理 ACK/NACK
                         |
                       Engine
                         |-> DecisionProvider.decide(context) -> TOOL_CALL / FINAL
                         |-> 注册工具 -> observation -> 下一轮决策
                         `-> PostgreSQL：attempts / actions / Steps / 结果
```

RabbitMQ 投递的是 `{"run_id": "UUID"}`，不是完整上下文；一条消息触发整个 Run，不是每个 Step 各入队一次。
PostgreSQL 是任务和业务状态来源，Broker 不理解 Run 是否已取消、谁持有有效 lease 或库存是否已扣。

| 组件/参与者 | 当前责任 | 不应推断的能力 |
| --- | --- | --- |
| 调用方 Agent/应用 | 提交 task/input，为同一逻辑请求复用 key；通过适配器提供决策 | 不是终端用户每次手动输入 key；不是已接入某家 LLM |
| API | 校验请求、保存任务、派发、查询 | 不在 HTTP 请求内跑完整多轮任务 |
| DecisionProvider | 输入 context，输出一个工具动作或 FINAL | 没有真实 HTTP 回调、注册或按 Run 切换 provider 的接口 |
| Engine | 决策循环、动作校验、工具调度、持久化与预算 | 不训练模型，不只是日志记录器 |
| Worker | 有资格地执行 Run，维持 lease，处理消息确认 | Broker 分配消息不等于 Worker 自动拥有执行资格 |
| 注册工具 | get_inventory、reserve_inventory | 不接受上游任意工具、代码或 URL 并自动执行 |

重要实现边界：`POST /runs` 只接收 `task` 和 `input`。当前同一个 Worker 使用启动时注入的 provider；
API 没有 `provider_url`/模型参数。未来可实现 HTTP adapter，但须明确认证、超时和错误映射，不能说现阶段已具备它。
每轮最多一个工具调用或一个 FINAL；沒有多工具并行、Agent 协作或动态工作流图。

<a id="modules"></a>

## 2. 文件与方法职责

方法名以下以实际代码为准。API 方法对外映射 HTTP；store 和 Engine 方法是 Python 内部调用，不需要各有 endpoint。

```text
ai_agent_execution_service/
|-- app/
|   |-- main.py, api.py, schemas.py, config.py
|   |-- broker.py, worker.py, engine.py
|   |-- decision_provider.py, fake_llm.py, tools.py
|   `-- db/
|       |-- session.py, models.py
|       `-- run_store.py, execution_store.py, inventory_store.py
|-- migrations/                 # env.py、script.py.mako、versions/0001..0009
|-- tests/                      # conftest.py、test_api.py、test_execution.py、test_worker.py
|-- scripts/                    # 演示、造测试数据、故障验证、SQL 查询
|-- docs/                       # architecture、testing、verification、local-windows
|-- .github/workflows/ci.yml
|-- .env.example, .gitignore, pyproject.toml, alembic.ini
|-- docker-compose.yml          # 仅 PostgreSQL，未在本机验证
|-- README.md
`-- AI_Agent_Execution_Service_Blueprint_v2.md  # 本文
```

各包的 `__init__.py` 当前只标识 Python 包。以下表格合并职责相近的方法，避免逐行复述代码。

### 2.1 API、配置与连接

| 文件 / 真实方法或类型 | 做什么，以及为什么放在这里 |
| --- | --- |
| `config.py`：Settings、get_settings() | Pydantic 读取并校验环境变量；缓存配置，避免各模块重复处理连接与时序参数 |
| `Settings.validate_lease_timing()` | 要求 heartbeat_interval × 3 ≤ lease_ttl，留出续租余量；不等于网络永不超时 |
| `main.py`：create_app()、lifespan() | 创建 FastAPI、挂载路由；启动验证 DB，退出释放 DB/Broker 资源 |
| `api.py`：create_run_endpoint() | 创建或复用 Run，提交后派发，映射 201/200/409；不自己写库存事务 |
| `get_run_endpoint()`、list_steps_endpoint() | 返回状态和轨迹，把 ORM/store 结果转成响应 schema |
| `cancel_run_endpoint()`、dispatch_run_endpoint() | 分别记录取消意图、补发消息；不是直接杀 Worker 或重置任务 |
| `schemas.py`：CreateRunRequest、RunResponse、RunCreatedResponse、DispatchResponse | 稳定的 HTTP 契约；拒绝调用方随意设置内部状态、claim 或预算字段 |
| StepResponse、StepPage、DecisionAttemptsResponse | 轨迹与分页契约；错误是 Run 级字段，尝试次数不是逐次错误日志 |
| GetInventoryAction、ReserveInventoryAction、FinalAction | 校验决策格式、工具名与参数；ToolAction 是按 tool_name 区分的联合类型 |
| `db/session.py`：init_db() | 执行 SELECT 1 验证连接；不负责建表 |
| session_scope()、close_db() | 提供独立 AsyncSession、释放连接池；事务由具体 store 的 session.begin() 管理 |
| `db/models.py`：Base 与各 ORM class | Base 汇集 metadata；各 class 描述表与约束。Base 不代表一个任务，也不存 Run state |

SQLAlchemy AsyncEngine 使用 asyncpg；连接池启用 pool_pre_ping，session 使用 expire_on_commit=False。
ORM 可以表达 SQL，但业务事务边界仍须显式设计；Alembic 创建/演进表，不靠启动时 create_all 偷改数据库。
API 启动要求 DB 可用；RabbitMQ 不可用时记录警告仍允许启动，派发失败通过 UNCONFIRMED 呈现。

### 2.2 `run_store.py`：任务与执行资格

| 方法 / 类型 | 用途与必要性 |
| --- | --- |
| create_run(input_payload) | 内部演示/测试直接创建独立 Run；不会自动发消息 |
| create_or_get_run(input_payload, request_key, caller_scope) | API 提交入口；唯一约束 + INSERT ON CONFLICT 原子去重，避免“先查再插”的并发竞态 |
| get_run(run_id) | 普通查询，找不到返回 None；读取不需要执行 claim |
| request_cancel(run_id) | 锁定 Run 写 cancel_requested；重复取消与终态请求保持原状 |
| new_run_limits() | 创建时快照 decision_max_attempts 和 deadline_at；以后重派发不能增加额度 |
| Claim(run_id, claim_id, worker_id) | 本次领取的凭证；worker_id 标识进程，claim_id 标识这一次领取 |
| try_claim_run(run_id, worker_id) | 行锁 + DB 时钟，只有无 owner/已过期的非终态 Run 才能获得新 claim |
| require_claim(session, claim, check_deadline=True) | 在已有事务里锁 Run，验证身份、状态、lease、期限；保留锁直到该事务结束 |
| assert_claim(claim) | 短事务包装 require_claim，供 Engine 在安全位置检查资格 |
| renew_lease(claim) | 仅有效 claim 能延长 lease；已过期的旧 claim 不能复活 |
| run_seconds_remaining(run, db_now)、remaining_run_seconds(claim) | 用 DB 时间算剩余 Run 总时间，供 Engine 的本地 timeout 使用 |

IdempotencyConflict 表示同 scope/key 对应了不同规范化请求；LeaseLost 表示已无执行资格。
RunDeadlineExceeded 和 RunCancelled 分别阻止超期推进与开始新的决策尝试。
require_claim 默认检查 deadline；续租和终态清理可跳过 deadline，但仍必须有有效 lease/身份。
过期任务可先获得 claim，再由 Engine 持久化失败；没有后台自动清理所有过期任务。

### 2.3 `execution_store.py`：计划、计数和进度

| 方法 | 用途与必要性 |
| --- | --- |
| begin_decision_attempt(claim, iteration) | 网络调用前先提交 attempt_count；崩溃不退款，避免重启重置重试预算 |
| load_observations(claim) | 读取已完成轮次的 TOOL_CALL 输出，重建下一轮上下文 |
| load_action(claim, iteration) | 恢复时先找已保存决策，避免同一轮再次询问 provider 得到不同动作 |
| save_action(claim, iteration, input_payload, decision_payload) | 保存校验后的决策与原输入；相同内容复用 ID，不同内容不能覆盖 |
| require_next_iteration(run, iteration) | 只能提交 iteration_count + 1，且不超过 max_iterations，防止跳轮/超预算 |
| save_iteration(claim, iteration, steps, final_result=None) | 一个事务里保存 Steps、标记 action COMPLETED、更新轮数；FINAL 同时写终态和结果 |
| finish_failure(claim, code, message) | 有资格地写 FAILED/CANCELLED；取消优先，否则过期 deadline 覆盖晚到错误 |
| clear_ownership(run) | 终态清空 claim_id、worker_id、lease_expires_at，避免残留所有权 |
| list_steps(run_id, limit, cursor) | 一次只读 REPEATABLE READ 快照读取 Run、计数和 Steps，避免同页数据不一致 |
| encode_step_cursor()、decode_step_cursor() | 用版本、Run ID、轮次、step 类型表示继续位置；验证 cursor 属于目标 Run |

不是每个动作单独 commit 任意字段：完成一轮的相关执行记录需要原子一致。
注意：action 的 DECIDED 只表示“该轮尚未完成提交”，不表示工具从未执行；失败/取消 Run 可以保留 DECIDED action。

### 2.4 Engine、provider 与工具

| 文件 / 方法 | 用途与必要性 |
| --- | --- |
| `engine.py`：execute_run(claim, decision_provider) | 根据原 deadline 包住整段执行；到期后进行有资格的终态清理 |
| advance_run(claim, decision_provider) | 检查取消/轮数、恢复 action、调用 provider、执行工具、保存进度，串起每轮业务 |
| call_decision_with_retry(claim, provider, context) | 消耗持久化次数，设置单次 timeout，仅暂时性错误退避重试 |
| parse_action(response) | Pydantic 联合类型校验 TOOL_CALL/FINAL；非法响应在执行工具前失败 |
| `decision_provider.py`：DecisionProvider.decide(context) | Protocol 约定输入/输出；让 Engine 不依赖 fake provider 或某家模型 SDK |
| DecisionProviderError、TransientDecisionProviderError | 适配器标记永久/暂时错误；框架不自动理解所有 HTTP 异常 |
| `fake_llm.py`：FakeCallerDecisionProvider.decide(context) | 以 task/observations 模拟调用方决策，便于确定性演示和注入故障 |
| `tools.py`：execute_tool(claim, action, operation_id) | 路由到允许的工具；operation_id 使用已保存的 Action.id，不在重试时重新生成 |
| `inventory_store.py`：get_inventory(claim, sku) | 校验资格/取消后读真实库存，未知 SKU 抛业务错误 |
| reserve_once(claim, operation_id, sku, quantity) | 验证持久化动作与参数，复用 effect，或锁库存并原子保存业务变更 |

Engine 对 provider 输入使用 deepcopy，防止适配器直接修改其维护的上下文。
真实适配器未来须把可重试 429/5xx 等映射到 TransientDecisionProviderError；当前只验证模拟错误，不是已验证真实 HTTP 重试。

### 2.5 Broker 与 Worker

| 方法 | 用途与必要性 |
| --- | --- |
| `Broker.connect()` | robust 连接、声明 durable 主队列/死信队列/交换机；连接锁避免并发重复建立 |
| Broker._publish()、publish_run(run_id) | 发送 persistent 消息，mandatory + publisher confirms；确认不明确时返回 UNCONFIRMED |
| Broker.consume(handler) | 设置 prefetch 并以 manual ACK 消费；Broker 不判断数据库执行资格 |
| Broker.close() | 关闭连接；未 ACK 消息在连接丢失后可被重投 |
| `worker.py`：execute_with_lease(run_id, provider, worker_id=None) | 领取后并行运行 Engine/heartbeat；终态直接返回，别人持有 lease 时报告 RunAlreadyRunning |
| heartbeat_loop(claim) | 独立续租，不被 provider 调用或退避等待阻塞；续租失败触发停止执行 |
| Worker.handle_delivery() | 登记活跃 callback，便于退出时取消并回收 |
| Worker._handle_delivery() | 校验消息、等待/执行 Run、ACK/NACK/DLQ；用 semaphore 限制本地并发 |
| Worker.run() | 连接、消费、等待退出；异常时取消自己的任务，关闭连接让未确认消息重投 |
| worker.main() | 要求显式 --decision-provider fake-caller，然后注入示例 provider |

当前主队列是 agent.runs，死信队列 agent.runs.dead，交换机 agent.runs.dlx。
别人仍持有有效 lease 的重复消息保持 unacked，每 1 秒再检查；简单但占用 prefetch/concurrency 位置。

### 2.6 Migration、脚本与测试

| 文件/目录 | 当前用途 |
| --- | --- |
| migrations/env.py、script.py.mako、alembic.ini | 用真实 Settings/Base.metadata 运行迁移；模板与日志配置 |
| versions/0001..0009 | 依次增加 Run、提交幂等、取消、执行记录、lease、action、库存/effect、预算/deadline、稳定默认时间表达式 |
| scripts/seed_inventory.py | 只插入缺失 SKU，不覆盖已有数量 |
| scripts/demo_service.py | 真正通过 API/MQ/Worker 提交、同 key 重放、轮询结果、查询轨迹；默认只读 |
| demo_engine.py、demo_run.py、check_db.py、check_broker.py | 内部执行/建任务演示与连接检查；不都代表完整 HTTP 闭环 |
| scripts/check_*.py、worker_crash_fixture.py | 多个断言与故障注入；真实终止测试只终止自己的 fixture Worker |
| inspect_runs.sql | SQLTools 中观察表、所有权、attempts、action、effect 与库存 |
| create_test_database.py、cleanup_test_queue.py | 创建允许命名的独立测试 DB；清理严格匹配的专用测试队列，不删除演示 DB/队列 |
| tests/conftest.py | 隔离 DB、临时 API 端口、专用队列和子进程；包装既有场景脚本 |
| tests/test_api.py、test_execution.py、test_worker.py | 分组验收接口、执行与真实消息/进程故障 |
| .github/workflows/ci.yml | 配置 Linux 服务容器与两个 Python 版本；不是已验证的本地 Docker 部署 |

Migration 意义：把表结构变更作为版本记录，其他环境仍需主动执行 alembic upgrade head；拉代码不等于自动同步数据库。
`.venv` 隔离 Python 依赖，`.env` 保存运行配置，`.env.example` 提供示例；它们不是同一种“封装环境”。

<a id="api"></a>

## 3. HTTP 接口：真实契约

| Endpoint / Python 方法 | 功能与结果 |
| --- | --- |
| POST /runs / create_run_endpoint | 新任务 201；同 key/请求复用 200；相同 key 不同请求 409 |
| GET /runs/{run_id} / get_run_endpoint | RunResponse；不存在 404 |
| GET /runs/{run_id}/steps / list_steps_endpoint | StepPage；limit 默认 50、范围 1..100；cursor 分页 |
| POST /runs/{run_id}/cancel / cancel_run_endpoint | 200，记录取消意图；终态不改；不存在 404 |
| POST /runs/{run_id}/dispatch / dispatch_run_endpoint | 非终态补发；确认 202，不明确 503；终态 409、不存在 404 |

非法 UUID、请求字段或分页参数返回 422；cursor 不属于目标 Run 也返回 422。
POST /runs 的派发不明确仍可返回 201/200，并带 dispatch_status=UNCONFIRMED，因为数据库任务已经保存。
CONFIRMED 表示 Broker 确认接收，不表示业务执行成功；SKIPPED 表示复用的 Run 已是终态，无须派发。

### 3.1 请求与关键响应示例

请求只允许 task/input；不能把 max_iterations、deadline 或 claim 作为顶层字段传入。
Idempotency-Key 可选，1..200 字符且不能全空白；不传 key，每次都是独立任务。
当前 caller_scope 固定为 local，没有身份认证隔离。

```http
POST /runs
Content-Type: application/json
Idempotency-Key: reservation-order-1001
```

```json
{"task": "reserve_inventory", "input": {"sku": "BOOK-003", "quantity": 2}}
```

新建响应关键字段节选；UUID 是示例占位，不是预设 ID：

```json
{
  "id": "11111111-1111-4111-8111-111111111111",
  "status": "PENDING",
  "iteration_count": 0,
  "max_iterations": 5,
  "decision_max_attempts": 3,
  "dispatch_status": "CONFIRMED"
}
```

RunResponse 还包括 cancel_requested、deadline_at、input_payload、result_payload、error_payload、created_at、updated_at。
对同一逻辑请求重试复用相同 key；数量改成 3 却复用旧 key 会返回 409。新 key 可以合法再次预留。
key 通常由上游 Agent、应用后端或 SDK 生成并随业务请求保存，不要求终端用户自己管理 UUID。
hash 来自规范化 JSON（排序 key 等），但不是任意业务语义等价判断；input 内不同类型仍可能是不同请求。

### 3.2 Steps 响应

StepPage 字段：run_id、status、error_payload、decision_attempts、items、next_cursor。
每条 Step 返回 id、iteration_index、step_type、status、input_payload、output_payload、created_at。
同轮先 LLM_CALL 再 TOOL_CALL；LLM_CALL 在这里是“调用方决策”的历史存储标签。

decision_attempts 是整个 Run 已知各轮的计数，不局限于当前分页；不是每次失败的错误内容或工具尝试次数。
失败调用可以没有 Step，此时仍从 error_payload/decision_attempts 看失败原因和已消耗次数。
存在但没有完成记录的 Run 返回 200/items=[]，不是 404。旧迁移前的记录可能没有 attempt_count，不能自行猜成 1。
next_cursor=null 只表示当前没有后续记录；活跃 Run 后面可能新增轮次，需要刷新第一页。

<a id="database"></a>

## 4. 数据库分层：一个 PostgreSQL，七张业务/执行表

| 逻辑层 / 表 | 实际主要字段与约束 |
| --- | --- |
| 任务调度 / runs | UUID id；caller_scope/request_key/request_hash；input_payload；status/cancel_requested；iteration_count/max_iterations；decision_max_attempts/deadline_at；result_payload/error_payload；claim_id/worker_id/lease_expires_at；时间戳 |
| 执行 / decision_budgets | (run_id, iteration_index) 复合主键；attempt_count，当前每次 provider 调用前增加 |
| 执行 / actions | UUID id；run_id、iteration_index、原 context input_payload、decision_payload、status、created_at；每 Run/轮次唯一 |
| 执行 / steps | UUID id；run_id、iteration_index、step_type、status、input_payload/output_payload、created_at；每 Run/轮次/类型唯一 |
| 幂等 / tool_effects | operation_id 主键并外键到 actions.id；run_id、tool_name、arguments_payload、result_payload、created_at |
| 示例业务 / inventory | sku 主键；available；数据库约束 available ≥ 0 |
| 示例业务 / reservations | UUID id；operation_id 唯一且外键到 tool_effects；sku 外键到 inventory；quantity ≥ 1；created_at |

另有 Alembic 管理的 alembic_version 表记录迁移版本，它不是业务任务表。
runs 的 scope/key 唯一约束负责提交幂等；claim 三字段必须全空或全非空。没有 request_key 时 PostgreSQL 允许多个独立 Run。
Run 状态为 PENDING/RUNNING/COMPLETED/FAILED/CANCELLED；Action 状态为 DECIDED/COMPLETED。
当前所有写出的 Step 都是 COMPLETED；Step 表没有失败尝试详情，也没有 action_id、attempt_count 或耗时字段。

```text
runs -> decision_budgets (按轮计数)
     -> actions (按轮决策) -> tool_effects (按 action 的业务结果)
     -> steps (按轮/类型的已完成记录)       -> reservations -> inventory
```

细节不能沿用旧蓝图：没有 checkpoints/latest_checkpoint_id；effect 保存完整 arguments_payload，不是 arguments_hash；
reservation 本身没有 run_id，需经 effect 关联 Run；Step 和 Action 通过 Run/轮次对应，没有直接外键。
这里的“幂等”有三层：scope/key 去重请求，Run/轮次/类型去重完成记录，operation_id 去重库存副作用。

<a id="execution"></a>

## 5. 一次任务如何推进，以及为什么要按这个顺序

假设 BOOK-003 库存初始为 20；没有其他并发变更。Fake caller 演示预留 2 件。

| 顺序 | 方法与结果 | 为什么需要这一步 |
| --- | --- | --- |
| 1 | create_or_get_run 保存 PENDING、初始额度和原 deadline | API 返回前先有可查询的可信任务；同请求不能建两个 Run |
| 2 | Broker.publish_run 发送 run_id | HTTP 请求与长任务解耦，Worker 离线仍可积压消息 |
| 3 | execute_with_lease -> try_claim_run，Run 变 RUNNING | 收到消息不够，必须取得 DB 里的唯一当前执行资格 |
| 4 | execute_run -> remaining_run_seconds，启动整体 timeout 与独立 heartbeat | 限制总耗时，同时长调用期间不丢有效所有权 |
| 5 | advance_run -> load_observations/load_action | 若是恢复，先利用已完成结果和已经确定的计划 |
| 6 | 无 action 才 begin_decision_attempt -> provider.decide -> parse_action | 调用前扣次数，执行前校验输出，不让重启或非法动作绕过预算 |
| 7 | save_action 先提交决策和固定 ID | 工具执行前，恢复所需的“到底要做什么”已经确定 |
| 8 | execute_tool -> get_inventory/reserve_once | 工具自己也验证 claim，副作用事务不能只信 Engine 的先前检查 |
| 9 | save_iteration 原子保存本轮记录和进度 | 不能出现 Step 已保存但轮次没更新，或 FINAL 结果与状态不同步 |
| 10 | 再循环；FINAL 保存 COMPLETED、清空所有权，Worker ACK | ACK 放在持久化终态后，避免任务完成前消息已被消耗 |

### 5.1 三轮决策示例

第一轮：

```json
{"kind": "TOOL_CALL", "tool_name": "get_inventory", "arguments": {"sku": "BOOK-003"}}
```

工具 observation：

```json
{"sku": "BOOK-003", "available": 20, "source": "postgresql"}
```

第二轮 provider 看到的 context 包含 task、input、run_id、iteration=2、observations=[上面的查询输出]。
它返回：

```json
{"kind": "TOOL_CALL", "tool_name": "reserve_inventory", "arguments": {"sku": "BOOK-003", "quantity": 2}}
```

先保存第二轮 Action.id，然后作为 operation_id 执行预留。reserve_once 的关键顺序：

```text
锁 Run 并验证 claim / lease / deadline / 取消
  -> 验证 operation_id 属于当前保存的预留动作，且 SKU/quantity 一致
  -> 已有 effect：返回原结果
  -> 没有 effect：锁 inventory 对应 SKU
  -> 等锁后再次检查 claim / lease / deadline
  -> 扣库存 + 写 effect + 写 reservation -> 同一事务提交
```

成功工具结果示例：

```json
{
  "operation_id": "22222222-2222-4222-8222-222222222222",
  "sku": "BOOK-003",
  "quantity": 2,
  "reserved": true,
  "reservation_id": "33333333-3333-4333-8333-333333333333",
  "available": 18
}
```

第三轮：fake caller 返回 `{"kind": "FINAL", "result": 最后一个 observation}`；该描述中的 result 是对象，不是字符串。
最终 result_payload 是上面的预留结果。总共 **3 个 iteration / 5 条 Steps**：查询轮 2 条，预留轮 2 条，FINAL 轮 1 条。
同一轮重试不会增加 iteration_count；只增加 decision_budgets.attempt_count。

### 5.2 两种事务为什么分开

事务 A：保存 action。事务 B：提交库存/effect/reservation。事务 C：完成本轮 action/Steps/Run 进度。
工具业务事务与执行进度并非一个跨网络大事务；B 与 C 之间的崩溃窗口靠已提交的 effect 结果桥接。
session.flush 只把变更送到当前 DB 事务，不等于 commit；中途失败会回滚同事务的库存与记录。
effect 先 flush，再插引用它的 reservation，满足外键；直到 session.begin 正常退出才一起提交。

<a id="recovery"></a>

## 6. 崩溃、重复与取消：实际能保证到哪里

| 场景 | 当前恢复行为 | 保证边界 |
| --- | --- | --- |
| 请求重复 / 重复派发 | 同 key/相同请求复用 Run；消息可能重复，终态不再执行 | 请求去重依赖上游复用 key；换 key 是新任务 |
| 决策调用后、action 保存前崩溃 | 有剩余次数才再次调用 provider | 可能得到不同决策，但工具还没开始；已消耗调用不退款 |
| action 已保存、工具未开始 | 新 owner 复用原动作和固定 ID | 不再询问该轮 provider |
| 库存已扣、effect 已提交，轮次未保存 | reserve_once 读取同 operation_id 的原结果，再补齐执行记录 | 库存不会再扣；effect.result.available 是原提交结果，不保证等于此刻库存 |
| 一轮已经完成提交 | 从已完成 observations 开始下一轮 | 不是独立 checkpoint 快照，不恢复任意 Python 内存或模型隐藏状态 |
| 终态已保存、ACK 前崩溃 | 重投后读到终态，直接 ACK | 不重跑工具，不补偿已经提交的业务 |
| Worker 失联/lease 过期 | 连接关闭或 NACK 才启用重投；新 owner 在 lease 过期后可领取 | lease 过期不会自行把 RabbitMQ 消息放回池里 |
| 旧 Worker 恢复后尝试写入 | UUID claim 不匹配，LeaseLost 拒绝推进或库存事务 | 这是 DB 内的所有权校验，不是外部服务都认识的全局 fencing token |

**effect 后崩溃例：** 初始库存 10 -> Worker A 提交预留 2 件和 effect -> 进度还停在前一轮 -> A 被终止。
RabbitMQ 重投，B 等 lease 到期获得新 claim，复用第二轮 action/effect，库存仍为 8；之后保存进度并走 FINAL。
已验证一条 effect、一条 reservation，最后 3 轮/5 Steps。

**库存不足：** reserve_once 保存 reserved=false、reason=INSUFFICIENT_STOCK 的 effect，不扣库存，不建 reservation。
同 operation_id 重放仍返回原拒绝，即使后来补货；换一条新的业务动作才重新判断库存。
fake caller 将此结果返回 FINAL，所以 Run 可以 COMPLETED；“执行完成”不等于“业务预留成功”。

**取消：** /cancel 只记意图。provider 调用/退避期间没有独立即时取消 watcher，下一安全检查或 timeout 才结束。
工具提交前观察到取消，就不扣库存；取消若晚于库存事务，预留已成立，后续 Run 即使 CANCELLED 也不自动还库存。
当前没有 release/compensation；不能从 CANCELLED 或 action=DECIDED 推断“完全没有业务副作用”。

<a id="budgets"></a>

## 7. 预算、时间与错误处理

### 7.1 默认值与快照位置

| 限制 | 默认 | 当前含义 |
| --- | --- | --- |
| max_iterations | 5 | Run 表默认；当前 API 不允许用户设置 |
| decision_max_attempts | 3 | 每轮含第一次最多 3 次；创建时写入 Run，不随 Worker 新配置重置 |
| decision_timeout_seconds | 10 秒 | 每次 provider 调用的本地等待上限；Worker 配置 |
| retry backoff | 1、2 秒 + 0..0.2 秒 jitter | 第 1/2 次暂时失败后的退避；没有持久化下次重试的预约时间 |
| run_timeout_seconds | 300 秒 | 创建时快照成 deadline_at，包含排队；heartbeat/dispatch/重放不延长 |
| lease_ttl / heartbeat_interval | 30 / 10 秒 | 执行所有权时序，与单次调用 timeout、重试额度和 Run deadline 分开 |
| worker_prefetch / concurrency | 1 / 1 | 消息在途数量 / 本 Worker 同时执行数量；不等于总 Worker 数量 |
| broker_timeout_seconds | 5 秒 | 连接/发布确认的等待限制 |

deadline 使用数据库时间建立/校验，再换算成 Engine 的本地 asyncio.timeout；协作式取消不能保证远端 HTTP 请求也停止。
续租不因 deadline 立即拒绝，以便当前 owner 写终态；普通 action、进度与库存操作仍拒绝过期 Run。
数据库断连或无法确认续租时不能承诺准时落库；这不是跨服务的硬实时截止保证。

**保守计数：** attempt 在网络调用前提交。如果随后立刻崩溃，实际 HTTP 可能没发送，但这次仍算消耗。
第三次尝试中断且 action 未保存，恢复直接失败，不发起第四次；如果 action 已保存，即使额度耗尽也可复用它继续工具阶段。
迁移 0008 给旧 Run 回填 created_at+300 秒和上限 3；迁移前真实调用次数不可重建，不能补造历史。
迁移 0009 的 DB fallback default 用 now()，使 Alembic 默认值对比稳定；正常创建路径仍使用 DB clock 建立配置期限。

### 7.2 分类处理

| 条件 | 当前行为 |
| --- | --- |
| TimeoutError、ConnectionError、适配器标记的 TransientDecisionProviderError | 有额度则退避重试；单轮 budget 持久化 |
| 次数耗尽或恢复看到额度已耗尽 | FAILED / DECISION_RETRY_EXHAUSTED，然后 Worker ACK，不无限重新入队 |
| provider 的永久错误 | FAILED，保留适配器 error.code；不重试 |
| 决策格式、工具名或参数不合法 | FAILED / INVALID_ACTION；校验发生在工具执行前 |
| 工具 ValueError，例如未知 SKU | FAILED / TOOL_ERROR；没有工具重试循环 |
| 超过 max_iterations | FAILED / MAX_ITERATIONS |
| 原 Run deadline 已过 | FAILED / RUN_DEADLINE_EXCEEDED；尚未执行的过期排队任务不调用 provider |
| 取消在清理时可见 | CANCELLED 优先，清理所有权 |
| LeaseLost | 停止本次执行，Worker NACK/requeue；旧 owner 不写 FAILED 覆盖新 owner |
| DB 或未预期异常 | Worker 停止并关闭连接，未 ACK 可重投；没有自动 supervisor |
| 格式不合法或 Run 不存在的消息 | reject/requeue=false，进入 DLQ |

重试时 heartbeat 独立继续续租；“重试超过一次 heartbeat 间隔”不表示必然失去资格。
当前不是让单个 Worker 一失败就丢回 Broker：可恢复的短暂决策错误在本轮有限重试，失去所有权才停止推进。

<a id="tradeoffs"></a>

## 8. 设计思考与 trade-off

这些是对当前实现的设计解释，不是未验证的性能结论。

| 当前选择 | 为什么这样做 | 代价 / 以后何时改变 |
| --- | --- | --- |
| API / Engine / store / tool 分层 | HTTP 契约、流程控制和 DB 事务分开，可独立验证；同一内部方法可被 API/Worker/脚本复用 | 小项目文件更多；不为每个小动作继续增加抽象层 |
| DecisionProvider Protocol + 注入 fake caller | 验证执行可靠性不依赖模型输出或调用成本，保留真实 adapter 的接入位置 | 尚未证明真实 Agent/HTTP 集成；适配器注册和多调用方策略仍要设计 |
| 固定注册工具与严格动作 schema | 减少任意输入的执行面，明确库存工具支持范围 | 不通用；新增工具须补契约、路由、业务 store 与故障验证 |
| PostgreSQL 持久化 + RabbitMQ 投递 | DB 管状态/约束，队列管消息与离线积压，避免长期占用 HTTP 请求 | 两个基础设施；DB 和 MQ 存在双写窗口 |
| 提交 DB 后 publish + 手动 dispatch 修复 | 先保住任务记录，publisher confirm 不明确时调用方有可追踪 ID | API 提交后崩溃可能未发消息，不能自动保证最终派发；可靠自动修复可用 outbox |
| UUID claim + Run 行锁 + DB 时间 | 新旧领取可区分，不依赖各 Worker 系统时钟；资格校验与写入共用锁 | 多次短事务、同 Run 串行；不等于禁止所有重复网络调用或外部写入 |
| heartbeat 与 Engine 独立任务 | 长调用和退避期间仍续租；续租失败可取消本次执行 | 生命周期清理更复杂，DB 锁等待也可能导致续租失败；时间值须符合实际延迟 |
| 先保存 action，再执行工具 | 决策固定后可恢复，避免重问模型选择了另一个动作 | 工具提交与进度之间仍有窗口，必须配合 effect 幂等 |
| stock/effect/reservation 同 DB 事务 | 要么都成功，要么都回滚；原 effect 能桥接进度未提交的崩溃窗口 | 同数据库耦合；外部支付/邮件服务须另有 idempotency key、查询或补偿机制 |
| 行锁避免并发超卖 | 不同 Run 修改同 SKU 时串行检查/更新，逻辑容易审计 | 热点 SKU 会等待；需要吞吐数据后再考虑条件 UPDATE 等优化 |
| 调用前持久化 attempt，使用有限退避 | 崩溃不会重置额度，拒绝无限 retry；永久错误快速终止 | 提交后未实际调用也可能损失一次机会；不是精确的网络调用审计 |
| Steps/observations 恢复，暂无独立 checkpoint | 当前上下文简单，避免维护重复快照；完成轮次是清晰恢复边界 | 不能保存任意大状态/部分阶段；复杂上下文时再引入版本化 checkpoint |
| deadline 从提交起计算 | 排队也占预算，任务有固定业务有效期 | 高积压下未执行就过期；如要“开始后执行预算”，应另加执行期限而非偷改原语义 |
| 保持重复消息 unacked 等 owner | 防止提前 ACK 掉可能仍需恢复的投递，当前控制流简单 | 占用 prefetch，频繁重复可能拖慢吞吐；可研究延迟重投/独立恢复扫描，但不能无条件 ACK |
| 轨迹用 keyset cursor + 单页一致快照 | 按轮/类型稳定排序，避免简单 offset 随追加偏移；同页状态/计数不打架 | 全部分页不是冻结快照；当前未签名 cursor，验证格式/Run 范围但它不是认证机制 |
| 用真实服务的隔离集成测试 | 能观察行锁、事务、MQ ACK 和真实进程终止，比只 mock 更贴近主要风险 | 测试较慢且依赖服务；14 个“测试组”不是 14 个独立断言，也不是生产证明 |

关注的核心不变量：终态不被旧 owner 覆盖；同一已保存库存动作最多产生一条已提交 reservation；
进度只能推进下一轮；同一逻辑提交复用 Run；attempt/deadline 不因恢复增加。
这些保证依赖现有受控写入路径，不能直接推断为任意插件/外部系统的 exactly-once。

<a id="evidence"></a>

## 9. 运行、测试和交付证据

### 9.1 本地运行

实际采用 D 盘项目、原生 PostgreSQL/RabbitMQ，Python 3.14；不要求升级 WSL。
首次创建 .venv、安装 `.[dev]`，从 .env.example 配置自己的连接，然后：

```powershell
python -m alembic upgrade head
python -m scripts.seed_inventory
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
# 另一个已激活 .venv 的 terminal：
python -m app.worker --decision-provider fake-caller
```

示例与文档凭据只是本地示例；实际 .env、.venv、VSCode 本地配置和测试报告被 Git 忽略。
具体服务位置见 docs/local-windows.md。Compose 只含 PostgreSQL，没有 API/Worker/RabbitMQ，不能声称完整 Docker 已复现。

### 9.2 验证状态

独立 ai_agent_test DB、临时 API 端口、随机 agent.tests.* 队列运行 pytest；拒绝误用 ai_agent 演示 DB。
测试串行执行，子进程隔离 asyncio/连接池；测试数据留下用于检查，只清理本次专用消息资源。

```powershell
# 设置自己的 TEST_DATABASE_URL 后：
python -m scripts.create_test_database
python -m pytest -q --junitxml=reports/tests.xml
```

记录结果：**14 passed in 68.50s**，来自本次实际本地验证；不是吞吐量、延迟或生产 SLA。
对应 test_execution 7 组（含 schema）、test_api 3 组、test_worker 4 组。
覆盖多轮执行、并发同 key、取消、lease、action 恢复、stock 原子事务、持久化预算/deadline、Steps 分页、ACK/DLQ，
以及“完成一轮后”“effect 提交后”“第三次决策调用中”三个真实 Worker 终止/重投场景。
测试不证明所有竞态交错、网络分区、真实 LLM 兼容或外部 exactly-once。

CI 文件已准备：Ubuntu 24.04、PostgreSQL 17、RabbitMQ 4.3 management、Python 3.12/3.14，上传 JUnit 和服务日志。
**没有云端 CI 成功结果，也没有完整容器部署成功证据。** 当前库文档不应提前展示成功 badge。
详细事实见 docs/testing.md 与 docs/verification.md。

<a id="discussion"></a>

## 10. 后续讨论应保留的准确表述

| 容易误记的说法 | 当前准确表述 |
| --- | --- |
| “我们实现了自己的 LLM” | 实现了决策接口和调用方模拟器；真实 provider 尚未接入 |
| “上游任何工具都能用” | 仅支持明确注册并实现的两个库存工具 |
| “已经有独立 checkpoint” | 用持久化 action 与已完成 Steps 恢复，暂无 checkpoint 表/完整上下文快照 |
| “Broker 分配后就是唯一 Worker” | 消息会重复；DB claim/lease 校验确定当前可提交写入的 owner |
| “超过 heartbeat 的重试就回队列” | heartbeat 独立续租；暂时错误有限重试，lease 丢失停止，耗尽则 FAILED+ACK |
| “重试次数存在 Worker 内存里” | 已写 decision_budgets；每轮最多 3 次含首次，恢复不退款 |
| “取消就撤销库存” | 取消是执行意图；已提交预留不自动补偿 |
| “同 key 会得到最新库存” | 同 key 复用原 Run；同 effect 返回原提交结果，不重做业务 |
| “幂等让所有工具 exactly-once” | 对这个同 DB 的预留事务做副作用去重；不是外部系统的普遍保证 |
| “API 能提交 max_iterations/模型配置” | 当前 CreateRunRequest 只有 task/input，其他顶层字段被拒绝 |
| “迁移会自动同步别人数据库” | 版本文件随 Git 分享，其他环境需要执行 upgrade head |
| “本地测试过就是 CI/性能也过” | 仅本地 14 组集成验证；云端、真实 provider 和基准仍未知 |

可用于作品集的描述：用 FastAPI、PostgreSQL 和 RabbitMQ 构建多轮 Agent 执行框架；通过持久化动作与尝试预算、
lease/heartbeat 和事务内工具幂等实现受控恢复，并用真实 Worker 终止测试验证重复投递不重复扣库存。
不要添加未测的性能数字，不把个人演示项目说成生产部署，不把 fake caller 的确定性决策当作模型能力。

后续扩展按实际需要选择：真实 provider adapter、自动 outbox 派发、checkpoint、身份/scope、release/compensation、
worker supervisor、到期扫描与可观测性。它们是候选方向，不是当前代码已经存在的 module/endpoint。

本文是项目事实快照；后续代码变化时同时更新记忆文档。若只有本文而没有代码，应先核对新实现再修改上述结论。
