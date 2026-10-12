# AI Agent Execution Service

A backend framework that executes multi-round Agent runs with durable progress,
bounded retries and recovery after Worker crashes.

The calling Agent supplies decisions through a `DecisionProvider`. This service
validates those decisions, executes registered tools and stores state in PostgreSQL.
`FakeCallerDecisionProvider` simulates the caller for demonstrations; no real LLM
or external decision endpoint is integrated.

```text
Caller -> FastAPI -> PostgreSQL Run -> RabbitMQ -> independent Worker
                                                  |
                              Engine -> DecisionProvider -> TOOL_CALL / FINAL
                                |              ^
                                +-> tool -> observation
                                +-> PostgreSQL actions / Steps / results
```

## What is implemented

- Idempotent submission with an atomic `(caller_scope, Idempotency-Key)` constraint.
- Durable messages, publisher confirmations, manual ACK after terminal persistence,
  and dead-lettering of malformed/unknown Run messages.
- Atomic claims, database-clock leases, independent heartbeat and stale-owner rejection.
- Saved actions and completed observations for multi-round recovery.
- Durable decision attempt budgets, transient-error backoff and a fixed Run deadline.
- Real database stock queries and reservations; stock, effect and reservation commit
  together, deduplicated by the saved action UUID.
- Cancellation intent, result/status queries and cursor-paginated execution traces.

## Run locally

Prerequisites: Python 3.12+, PostgreSQL 17 and RabbitMQ with its management plugin.
Local verification used Windows, Python 3.14 and native services; Docker/WSL is not
required. Existing Windows installation details are in [local setup](docs/local-windows.md).

From the project directory in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
Copy-Item .env.example .env  # First setup only; edit connection values to match your services.
python -m alembic upgrade head
python -m scripts.seed_inventory
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open [Swagger](http://127.0.0.1:8000/docs). Start the demonstration Worker in another
activated terminal:

```powershell
python -m app.worker --decision-provider fake-caller
```

Seeding inserts missing demo SKUs without resetting existing stock: BOOK-001 starts
at 10, BOOK-002 at 0, BOOK-003 at 20. These are example business data, not a real warehouse.

## Try a Run

POST `/runs` with header `Idempotency-Key: your-logical-task-key`:

```json
{"task": "reserve_inventory", "input": {"sku": "BOOK-003", "quantity": 2}}
```

The fake caller queries stock, reserves it, then returns FINAL: three rounds / five
Steps. Reuse the key for resubmission of the same logical task; a new Run can reserve
again. Insufficient stock is a completed business outcome with `reserved=false`.

Or run the read-only end-to-end demo, which submits, replays the same key and queries
the result and trace:

```powershell
python -m scripts.demo_service
# This variation changes stock:
python -m scripts.demo_service --task reserve_inventory --sku BOOK-003 --quantity 2
```

| Endpoint | Purpose |
| --- | --- |
| POST `/runs` | Create (201), replay (200), or reject a mismatched key (409) |
| GET `/runs/{id}` | Status, result/error, deadline and attempt allowance |
| GET `/runs/{id}/steps` | Completed decisions/tools, Run error and per-round attempt counts |
| POST `/runs/{id}/cancel` | Record cooperative cancellation intent |
| POST `/runs/{id}/dispatch` | Republish a nonterminal Run without resetting its progress |

`dispatch_status` reports CONFIRMED, UNCONFIRMED or SKIPPED. For UNCONFIRMED, replay
the same key or use dispatch. Trace pages accept `limit=1..100` (default 50) and a
Run-specific `cursor`. LLM_CALL is the storage label for caller decisions. Failed or
in-flight calls are not completed Steps; inspect the Run error and reserved attempts.

## Test

Use a dedicated database, separate from `ai_agent`. With local services running:

```powershell
$env:TEST_DATABASE_URL = 'postgresql+asyncpg://app_user:app_password@127.0.0.1:5432/ai_agent_test'
python -m scripts.create_test_database  # Once; requires the local PostgreSQL admin password.
python -m pytest -q
```

Tests start their own API on a free port and allocate a unique RabbitMQ queue. They
run normal execution, concurrent submission, lease loss, retry/deadline enforcement,
stock rollback/deduplication, trace pagination and real Worker kill/redelivery cases.
The demo API and Worker can remain running. See [test setup and coverage](docs/testing.md).

[GitHub Actions workflow](.github/workflows/ci.yml) runs the suite with PostgreSQL,
RabbitMQ and Python 3.12/3.14. The project is published at
[KoOoNnnn/ai_agent_execution_service](https://github.com/KoOoNnnn/ai_agent_execution_service);
pushes trigger CI, whose hosted result must be checked separately from local tests.
Local evidence is in [verification notes](docs/verification.md).

## Scope and limits

Defaults: 30s lease / 10s heartbeat, 10s decision-call timeout, three attempts per
round, and 300s Run deadline including queue time. Attempts and deadline survive
recovery; replays and heartbeat do not reset them.

- Recovery uses saved actions and completed Steps; independent checkpoint snapshots
  and per-attempt error histories are not implemented.
- Reservation deduplication applies to this same-database tool, not arbitrary external
  APIs. Cancellation/deadline does not undo committed stock reservations; release is absent.
- Database-to-MQ dual writes use manual dispatch repair; transactional outbox is absent.
- No real provider adapter, authentication/multi-tenancy, expiration sweeper, tool retry
  loop or Worker supervisor. An expired queued Run is finalized when a Worker receives it.
- The Compose file currently provisions PostgreSQL only and remains unverified locally.
  It is not a complete container deployment of the service.

[Architecture and module responsibilities](docs/architecture.md) ·
[Current implementation and project memory (Chinese)](AI_Agent_Execution_Service_Blueprint_v2.md)
