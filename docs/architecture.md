# Architecture and reliability boundaries

## Execution flow

1. The API commits a Run, then publishes its ID with broker confirmation.
2. A Worker acquires the Run under a row lock, with a unique claim UUID and DB-clock lease.
3. The Engine restores completed tool observations and the next saved action, if present.
4. Without a saved action, it commits an attempt reservation before calling the provider.
5. A validated decision is saved before tool execution. Its UUID is the tool operation ID.
6. Stock reservation locks the Run and SKU, then commits stock/effect/reservation together.
7. Action completion, completed Steps and Run progress commit in one transaction.
8. The Worker ACKs after a terminal state is persisted. A crash leaves delivery unacked.

The Engine depends on the provider interface. The demo Worker explicitly injects the
fake caller; the Engine does not instantiate a model. Only registered tools are accepted.

## Module responsibilities

| File | Responsibility |
| --- | --- |
| app/config.py | Environment settings and timing validation |
| app/main.py | API creation and resource lifespan |
| app/api.py | HTTP validation/status codes and response mapping |
| app/schemas.py | Public request/response and validated decision contracts |
| app/broker.py | Durable queues, confirmations, publishing and consumption |
| app/worker.py | Claim acquisition, heartbeat, execution lifetime and ACK/NACK |
| app/engine.py | Decision loop, action reuse, retries, deadline and tool orchestration |
| app/decision_provider.py | Caller adapter interface and typed error categories |
| app/fake_llm.py | Demo simulation of the calling Agent's decisions |
| app/tools.py | Dispatch to the registered database tools |
| app/db/session.py | Async connections and per-operation sessions |
| app/db/run_store.py | Submission idempotency, claims, lease and Run queries |
| app/db/execution_store.py | Attempts, saved actions, completed Steps, terminal writes and trace pages |
| app/db/inventory_store.py | Fenced stock transactions and business-effect replay |
| app/db/models.py | Shared ORM metadata and table definitions |
| migrations/ | Ordered schema changes; application startup does not create tables |

## Database responsibilities

All tables share one PostgreSQL database; these are logical layers.

| Layer | Tables | Purpose |
| --- | --- | --- |
| Task/scheduling | runs | Identity, input, status, budget, deadline and ownership |
| Execution/recovery | decision_budgets, actions, steps | Reserved attempts, immutable decisions and completed observations |
| Business deduplication | tool_effects | Original per-action success/rejection result |
| Example business | inventory, reservations | Available stock and successful reservations |

Run and Step uniqueness constraints bound duplicate submission/progress. An action
is unique per Run/iteration. tool_effects uses the action UUID as its primary key;
reservations makes that operation ID unique. A stock constraint prevents negative quantities.

## Crash examples

| Interruption point | Recovery |
| --- | --- |
| Before decision is saved | Repeat provider call only if durable attempt allowance remains |
| After action commit, before tool | Reuse the decision; do not ask the provider again |
| After reservation commit, before iteration commit | Reuse the saved effect/result; do not deduct stock again |
| After iteration commit | Restore completed observations and advance to the next round |
| After terminal commit, before ACK | Redelivery sees terminal Run and ACKs without execution |

Lease expiry alone does not requeue a RabbitMQ message. Connection closure or explicit
NACK enables redelivery; acquisition after expiry gives a new claim UUID. Old-owner
progress and stock writes are rejected. Heartbeat continues during caller retries.

## Timings and observations

Attempt allowance and original deadline are snapshotted when the Run is created.
An attempt reserved just before a crash still counts, even if no network call occurred.
Local asynchronous execution is bounded by remaining DB time; progress/stock guards
also check the deadline. Terminal cleanup still requires valid ownership. Cancellation
observed at cleanup takes priority; otherwise expired deadline overrides a late error.

Each trace page reads status, attempts and Steps in a repeatable-read snapshot. Cursor
ordering is `(iteration_index, decision-before-tool)`; later pages may see appended rounds.
An active Run whose page has no next_cursor must be refreshed from the first page.

No general external exactly-once claim is made. Unknown provider requests may continue
remotely after local cancellation. Read-only tools may observe newer stock when replayed.
Committed reservations are not compensated automatically.
