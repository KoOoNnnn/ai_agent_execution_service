# Automated integration tests

## Requirements and isolation

Install development dependencies with `python -m pip install -e ".[dev]"`.
PostgreSQL and RabbitMQ (including management on port 15672) must be reachable.
The suite requires TEST_DATABASE_URL; it refuses demo database names and accepts
only ai_agent_test or ai_agent_test_*.

```powershell
$env:TEST_DATABASE_URL = 'postgresql+asyncpg://app_user:app_password@127.0.0.1:5432/ai_agent_test'
python -m scripts.create_test_database
python -m pytest -q --junitxml=reports/tests.xml
```

Database creation reads POSTGRES_ADMIN_PASSWORD from the environment or local .env
without printing it. It only creates a missing test database and assigns the URL's
application role as owner; it never drops/resets a database. Alternatively, an admin
can create the database manually. CI service containers create it at startup.

The suite migrates/seeds the test database, selects a free API port, and creates a
random agent.tests.* queue. It terminates its own processes and deletes only those
exact test queues/exchange. Test DB records remain for inspection. Temporary logs
are under `.venv/pytest-tmp`; reports and logs are ignored by Git. Run the suite
serially; concurrent suites sharing one test database are not supported.

Override RABBITMQ_URL and RABBITMQ_MANAGEMENT_URL if services use other ports or
credentials. Current queue checks use the default `/` vhost. Normal demo services
on port 8000 and queue agent.runs can remain running.

## Coverage

Tests invoke existing fault-scenario scripts in separate Python processes. This
preserves their assertions and avoids sharing asyncio connection pools across loops.
Pytest reports scenario groups, not one case per assertion.

| Test group | Evidence checked |
| --- | --- |
| Engine | Multi-round loop, terminal replay, invalid decisions, cancellation and iteration limit |
| Actions | Immutable action reuse, interrupted tool boundary, stale claim and saved FINAL |
| Inventory | Concurrent deduplication, no oversell, rollback, stable rejection and effect recovery |
| Retry/deadline | Transient-only retries, cancellation/backoff, durable allowance and deadline enforcement |
| Leases | Atomic claim, stale writes, heartbeat during a long call and ownership loss |
| API | Concurrent key reuse/conflict, cancellation, trace order/pagination and invalid requests |
| Queue | Offline Worker, duplicate dispatch, terminal ACK and malformed-message dead-lettering |
| Real Worker crashes | Kill after progress, after stock effect and during the third decision attempt |
| Schema | Alembic metadata matches the migrated database |

The fault tests use saved markers before terminating fixture Workers, rather than
assuming a crash point from elapsed time. They cannot prove every distributed failure
interleaving, external exactly-once behavior, throughput or production readiness.

## CI

`.github/workflows/ci.yml` runs on push, pull_request and manual dispatch. Each Python
3.12/3.14 job starts PostgreSQL 17 and RabbitMQ 4.3 management containers, waits for
service health, installs the project and runs pytest. JUnit and service logs are uploaded.
Credentials in that workflow are disposable test-service credentials.

Local Windows verification does not establish that hosted Linux CI is green. The first
GitHub run is still required; no workflow badge is shown before a repository exists.

## Individual checks

The scripts/check_*.py modules remain usable for learning and SQLTools inspection.
Their default API is http://127.0.0.1:8000; API_BASE_URL can select an isolated API.
Manual cancellation/queue/crash scripts require the normal Worker stopped.
For check_retry use lease 2s / heartbeat 0.5s; for check_leases use lease 6s / heartbeat 1s.
The automated harness supplies these timings and manages consumers itself.
