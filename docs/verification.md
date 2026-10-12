# Local verification evidence

Date: 2026-10-11. Environment: native Windows services, PostgreSQL 17.11,
RabbitMQ 4.3.6 and project Python 3.14. No real LLM calls were used.

## Automated suite

Command, after selecting the isolated ai_agent_test database:

```powershell
python -m pytest -q --junitxml=reports/tests.xml
```

Observed result: **14 passed in 68.50s**. This counts scenario groups; each group
contains multiple assertions. The migration check was included. The JUnit report
is local at reports/tests.xml and intentionally ignored by Git.

The suite used a separate test database, random queue and temporary API port.
It covered real Worker termination after progress, after the inventory effect commit
and during the third decision attempt. The effect case preserved stock 10 -> 8 with
one reservation/effect after recovery; the third-attempt case did not start a fourth call.

## End-to-end demonstration

`python -m scripts.demo_service` against the normal local API/Worker completed a
read-only BOOK-003 inventory query. Same-key replay reused Run
01273019-9c17-4f67-b29a-89407b8710de. It completed in two rounds with three Steps;
the result reported PostgreSQL stock 20. This UUID belongs only to the local database.

## Evidence limits

No throughput/latency benchmark, external side-effect guarantee, successful hosted CI
result, real provider integration or full container deployment is claimed. The 68.50s
value is test-suite duration, not a production performance measurement.

## Publication

The public repository is https://github.com/KoOoNnnn/ai_agent_execution_service.
Initial upload commit 1bd4573262fbba22971f26e620e336ff87a75801 preserved the GitHub
initialization commit and both local implementation/documentation commits. All 65
remote file blobs matched the local tree; .env and .venv were excluded. The first
push-triggered CI run started; completion/success is a separate verification.
