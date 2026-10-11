# Existing native Windows setup

This machine uses native PostgreSQL and RabbitMQ services. Docker and WSL are not
needed to run the project.

| Component | Local location |
| --- | --- |
| Project / virtual environment | D:\ai_agent_execution_service / .venv |
| PostgreSQL 17 program | D:\PostgreSQL\17 |
| PostgreSQL data | D:\PostgreSQLData\17 |
| Erlang | D:\Erlang |
| RabbitMQ program | D:\RabbitMQ\rabbitmq_server-4.3.6 |
| RabbitMQ data | D:\RabbitMQData |

PostgreSQL listens on localhost:5432. The demo database is ai_agent with role app_user;
use the actual connection from your private .env. The test database is ai_agent_test.
The project contains no committed PostgreSQL admin password.

RabbitMQ listens on localhost:5672, management on http://127.0.0.1:15672. The local
demo uses guest/guest. This is an existing local setup, not a public deployment guide.

VSCode SQLTools connection settings are local and ignored by Git. Open
scripts/inspect_runs.sql to inspect Run ownership, attempts, actions, effects and stock.

The existing docker-compose.yml only provisions PostgreSQL. It has not been run
successfully on this machine, and does not provision API/Worker/RabbitMQ.
