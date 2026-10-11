"""Create a separate test database if absent; never drop or reset a database."""

import asyncio
import os
from pathlib import Path
import re

import asyncpg
from dotenv import dotenv_values
from sqlalchemy.engine import make_url


async def main():
    target = os.environ.get("TEST_DATABASE_URL")
    if not target:
        raise SystemExit("Set TEST_DATABASE_URL to a dedicated ai_agent_test database first")
    url = make_url(target)
    if not url.database or not re.fullmatch(r"ai_agent_test(?:_[a-z0-9_]+)?", url.database):
        raise SystemExit("Only databases named ai_agent_test or ai_agent_test_* are allowed")
    if not url.username or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", url.username):
        raise SystemExit("The database owner must be a simple PostgreSQL role name")
    local = dotenv_values(Path(__file__).resolve().parents[1] / ".env")
    password = os.environ.get("POSTGRES_ADMIN_PASSWORD") or local.get("POSTGRES_ADMIN_PASSWORD")
    if not password:
        raise SystemExit("Set POSTGRES_ADMIN_PASSWORD or create the test database manually")
    connection = await asyncpg.connect(
        host=url.host or "localhost", port=url.port or 5432, database="postgres",
        user=os.environ.get("POSTGRES_ADMIN_USER", "postgres"), password=password,
    )
    try:
        if not await connection.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", url.database):
            await connection.execute(f'CREATE DATABASE "{url.database}" OWNER "{url.username}"')
            print("Created isolated test database:", url.database)
        else:
            print("Test database already exists; left unchanged:", url.database)
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
