"""Database connections and sessions; business stores own their transactions."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings


engine = create_async_engine(
    str(get_settings().database_url),
    pool_pre_ping=True,
)

session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,
)


async def init_db() -> None:
    """Check connectivity without creating or modifying tables."""
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT 1"))
        if result.scalar_one() != 1:
            raise RuntimeError("Database connection check returned an unexpected result")


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """Provide a separate session per operation; callers explicitly commit."""
    async with session_factory() as session:
        yield session


async def close_db() -> None:
    """Release pooled connections during application shutdown."""
    await engine.dispose()
