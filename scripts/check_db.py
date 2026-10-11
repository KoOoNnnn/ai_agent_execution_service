"""Run with: python -m scripts.check_db"""

import asyncio

from sqlalchemy import text

from app.db.session import close_db, init_db, session_scope


async def main() -> None:
    try:
        await init_db()
        async with session_scope() as session:
            result = await session.execute(text("SELECT 1"))
            print("Database connection OK; SELECT 1 =", result.scalar_one())
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
