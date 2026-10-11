"""Insert demo stock without resetting existing quantities or reservations."""

import asyncio

from sqlalchemy.dialects.postgresql import insert

from app.db.models import Inventory
from app.db.session import close_db, session_scope


async def main():
    try:
        async with session_scope() as session:
            async with session.begin():
                await session.execute(insert(Inventory).values([
                    {"sku": "BOOK-001", "available": 10},
                    {"sku": "BOOK-002", "available": 0},
                    {"sku": "BOOK-003", "available": 20},
                ]).on_conflict_do_nothing(index_elements=[Inventory.sku]))
        print("Demo SKUs ready; existing quantities left unchanged")
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
