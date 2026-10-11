"""Registered database tools; the Engine supplies the claim and saved operation ID."""

from typing import Any
from uuid import UUID

from app.db import inventory_store
from app.db.run_store import Claim
from app.schemas import GetInventoryAction, ToolAction


async def execute_tool(claim: Claim, action: ToolAction, operation_id: UUID) -> dict[str, Any]:
    if isinstance(action, GetInventoryAction):
        return await inventory_store.get_inventory(claim, action.arguments.sku)
    return await inventory_store.reserve_once(
        claim, operation_id, action.arguments.sku, action.arguments.quantity,
    )
