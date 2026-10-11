"""Fenced stock transactions and per-action business-effect deduplication."""

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select

from app.db.models import Action, Inventory, Reservation, ToolEffect
from app.db.run_store import Claim, require_claim
from app.db.session import session_scope


class ToolCancelled(RuntimeError):
    """Cancellation was visible before the stock transaction committed."""


async def get_inventory(claim: Claim, sku: str) -> dict[str, Any]:
    async with session_scope() as session:
        async with session.begin():
            run = await require_claim(session, claim)
            if run.cancel_requested:
                raise ToolCancelled("Cancellation requested")
            stock = await session.get(Inventory, sku)
            if stock is None:
                raise ValueError("Unknown inventory SKU")
            return {"sku": sku, "available": stock.available, "source": "postgresql"}


async def reserve_once(claim: Claim, operation_id: UUID, sku: str, quantity: int) -> dict[str, Any]:
    """Commit stock, effect and reservation together; replay returns the stored result."""
    if not sku or type(quantity) is not int or not 1 <= quantity <= 1_000_000:
        raise ValueError("Reservation requires a SKU and a positive integer quantity")
    arguments = {"sku": sku, "quantity": quantity}
    async with session_scope() as session:
        async with session.begin():
            run = await require_claim(session, claim)  # Same-run calls serialize here.
            if run.cancel_requested:
                raise ToolCancelled("Cancellation requested")
            action = await session.get(Action, operation_id)
            if (
                action is None or action.run_id != claim.run_id or action.status != "DECIDED"
                or action.iteration_index != run.iteration_count + 1
                or action.decision_payload != {
                    "kind": "TOOL_CALL", "tool_name": "reserve_inventory", "arguments": arguments,
                }
            ):
                raise ValueError("Operation must match the current persisted reservation action")
            effect = await session.get(ToolEffect, operation_id)
            if effect is not None:
                if effect.run_id != claim.run_id or effect.tool_name != "reserve_inventory" or effect.arguments_payload != arguments:
                    raise ValueError("Operation ID does not match its committed effect")
                return effect.result_payload

            # Different runs compete for the same SKU under this stock row lock.
            stock = await session.scalar(select(Inventory).where(Inventory.sku == sku).with_for_update())
            # Waiting for a stock lock may outlast the lease; reject before any mutation.
            await require_claim(session, claim)
            if stock is None:
                raise ValueError("Unknown inventory SKU")
            result = {
                "operation_id": str(operation_id), "sku": sku, "quantity": quantity,
                "reserved": stock.available >= quantity,
            }
            reservation_id = None
            if result["reserved"]:
                stock.available -= quantity
                reservation_id = uuid4()
                result["reservation_id"] = str(reservation_id)
            else:
                # Persist rejection too, so replay cannot turn an old rejection into a success.
                result["reason"] = "INSUFFICIENT_STOCK"
            result["available"] = stock.available
            session.add(ToolEffect(operation_id=operation_id, run_id=claim.run_id,
                                   tool_name="reserve_inventory", arguments_payload=arguments,
                                   result_payload=result))
            # Insert the referenced effect before the reservation; both remain uncommitted.
            await session.flush()
            if reservation_id is not None:
                session.add(Reservation(id=reservation_id, operation_id=operation_id, sku=sku, quantity=quantity))
                await session.flush()
        return result
