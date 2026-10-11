"""Demo-only simulation of the caller's decision service; no model/API calls.

Demo scripts inject this adapter. The Engine does not import or select it.
"""

from typing import Any

from app.decision_provider import DecisionProviderError


class FakeCallerDecisionProvider:
    async def decide(self, context: dict[str, Any]) -> dict[str, Any]:
        if context["task"] not in {"check_inventory", "reserve_inventory"}:
            raise DecisionProviderError(
                "The fake caller only supports check_inventory and reserve_inventory",
                code="UNSUPPORTED_TASK",
            )
        if not context["observations"]:
            return {
                "kind": "TOOL_CALL",
                "tool_name": "get_inventory",
                "arguments": {"sku": context["input"].get("sku")},
            }
        if context["task"] == "reserve_inventory" and len(context["observations"]) == 1:
            return {
                "kind": "TOOL_CALL", "tool_name": "reserve_inventory",
                "arguments": {"sku": context["input"].get("sku"),
                              "quantity": context["input"].get("quantity")},
            }
        return {"kind": "FINAL", "result": context["observations"][-1]}
