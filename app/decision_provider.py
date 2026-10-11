"""Contract for decisions supplied by the calling Agent's adapter."""

from typing import Any, Protocol


class DecisionProvider(Protocol):
    async def decide(self, context: dict[str, Any]) -> dict[str, Any]:
        """Return TOOL_CALL or FINAL for the current run/iteration/context."""
        ...


class DecisionProviderError(Exception):
    """A decision adapter reports a defined failure to the execution framework."""

    def __init__(self, message: str, code: str = "DECISION_PROVIDER_ERROR") -> None:
        super().__init__(message)
        self.code = code


class TransientDecisionProviderError(DecisionProviderError):
    """Adapters use this for temporary 429/5xx failures eligible for retry."""

    def __init__(self, message: str, code: str = "DECISION_TRANSIENT") -> None:
        super().__init__(message, code)
