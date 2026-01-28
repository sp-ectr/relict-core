"""
Base LLM Client Interface
Defines the contract for any LLM client (stateless & stateful).
"""
from typing import Protocol, runtime_checkable, Any


@runtime_checkable
class BaseLLMClient(Protocol):
    """
    Contract for any LLM client.

    Rules:
    - All methods are async; external code expects awaitable API.
    - generate_single / send_in_session return raw text (str).
    - Errors raise LLMError.
    - start_session creates context; end_session removes it idempotently.
    - session_id (SessionId) is an arbitrary identifier for the caller.
    """

    async def start_session(self, session_id: str | int, system_prompt:  -> str:
        """Initialize a stateful session (chat)."""
        pass

    async def send_in_session(self, session_id: str | int, prompt: dict[str, Any] | list[Any]) -> str:
        """Send a message in an existing session and receive the response."""

    async def end_session(self, session_id: str | int) -> None:
        """End/delete a session. Idempotent operation."""
        pass
