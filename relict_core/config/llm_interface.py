"""
Base LLM Client Interface
Defines the contract for any LLM client (stateless & stateful).
"""
from typing import Protocol, runtime_checkable, Any

from relict_core.config.schemas import LLMRequest, PersonalityManifest, LLMResponse


@runtime_checkable
class BaseLLMClient(Protocol):
    """
    Contract for any LLM client (stateless & stateful).

    Attributes:
        sessions: Active sessions keyed by session_id.
            Used to determine whether to start a new session or reuse an existing one.
    """
    sessions: dict[str | int, Any]

    async def start_session(self, session_id: str | int, system_instruction: PersonalityManifest,
                            prompt: LLMRequest) -> LLMResponse:
        """Initialize a stateful session (chat)."""
        ...

    async def send_in_session(self, session_id: str | int, request: LLMRequest) -> LLMResponse:
        """Send a message in an existing session and receive the response."""
        ...

    async def end_session(self, session_id: str | int) -> None:
        """End/delete a session. Idempotent operation."""
        ...
