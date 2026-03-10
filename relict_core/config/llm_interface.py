"""
Base LLM Client Interface
Defines the contract for any LLM client (stateless & stateful).
"""
from typing import Protocol, runtime_checkable

from relict_core.config.schemas import LLMRequest, PersonalityManifest, LLMResponse


@runtime_checkable
class BaseLLMClient(Protocol):
    """
    Contract for any LLM client.
    """

    async def start_session(self, session_id: str | int, system_instruction: PersonalityManifest,
                            prompt: LLMRequest) -> LLMResponse:
        """Initialize a stateful session (chat)."""
        pass

    async def send_in_session(self, session_id: int, request: LLMRequest) -> LLMResponse:
        """Send a message in an existing session and receive the response."""

    async def end_session(self, session_id: str | int) -> None:
        """End/delete a session. Idempotent operation."""
        pass
