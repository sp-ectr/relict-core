"""
Base LLM Client Interface
Defines the contract for any LLM client (only stateful).
"""
from typing import Protocol, runtime_checkable
from events import LLMRequestStart, LLMRequestEnd, LLMRequestPulse, LLMResponse

@runtime_checkable
class BaseLLMClient(Protocol):

    async def start_session(self, llm_request: LLMRequestStart) -> LLMResponse:
        """Initialize a stateful session (chat)."""
        pass

    async def send_in_session(self, llm_request: LLMRequestPulse) -> LLMResponse:
        """Send a message in an existing session and receive the response."""

    async def end_session(self, llm_request: LLMRequestEnd) -> None:
        """End/delete a session. Idempotent operation."""
        pass