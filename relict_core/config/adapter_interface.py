"""
Transport-agnostic interface for outbound message delivery.

Defines a minimal contract for messaging adapters
(Telegram, Discord, etc.).
"""
from typing import Protocol, runtime_checkable


@runtime_checkable
class BaseAdapter(Protocol):
    """Contract for message delivery adapters."""

    async def send_message(self, chat_id: int | str, text: str) -> None:
        """Send a text message to a specific chat."""
        ...

    async def send_typing(self, chat_id: int | str) -> None:
        """Show typing indicator in the chat."""
        ...

    async def close(self) -> None:
        """Close underlying connection or session."""
        ...