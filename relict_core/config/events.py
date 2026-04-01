"""
Defines the event models for the system's event-driven architecture.

Each class represents a unique, strictly-typed event that components
(Producers and Consumers) use to communicate via Redis Streams.
"""
import uuid
from datetime import datetime

from pydantic import BaseModel, Field, computed_field

from relict_core.config.schemas import LLMResponse


class BaseEvent(BaseModel):
    """
    An abstract base model for all system events.

    Provides a unique `trace_id` for observability and a computed
    `event_type` for deserialization and routing.
    """
    trace_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="A unique identifier to trace the event across the entire system."
    )

    @computed_field
    @property
    def event_type(self) -> str:
        """The class name of the event, used for routing by consumers."""
        return self.__class__.__name__


# --- Raw Input Events
class RawMessage(BaseEvent):
    """
    Raw inbound message received directly from the platform (e.g. Telegram).
    Passed as-is into the raw_messages stream for further processing.

    Attributes:
        chat_id: Platform-specific chat identifier.
        user_id: Platform-specific user identifier.
        user_name: Display name of the sender.
        text: Raw message text.
    """
    chat_id: int
    user_id: int
    user_name: str
    text: str


class Message(BaseEvent):
    """
    A lightweight, normalized message object ready for the LLM prompt.
    Contains only the essential data needed for generating a response.
    """
    user_id: int
    user_name: str
    text: str


# --- System Lifecycle Events


class EventStart(BaseEvent):
    """
    Signals that a new bot configuration has been successfully created.
    Triggers the scheduling of daily and pulse-based activities.
    """
    config_id: int


class EventClean(BaseEvent):
    """
    Signals that a user has requested a full cleanup for a chat configuration.
    Initiates the teardown of all scheduled jobs and associated data.
    """
    config_id: int
    chat_id: int


# --- Scheduler Command Events ---

class CommandDayStart(BaseEvent):
    """
    A command to begin the daily activity cycle for a configuration.
    Triggers the planning of pulses for the day.
    """
    config_id: int


class CommandDayEnd(BaseEvent):
    """
    A command to end the daily activity cycle.
    Triggers the cleanup of any active sessions or pulse jobs.
    """
    config_id: int


# --- Pulse & Turn Events ---

class CommandPulse(BaseEvent):
    """
    A fine-grained trigger for the bot to "act" (think or speak).
    Represents a single beat in the bot's "heartbeat".
    """
    config_id: int
    label: str
    timestamp: datetime
    is_first_of_slot: bool = False
    is_last_of_slot: bool = False


class Response(BaseEvent):
    """
    Carries the LLM's structured response back to the ResponseWorker
    for delivery and state updates.

    Attributes:
        config_id: ID of the bot configuration this response belongs to.
        content: Structured LLM output including reply text and side effects.
    """
    config_id: int
    content: LLMResponse
