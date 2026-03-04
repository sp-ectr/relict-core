"""
Core Pydantic schemas for the Relict engine.

This module defines all shared data models used across the system:
database query parameters, domain entities (BotConfig, Participant),
Redis key wrappers, stream configuration, worker identity,
and raw stream message containers.

All models are strict by design — field patterns, literals, and generics
are used to enforce correctness at the boundary layer, before data
reaches business logic.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Literal, TypeVar, Generic

T = TypeVar("T", bound=BaseModel)


class SQLParams(BaseModel):
    """
    SQL execution parameters passed to the database client.

    Attributes:
        query: SQL string with positional placeholders ($1, $2, ...).
        params: Tuple of values bound to the query placeholders. Defaults to empty tuple.
        mode: Execution mode controlling how results are returned.
            - 'execute': fire-and-forget, no result returned.
            - 'fetch_all': returns all rows as a list.
            - 'fetch_row': returns a single row as a mapping.
            - 'fetch_val': returns a single scalar value.
    """
    query: str
    params: tuple = ()
    mode: Literal["execute", "fetch_all", "fetch_row", "fetch_val"] = "execute"


class BotConfig(BaseModel):
    """
    Configuration of a bot for a specific chat. Controls bot behavior, scheduling,
    and associated LLM client.

    Attributes:
        id: Unique identifier of the configuration. Assigned automatically when the record is created in the database.
        chat_id: Unique identifier of the chat; indexed in the database for faster lookup.
        admin_id: Unique identifier of the admin user for this chat.
        timezone: Timezone string (e.g., "Europe/Moscow") used for scheduling and time-based operations.
        llm_client_name: Name of the language model client to use. Defaults to "gemini".
    """
    id: int | None = None
    chat_id: int
    admin_id: int
    timezone: str
    llm_client_name: str = "gemini"


class Participant(BaseModel):
    """
    Participant of a specific chat managed by the bot. Added to the database automatically
    after the first interaction.

    Attributes:
        id: Unique identifier of the participant in the database. Assigned automatically when the record is created.
        config_id: ID of the bot configuration the participant is associated with; indexed for faster lookup.
        user_id: Unique identifier of the participant from an external source (e.g., Telegram via aiogram);
            indexed together with config_id.
        custom_name: Name remembered or assigned by the bot, or the name the participant provided.
        gender: Participant's gender.
        relationship_score: Bot's perception of the participant, used to influence responses.
            Default is 50. Bot adjusts up or down based on interactions.
        is_ignored: If True, the bot silently drops all messages from this participant.
        last_interaction_at: Timestamp of the participant's last interaction with the bot.
        memories: List of participant's long-term memories stored by the bot. None if not yet loaded.
    """
    id: int = None
    config_id: int
    user_id: int
    custom_name: str
    gender: str
    relationship_score: int = 50
    is_ignored: bool = False
    last_interaction_at: datetime | None = None
    memories: list[str] | None = None


class RedisKey(BaseModel):
    """
    A validated Redis key for simple flag and counter operations.

    Used for keys that store primitive values (empty strings, integers)
    with no associated Pydantic model. Pattern enforces that only
    known key namespaces are used.

    Allowed namespaces:
        - silence_lock:<chat_id>
        - silence_counter:<chat_id>
        - rate_limit_user:<user_id>

    Attributes:
        key: The full Redis key string. Must match the allowed pattern.
    """
    key: str = Field(
        ...,
        pattern=r"^(silence_lock:\d+"
                r"|silence_counter:\d+"
                r"|rate_limit_user:\d+)$"
    )


class RedisData(BaseModel, Generic[T]):
    """
    A validated Redis key paired with its associated Pydantic model type.

    Generic over T (bound to BaseModel) — the type parameter determines
    what model is stored and retrieved for this key. Use the provided
    classmethods as factories to construct instances; this ensures
    the key format and model type are always consistent.

    Attributes:
        key: The full Redis key string. Must match the allowed pattern.
        model: The Pydantic model class associated with this key.
            Used by the Redis client to deserialize stored JSON.

    """
    key: str = Field(
        ...,
        pattern=r"^(bot_config:\d+"
                r"|participant_config:\d+:\d+)$"
    )
    model: type[T]

    @classmethod
    def bot_config(cls, chat_id: int) -> "RedisData[BotConfig]":
        """Create a RedisData key for BotConfig by chat ID."""
        return cls(key=f"bot_config:{chat_id}", model=BotConfig)

    @classmethod
    def participant_config(cls, bot_id: int, user_id: int) -> "RedisData[Participant]":
        """Create a RedisData key for Participant by bot config ID and user ID."""
        return cls(key=f"participant_config:{bot_id}:{user_id}", model=Participant)


class StreamContext(BaseModel):
    """
    Context required to interact with a Redis Stream consumer group.

    Attributes:
        stream: Name of the Redis stream.
        group: Consumer group name. Restricted to known system groups.
        consumer: Consumer identifier within the group.
    """
    stream: str = Field(..., pattern=r"^(raw_messages"
                                     r"|messages_stream:\d+"
                                     r"|system_stream"
                                     r"|session_stream)$")
    group: Literal["test", "operators", "schedulers"]
    consumer: str = Field(default="consumer_0", pattern=r"^operator_\d+|scheduler_d+$")


class WorkerIdentety(BaseModel):
    """
    Identity descriptor for a worker instance.

    Used to generate consistent consumer names for Redis stream groups
    and to distinguish workers in logs.

    Attributes:
        worker_name: Role of the worker.
        index: Numeric index of this worker instance. Used to differentiate
            multiple workers of the same type running concurrently.

    Properties:
        consumer_name: Derived consumer identifier in the format '<worker_name>_<index>'.
            Used when registering the worker as a consumer in a Redis stream group.
    """
    worker_name: Literal["operator", "scheduler"]
    index: int

    @property
    def consumer_name(self) -> str:
        """Returns the Redis consumer name derived from worker identity."""
        return f"{self.worker_name}_{self.index}"


class RawStreamData(BaseModel):
    """
    A single message read from a Redis stream consumer group.

    Returned by the Redis client after reading from a stream. If parsing
    fails, the error flag is set to True and payload will be None.
    The data_id is always present and must be acknowledged regardless
    of the error state.

    Attributes:
        data_id: Redis stream message ID (e.g. '1699999999999-0').
            Used to acknowledge the message after processing.
        payload: Deserialized message content as a dictionary.
            None if the message could not be parsed.
        error: True if the message was malformed and could not be deserialized.
            Consumers should acknowledge and skip messages with error=True.
    """
    data_id: str = None
    payload: dict = None
    error: bool = False


class SchedulerSettings(BaseModel):
    """
    Biological clock configuration for the bot entity.

    Defines the behavioral rhythm of the bot, controlling when it is active
    and how frequently it interacts (pulses). This acts as the core personality
    mechanic, preventing spam and simulating human-like presence.
    All fields enforce strict boundaries to prevent system overload or API abuse.

    Attributes:
        day_start_hour: Hour (0-23) when the bot wakes up and can start sessions.
            Defaults to 9.
        day_end_hour: Hour (0-23) when the bot goes to sleep. Defaults to 22.
        min_sessions_per_day: Minimum number of active communication windows per day.
            Must be between 1 and 10. Defaults to 5.
        max_sessions_per_day: Maximum number of active communication windows per day.
            Must be between 2 and 15. Defaults to 7.
        min_session_duration_min: Minimum length of a single active session in minutes.
            Must be between 5 and 60. Defaults to 20.
        max_session_duration_min: Maximum length of a single active session in minutes.
            Must be between 10 and 240. Defaults to 40.
        min_pulse_interval_sec: Minimum delay between individual actions (pulses) within a session.
            Acts as a hard rate-limit against LLM spam. Must be between 10 and 3600 seconds. Defaults to 60.
        max_pulse_interval_sec: Maximum delay between individual actions within a session.
            Simulates a "thinking" or "distracted" delay. Must be between 30 and 3600 seconds. Defaults to 180.
    """
    day_start_hour: int = Field(default=9, ge=0, le=23)
    day_end_hour: int = Field(default=22, ge=0, le=23)

    min_sessions_per_day: int = Field(default=5, ge=1, le=10)
    max_sessions_per_day: int = Field(default=7, ge=2, le=15)

    min_session_duration_min: int = Field(default=20, ge=5, le=60)
    max_session_duration_min: int = Field(default=40, ge=10, le=240)

    min_pulse_interval_sec: int = Field(default=60, ge=10, le=3600)
    max_pulse_interval_sec: int = Field(default=180, ge=30, le=3600)

class Pulse(BaseModel):
    """Represents a single, precise moment for the bot to act."""
    timestamp: datetime
    label: str
    is_first_of_day: bool = False
    is_last_of_day: bool = False

class SessionSlot(BaseModel):
    """Represents a macro-level window of bot activity."""
    start: datetime
    end: datetime
