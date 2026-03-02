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

    Allowed namespaces:
        - bot_config:<chat_id>
        - participant_config:<bot_id>:<user_id>

    Attributes:
        key: The full Redis key string. Must match the allowed pattern.
        model: The Pydantic model class associated with this key.
            Used by the Redis client to deserialize stored JSON.

    Example:
        key = RedisData.bot_config(chat_id=123)
        result = await redis.get_data(key)  # returns BotConfig | None
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
        stream: Name of the Redis stream. Must be either 'raw_messages'
            or 'messages_stream:<chat_id>'.
        group: Consumer group name. Restricted to known system groups.
        consumer: Consumer identifier within the group.
            Defaults to 'consumer_0'. Must match 'operator_<index>' pattern.
    """
    stream: str = Field(..., pattern=r"^(raw_messages|messages_stream:\d+)$")
    group: Literal["operators"]
    consumer: str = Field(default="consumer_0", pattern=r"^operator_\d+")


class WorkerIdentety(BaseModel):
    """
    Identity descriptor for a worker instance.

    Used to generate consistent consumer names for Redis stream groups
    and to distinguish workers in logs.

    Attributes:
        worker_name: Role of the worker. Currently only 'operator' is supported.
        index: Numeric index of this worker instance. Used to differentiate
            multiple workers of the same type running concurrently.

    Properties:
        consumer_name: Derived consumer identifier in the format '<worker_name>_<index>'.
            Used when registering the worker as a consumer in a Redis stream group.
    """
    worker_name: Literal["operator"]
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