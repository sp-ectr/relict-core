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
        shard_id: The shard index this bot is assigned to. Determines which BrainWorker process handles this bot's LLM sessions. Defaults to 0 (single-worker mode).
        timezone: Timezone string (e.g., "Europe/Moscow") used for scheduling and time-based operations.
        llm_client_name: Name of the language model client to use. Defaults to "gemini".
    """
    id: int | None = None
    chat_id: int
    admin_id: int
    timezone: str
    shard_id: int = 0
    llm_client_name: str = "gemini"


class Participant(BaseModel):
    """
    Participant of a specific chat managed by the bot. Added to the database automatically
    after the first interaction.

    Attributes:
        config_id: ID of the bot configuration the participant is associated with; indexed for faster lookup.
        user_id: Unique identifier of the participant from an external source (e.g., Telegram via aiogram);
            indexed together with config_id.
        user_name: Name remembered or assigned by the bot, or the name the participant provided.
        relationship_score: Bot's perception of the participant, used to influence responses.
            Default is 50. Bot adjusts up or down based on interactions.
        is_ignored: If True, the bot silently drops all messages from this participant.
        last_interaction_at: Timestamp of the participant's last interaction with the bot.
        memories: List of participant's long-term memories stored by the bot. None if not yet loaded.
    """
    config_id: int
    user_id: int
    user_name: str
    relationship_score: int = 50
    is_ignored: bool = False
    last_interaction_at: datetime | None = None
    memories: list[str] | None = None


class ParticipantInfo(BaseModel):
    """
    Lightweight participant snapshot passed to the LLM on every pulse.
    Contains only the fields relevant for LLM context building.

    Attributes:
        user_id: Platform-specific user identifier.
        custom_name: Name remembered or assigned by the bot.
        relationship_score: Current relationship score (0-100).
        memories: Long-term memories associated with this participant. None if none exist.
    """
    user_id: int
    custom_name: str | None = None
    relationship_score: int
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
        pattern=r"^(silence_lock|silence_counter|rate_limit_user|messages_stream):[a-zA-Z0-9_-]+$"
    )

    @classmethod
    def silence_lock(cls, config_id: int) -> "RedisKey":
        return cls(key=f"silence_lock:{config_id}")

    @classmethod
    def silence_counter(cls, config_id: int) -> "RedisKey":
        return cls(key=f"silence_counter:{config_id}")

    @classmethod
    def rate_limit(cls, user_id: int) -> "RedisKey":
        return cls(key=f"rate_limit_user:{user_id}")

    @classmethod
    def messages_stream(cls, config_id: int) -> "RedisKey":
        return cls(key=f"messages_stream:{config_id}")


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
        pattern=r"^(bot_config:[a-zA-Z0-9_-]+"
                r"|participant_config:[a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+)$"
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
                                     r"|messages_stream:[a-zA-Z0-9_-]+"
                                     r"|system_stream"
                                     r"|session_stream:shard_\d+"
                                     r"|brain_stream)$")
    group: Literal["test", "operators", "schedulers", "brain_workers", "response_workers"]
    consumer: str = Field(default="consumer_0",
                          pattern=r"^consumer_\d|operator_\d+|scheduler_\d+|brain_worker_\d+|response_worker_\d+$")

    @classmethod
    def raw_messages(cls, consumer: str) -> "StreamContext":
        return cls(stream="raw_messages", group="operators", consumer=consumer)

    @classmethod
    def message_stream(cls, config_id: int, consumer: str) -> "StreamContext":
        return cls(stream=f"messages_stream:{config_id}", group="brain_workers", consumer=consumer)

    @classmethod
    def system_stream(cls, consumer: str) -> "StreamContext":
        return cls(stream="system_stream", group="schedulers", consumer=consumer)

    @classmethod
    def session_stream(cls, shard: int, consumer: str) -> "StreamContext":
        return cls(stream=f"session_stream:shard_{shard}", group="brain_workers", consumer=consumer)

    @classmethod
    def brain_stream(cls, consumer: str) -> "StreamContext":
        return cls(stream="brain_stream", group="response_workers", consumer=consumer)


class WorkerIdentity(BaseModel):
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
    worker_name: Literal["operator", "scheduler", "brain_worker", "response_worker"]
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
    data_id: str | None = None
    payload: dict | None = None
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
        min_gap_between_sessions_min: Minimum pause between consecutive sessions in minutes.
            Simulates natural breaks in human activity. Must be between 30 and 360. Defaults to 60.
        max_gap_between_sessions_min: Maximum pause between consecutive sessions in minutes.
            Simulates longer distractions or offline periods. Must be between 60 and 360. Defaults to 180.
        min_pulse_interval_sec: Minimum delay between individual actions (pulses) within a session.
            Acts as a hard rate-limit against LLM spam. Must be between 10 and 3600 seconds. Defaults to 60.
        max_pulse_interval_sec: Maximum delay between individual actions within a session.
            Simulates a "thinking" or "distracted" delay. Must be between 30 and 3600 seconds. Defaults to 180.
    """
    day_start_hour: int = Field(default=9, ge=0, le=23)
    day_end_hour: int = Field(default=23, ge=0, le=23)

    min_sessions_per_day: int = Field(default=3, ge=1, le=10)
    max_sessions_per_day: int = Field(default=5, ge=2, le=15)

    min_gap_between_sessions_min: int = Field(default=60, ge=30, le=360)
    max_gap_between_sessions_min: int = Field(default=240, ge=60, le=360)

    min_session_duration_min: int = Field(default=20, ge=5, le=60)
    max_session_duration_min: int = Field(default=60, ge=10, le=240)

    min_pulse_interval_sec: int = Field(default=90, ge=10, le=3600)
    max_pulse_interval_sec: int = Field(default=300, ge=30, le=3600)


class Pulse(BaseModel):
    """
    Represents a single, precise moment for the bot to act.

    Attributes:
        timestamp: Exact datetime of the pulse.
        label: Time-of-day label (morning, day, evening, night).
        is_first_of_slot: True if this is the opening pulse of the activity slot.
        is_last_of_slot: True if this is the closing pulse of the activity slot.
    """
    timestamp: datetime
    label: str
    is_first_of_slot: bool = False
    is_last_of_slot: bool = False


class SessionSlot(BaseModel):
    """
    Represents a macro-level window of bot activity.

    Attributes:
        start: Datetime when the slot begins.
        end: Datetime when the slot ends.
    """
    start: datetime
    end: datetime


class PersonalityManifest(BaseModel):
    """
    Core identity and behavioral contract for the bot.
    Serialized via model_dump_json() and passed ONCE as system_instruction
    when the LLM session opens. Never sent again — model holds it in context.
    """
    role: str = Field(
        description="[DEV] Who the bot is."
    )
    goal: str = Field(
        description="[DEV] Primary objective."
    )
    response_style: str = Field(
        description="[DEV] Communication style."
    )
    pulse_behavior: str = Field(
        description="[DEV] Pulse behavior rules."
    )
    relationship_rules: str = Field(
        default=(
            "Evaluate relationships on 0-100 scale (0=ignore, 100=trust). "
            "CRITICAL: output ONLY the CHANGE (delta), NOT absolute value. "
            "Range: -20 to +20 per pulse. "
            "Example: +10 for wise thought, -15 for insult."
        )
    )
    memories_behavior: str = Field(
        default=(
            "Max 10 memories per participant, oldest auto-deleted. Be selective. "
            "ALWAYS write in English. Max 5 words per entry. Facts only. "
            "Example: 'likes philosophy, reads'."
        )
    )
    restrictions: list[str] = Field(default_factory=list)


class LLMRequest(BaseModel):
    """
    Full context package sent to the LLM on every pulse.

    Attributes:
        timestamp: Exact time of the pulse. Use it to understand time of day and context.
        label: Time-of-day label — morning, day, evening, or night.
        participants_info: Known participants keyed by user_id (int).
            If a user_id appears in messages but not here — they are new.
            Introduce yourself in your persona's style and collect their info.
        messages: Recent chat messages keyed by user_id (int).
            Format: {user_id: "username: message text"}.
            Use user_id as the key to match messages with participants_info.
    """
    timestamp: datetime = Field(
        description="Exact time of the pulse. Use it to understand time of day, "
                    "how long you were offline, and what mood fits the moment."
    )
    label: str = Field(
        description="Time-of-day label: morning, day, evening, or night. "
                    "Use it to set the tone of your response."
    )
    participants_info: dict[int, ParticipantInfo] = Field(
        default={},
        description="Known participants keyed by user_id. "
                    "If a user_id from messages is missing here — this is a new person. "
                    "Introduce yourself and return their info in new_participants."
    )
    messages: dict[int, str] = Field(
        default={},
        description="Recent chat messages keyed by user_id. "
                    "Format: {user_id: 'username: message text'}. "
                    "Empty dict means no new messages — decide whether to initiate or stay silent."
    )
    is_first_of_slot: bool = Field(
        description="True if this is the first pulse of the activity slot. "
                    "You just came online. Improvise where you've been if asked."
    )
    is_last_of_slot: bool = Field(
        description="True if this is the last pulse of the slot. "
                    "Wrap up naturally, say goodbye in your persona's style."
    )

    @property
    def engine_directives(self) -> str:
        lines = []
        for name, field in self.model_fields.items():
            if field.description:
                lines.append(f"- {name}: {field.description}")
        return "\n".join(lines)


class LLMResponse(BaseModel):
    """
    Structured response returned by the LLM after processing a pulse.
    Always return a valid JSON object matching this schema exactly.
    Use null for fields that are not applicable this pulse.

    Attributes:
        text_reply: The message to send to the chat.
            None if the bot decides to stay silent this pulse.
        new_memories: New long-term memories to save, keyed by user_id.
            Be selective — memory is limited to 10 entries per participant,
            oldest are deleted automatically. Only save what truly matters.
        respect_updates: Updated relationship scores keyed by user_id.
            Return only scores that changed this pulse. Values must be 0-100.
            Score 0 triggers permanent ignore automatically.
        new_participants: Info about newly introduced participants keyed by user_id.
            Only return if introduction happened this pulse.
            Each entry must contain: custom_name (str), gender (male/female/unknown).
        set_block: List of user_ids to permanently block.
            Use only when a hard restriction from persona was violated.
    """
    text_reply: str | None = Field(
        default=None,
        description="Message to send to chat. None = stay silent this pulse."
    )
    new_memories: dict[int, str] | None = Field(
        default=None,
        description="Long-term memories keyed by user_id. Max 10 per participant. "
                    "ALWAYS write in English. Max 5 words per entry. Facts only. "
                    "Example: 'likes philosophy, reads'. dict[int, str]"
    )
    respect_updates: dict[int, int] | None = Field(
        default=None,
        description="Relationship score DELTA keyed by user_id. "
                    "MUST be integer change, NOT absolute score. "
                    "Example: +10 for wise thought, -15 for insult. Range: -20 to +20. user_id: 10 dict[int, int]"
    )
    new_participants: dict[int, str] | None = Field(
        default=None,
        description="Newly introduced participants keyed by user_id. "
                    "Required field: user_name str. dict[int, str]"
    )
    set_block: list[int] | None = Field(
        default=None,
        description="user_ids to permanently block. Only on hard restriction violation."
    )
