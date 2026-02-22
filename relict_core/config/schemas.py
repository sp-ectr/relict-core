from pydantic import BaseModel, Field
from datetime import datetime
from typing import Literal


class SQLParams(BaseModel):
    """
    SQL execution parameters.

    Attributes:
        query: SQL string with placeholders $1, $2
        params: Tuple of SQL parameters
        mode: Execution mode ('execute', 'fetch_all', 'fetch_row', 'fetch_val')
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
        llm_client_name: Optional name of the language model client to use; can be None if not configured.
    """
    id: int = None
    chat_id: int
    admin_id: int
    timezone: str
    llm_client_name: str = None


class Participant(BaseModel):
    """
      Participant of a specific chat managed by the bot. Added to the database automatically
      after the first interaction.

      Attributes:
          id: Unique identifier of the participant in the database. Assigned automatically when the record is created.
          config_id: ID of the bot configuration the participant is associated with; indexed for faster lookup.
          user_id: Unique identifier of the participant from an external source (e.g., Telegram via aiogram); indexed together with config_id.
          custom_name: Name remembered or assigned by the bot, or the name the participant provided.
          gender: Participant's gender.
          relationship_score: Bot's perception of the participant, used to influence responses.
              Default is 50. Bot adjusts up or down based on interactions.
          is_ignored: Boolean status. If True, the bot does not receive messages from this participant.
          last_interaction_at: Timestamp of the participant's last interaction with the bot.
          memories: List of participant's long-term memories; None if not loaded.
      """
    id: int = None
    config_id: int
    user_id: int
    custom_name: str
    gender: str
    relationship_score: int = 50
    is_ignored: bool = False
    last_interaction_at: datetime = None
    memories: list[str] | None = None


class RedisKey(BaseModel):
    key: str = Field(..., pattern=r"^(bot_config:\d+|silence_lock:\d+|silence_counter:\d+)$")


class StreamContext(BaseModel):
    stream: str = Field(..., pattern=r"^(raw_messages|messages_stream:\d+)$")
    group: Literal["operators"]
    consumer: str = Field(default="consumer", pattern=r"^(operator_\d+")


class WorkerIdentety(BaseModel):
    worker_name: Literal["operator"]
    index: int

    @property
    def consumer_name(self) -> str:
        return f"{self.worker_name}_{self.index}"


class RawStreamData(BaseModel):
    data_id: str = None
    payload: dict = None
    error: bool = False
