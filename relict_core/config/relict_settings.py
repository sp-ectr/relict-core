"""
Pydantic settings models for all system components.

Loads configuration from environment variables and .env file.
Covers PostgreSQL, Redis, LLM client, and bot adapter settings.
"""
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class _BaseSettings(BaseSettings):
    """
    Base class for loading environment variables from a `.env` file.

    This class configures common settings for all application
    configuration objects, including:
    - path to the `.env` file
    - case-insensitive environment variables
    - ignoring extra variables
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


class PostgreSettings(_BaseSettings):
    """
    Configuration object for AsyncPostgreManager.

    Attributes:
        db_user: PostgreSQL username.
        db_password: Password for the PostgreSQL user.
        db_host: Database host.
        db_port: Database port.
        db_name: Database name.
        pool_size_min: Minimum size of the connection pool.
        pool_size_max: Maximum size of the connection pool.

    Properties:
        database_url: Fully constructed DSN string for async PostgreSQL connection.
    """
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    db_name: str
    pool_size_min: int = 1
    pool_size_max: int = 10

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.db_user}:"
            f"{self.db_password}@{self.db_host}:"
            f"{self.db_port}/{self.db_name}"
        )


class RedisSettings(_BaseSettings):
    """
    Configuration object for Redis client.

    Attributes:
        redis_host: Redis server host.
        redis_port: Redis server port.
    """
    redis_host: str
    redis_port: int


class LLMSettings(BaseModel):
    """
    Configuration for LLM client (API credentials and model selection).

    Attributes:
        api_key: API key for the LLM provider.
        model_name: Model identifier to use for inference. Defaults to gemini-2.0-flash.
    """
    api_key: str
    model_name: str = "gemini-2.0-flash"


class AdapterSettings(_BaseSettings):
    """
    Configuration for the bot adapter (authentication settings).

    Attributes:
        bot_token: Authentication token for the bot platform (e.g. Telegram).
    """
    bot_token: str
