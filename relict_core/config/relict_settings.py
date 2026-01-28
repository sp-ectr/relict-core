from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class _BaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent.parent / ".env",
        case_sensitive=False,
        extra="ignore"
    )


class PostgresSetting(_BaseSettings):
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    db_name: str

    @property
    def database_url(self) -> str:
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


class RedisSettings(_BaseSettings):
    redis_host: str
    redis_port: int


class SchedulerSettings(_BaseSettings):
    DAY_START_HOUR: int
    DAY_END_HOUR: int
    MIN_SESSIONS_PER_DAY: int
    MAX_SESSIONS_PER_DAY: int
    MIN_SESSION_DURATION_MIN: int
    MAX_SESSION_DURATION_MIN: int
    MIN_PULSE_INTERVAL_SEC: int
    MAX_PULSE_INTERVAL_SEC: int
