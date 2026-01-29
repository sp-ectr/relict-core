"""
Asynchronous PostgreSQL manager built on connection pooling.
Handles pool lifecycle and provides high-level async query methods.
"""

import logging
import asyncio
import asyncpg
from asyncpg import Pool
from asyncpg import exceptions as error_database
from contextlib import asynccontextmanager
from typing import Any, Literal

import relict_core.config.sql_queries as queries
from relict_core.config.exceptions import (DatabaseConnectionError,
                                           DatabaseQueryError, DuplicateUserError,
                                           PoolConnectionError)
from relict_core.config.sql_queries import INSERT_LONG_TERM_MEMORY

logger = logging.getLogger(__name__)


class AsyncPostgresManager:
    """
    Async PostgreSQL manager that creates and manages its own connection pool.
    Provides methods for query execution and safe resource handling.
    """

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: Pool | None = None
        self._is_connected: bool = False

    @property
    def is_connected(self) -> bool:
        return self._pool is not None and self._is_connected

    async def _create_pool(self) -> None:
        """Creates a new connection pool."""
        if self.is_connected:
            raise PoolConnectionError(
                "Pool already exists. Close the current pool before creating a new one."
            )

        for attempt in range(3):
            try:
                self._pool = None
                self._pool = await asyncpg.create_pool(
                    dsn=self._dsn,
                    min_size=1,
                    max_size=10,
                    command_timeout=3
                )
                async with self._pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                    logger.debug("Successfully connected to the pool.")
                    self._is_connected = True
                    return
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1)
                else:
                    self._pool = None
                    self._is_connected = False
                    raise PoolConnectionError(
                        f"Unexpected error during connection attempt {attempt + 1}: {e}"
                    ) from e

    @asynccontextmanager
    async def _pool_acquire(self, timeout: float | None = 3):
        """Acquires a connection from the pool."""
        if not self.is_connected:
            raise PoolConnectionError("Pool is not initialized.")

        try:
            async with self._pool.acquire(timeout=timeout) as conn:
                yield conn
        except asyncio.TimeoutError as e:
            raise PoolConnectionError(
                "Connection pool operation timed out."
            ) from e

    async def _disconnect(self) -> None:
        """Closes all connections in the pool."""
        if not self.is_connected:
            logger.warning(
                "Attempted to close the pool, but it is already closed or not initialized."
            )
            return

        try:
            logger.debug("Initializing shutdown of the databases connection pool.")
            await self._pool.close()
            logger.debug("Database connection pool successfully closed.")
        finally:
            self._is_connected = False
            self._pool = None

    async def _execute(
            self,
            query: str,
            params: tuple = (),
            mode: Literal["execute", "fetch_all", "fetch_row", "fetch_val"] = "execute"
    ) -> Any | None:
        """
        Args:
            query: SQL string with placeholders $1, $2
            params: Tuple of SQL parameters
            mode: SQL execution mode:
                'execute': Returns number of affected rows (for INSERT/UPDATE/DELETE)
                'fetch_all': Returns all rows as a list of dictionaries
                'fetch_row': Returns a single row as a dictionary or None if no data
                'fetch_val': Returns a single value from the first row or None
        Returns:
            Depends on mode:
                'execute': int(number of affected rows)
                'fetch_all': list[dict[str, Any]]
                'fetch_row': dict[str, Any] | None
                'fetch_val': Any | None
        """
        if not self.is_connected:
            await self._create_pool()

        logger.debug(f"Executing SQL ({mode}).")
        try:
            async with self._pool_acquire() as conn:
                async with conn.transaction():
                    match mode:
                        case "execute":
                            status = await conn.execute(query, *params)
                            return int(status.rsplit(" ", 1)[-1]) if status else 0
                        case "fetch_all":
                            records = await conn.fetch(query, *params)
                            return self._records_to_list_records(records)
                        case "fetch_row":
                            record = await conn.fetchrow(query, *params)
                            return self._record_to_dict(record)
                        case "fetch_val":
                            return await conn.fetchval(query, *params)
                        case _:
                            raise ValueError(f"Invalid SQL query mode: {mode}.")
        except error_database.UniqueViolationError as e:
            raise DuplicateUserError("Participant with this user_id already exists in this config.") from e
        except error_database.PostgresError as e:
            raise DatabaseQueryError(
                f"Database query error: {e.__class__.__name__} - {e}"
            ) from e
        except Exception as e:
            raise DatabaseConnectionError(
                f"Unexpected error with connection to db: {type(e).__name__}: {e}"
            ) from e

    async def upsert_bot_config(
            self,
            chat_id: int,
            admin_id: int,
            timezone: str,
            llm_client_name: str = None,
    ) -> int:
        """Creates or updates bot configuration for a specific chat."""
        config_id = await self._execute(
            queries.UPSERT_BOT_CONFIG,
            params=(chat_id, admin_id, timezone, llm_client_name),
            mode="fetch_val",
        )
        logger.info(f"Configuration for chat {chat_id} saved/updated successfully.")
        return config_id

    async def get_bot_config(self, chat_id: int) -> dict | None:
        """Retrieves active bot configuration by chat_id."""
        logger.debug(f"Fetching configuration for chat {chat_id}...")
        config_dict = await self._execute(
            queries.GET_BOT_CONFIG, params=(chat_id,), mode="fetch_row"
        )
        if config_dict:
            logger.debug(f"Configuration for chat {chat_id} found.")
            return config_dict
        else:
            logger.debug(f"No active configuration found for chat {chat_id}.")
            return None

    async def get_bot_config_by_id(self, config_id: int) -> dict | None:
        """Fetches all bot information by id from the database."""
        logger.debug(f"Fetching configuration for chat {config_id}...")
        config_dict = await self._execute(
            queries.GET_BOT_CONFIG_BY_ID, params=(config_id,), mode="fetch_row"
        )
        if config_dict:
            logger.debug(f"Configuration for chat {config_id} found.")
            return config_dict
        else:
            logger.debug(f"No active configuration found for chat {config_id}.")
            return None

    async def delete_bot_config(self, chat_id: int) -> int:
        """Deletes chat configuration and returns number of deleted rows."""
        logger.debug(f"Deleting configuration for chat {chat_id}...")
        deleted_count = await self._execute(
            queries.DELETE_BOT_CONFIG, params=(chat_id,)
        )
        if deleted_count > 0:
            logger.debug("Configuration for chat {chat_id} deleted successfully.")
        else:
            logger.warning(
                f"Attempted to delete non-existent configuration for chat {chat_id}."
            )
        return deleted_count

    async def delete_bot_config_by_id(self, config_id: int) -> int:
        """
        Deletes all bot configurations and cascades to participants and long-term memory.
        Returns the number of bot_configs deleted.
        """
        logger.warning(f"Deleting ALL bot_data {config_id} from the database. This operation is irreversible!")
        deleted_count = await self._execute(
            queries.DELETE_BOT_CONFIG_BY_ID,
            params=(config_id,)
        )
        if deleted_count > 0:
            logger.debug("Configuration for chat {chat_id} deleted successfully.")
        else:
            logger.warning(
                f"Attempted to delete non-existent configuration for config {config_id}."
            )
        return deleted_count

    async def insert_participant(
            self,
            config_id: int,
            user_id: int,
            custom_name: str,
            gender: str,
            relationship_score: int
    ) -> dict[str, Any]:
        """Adds a user to bot memory and returns a dict with unique ID and custom_name."""
        participant = await self._execute(
            queries.INSERT_PARTICIPANT,
            params=(config_id, user_id, custom_name, gender, relationship_score),
            mode="fetch_row",
        )
        logger.debug(
            f"Participant {user_id} added to bot with ID {config_id}. DB ID: {participant["id"]} /"
            f"name: {participant["custom_name"]}."
        )
        return participant

    async def get_participant(self, config_id: int, user_id: int) -> dict | str:
        """Retrieves full information about a participant by Telegram ID."""
        participant = await self._execute(
            queries.GET_PARTICIPANT, params=(config_id, user_id), mode="fetch_row"
        )
        if participant:
            return participant
        else:
            return "<UnknownUser>"

    async def get_all_participants_with_memories(self, config_id: int) -> list[dict]:
        """Retrieves all active participants for a config, embedding their
        latest memories directly into each participant's record."""
        return await self._execute(
            queries.GET_PARTICIPANTS_WITH_MEMORIES,
            params=(config_id,),
            mode="fetch_all",
        )

    async def update_personality_prompt(self, prompt: str, config_id: int):
        await self._execute(
            queries.UPDATE_PERSONALITY_PROMPT,
            params=(prompt, config_id)
        )

    async def set_ignore_status(self, participant_id: int, status: bool) -> None:
        """Sets the is_ignored flag for a participant and resets relationship_score to 0."""
        await self._execute(
            queries.SET_IGNORED_STATUS, params=(status, participant_id)
        )

    async def update_relationship_score(
            self, participant_id: int, score_change: int
    ) -> None:
        """Updates participant reputation only."""
        await self._execute(
            queries.UPDATE_RELATIONSHIP_SCORE,
            params=(score_change, participant_id)
        )

    async def add_long_term_memory(
            self, participant_id: int, memory_summary: str
    ) -> None:
        """Records a participant's memory and keeps only the latest 10 entries."""
        await self._execute(
            INSERT_LONG_TERM_MEMORY,
            params=(participant_id, memory_summary)
        )

    @staticmethod
    def _record_to_dict(record: asyncpg.Record | None) -> dict[str, Any] | None:
        return dict(record) if record else None

    @staticmethod
    def _records_to_list_records(records: list[asyncpg.Record]) -> list[dict[str, Any]]:
        return [dict(record) for record in records]
