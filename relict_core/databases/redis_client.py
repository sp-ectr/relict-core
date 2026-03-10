"""
Async Redis client focused on Streams.
Handles events, state, and counters with safe JSON serialization.
Designed as a data backbone for distributed systems.
"""
import json
import logging
from contextlib import asynccontextmanager

from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import ResponseError

from relict_core.config.events import BaseEvent
from relict_core.config.exceptions import RedisConnectionError, StreamError, RedisError
from relict_core.config.relict_settings import RedisSettings
from relict_core.config.schemas import T, RedisKey, RedisData, StreamContext, RawStreamData, BotConfig

logger = logging.getLogger(__name__)


class RedisClient:
    """
    Asynchronous Redis client.

    Supports TTL-based key-value storage, counters, flags,
    and Redis Streams with automatic trimming and expiration.
    Used as a foundation for event-driven and distributed systems.

    Parameters
    ----------
    opts : RedisSettings
        Pydantic settings object containing Redis configuration.
    """

    def __init__(self, opts: RedisSettings):
        self.opts = opts
        self._default_ttl = 30 * 60
        self._pool = ConnectionPool(
            host=self.opts.redis_host,
            port=self.opts.redis_port,
            db=0,
            decode_responses=True
        )
        self._client: Redis | None = None

    async def _connect(self):
        """Connect to Redis and verify connection."""
        try:
            self._client = Redis(connection_pool=self._pool)
            await self._client.ping()
            logger.info("Successfully connected to Redis.")
        except Exception as e:
            raise RedisConnectionError(f"Failed to connect to Redis: {e}") from e

    async def disconnect(self):
        """Close Redis connection and release resources."""
        if self._client:
            await self._client.close()
            await self._pool.disconnect()
            logger.info("Redis connection closed.")

    @asynccontextmanager
    async def lifecycle(self):
        """Context manager for safe connection handling."""
        await self._connect()
        try:
            yield self
        finally:
            await self.disconnect()

    # -------------------- Key-value / JSON / Flags -------------------- #
    async def set_key(self, key: RedisKey, ttl: int | None = None):
        ttl_to_use = self._default_ttl if ttl is None else None if ttl == 0 else ttl

        try:
            await self._client.set(key.key, "", ttl_to_use)
        except Exception as e:
            raise RedisError(f"Failed to store RedisData for key {key.key}: {e}")

    async def has_key(self, key: RedisKey) -> bool:
        return bool(await self._client.exists(key.key))

    async def set_data(self, key: RedisData[T], payload: T, ttl: int | None = None):
        ttl_to_use = self._default_ttl if ttl is None else None if ttl == 0 else ttl
        try:
            await self._client.set(key.key, payload.model_dump_json(), ttl_to_use)
        except Exception as e:
            raise RedisError(f"Failed to store data for key {key.key}: {e}")

    async def get_data(self, key: RedisData[T]) -> T | None:
        raw: str | None = await self._client.get(key.key)
        if raw is None:
            return None
        try:
            return key.model.model_validate_json(raw)
        except Exception as e:
            logger.critical(f"Failed to load RedisData for key {key}: {e}")
            return None

    async def increment_counter(self, key: RedisKey, ttl: int | None = None) -> int:
        """Increment a numeric counter with optional TTL. Returns new value."""
        ttl_to_use = self._default_ttl if ttl is None else None if ttl == 0 else ttl
        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.incr(key.key)
            await pipe.expire(key.key, ttl_to_use, nx=True)
            results = await pipe.execute()
        return results[0]

    async def delete(self, key: RedisKey) -> None:
        """Delete a single key."""
        await self._client.delete(key.key)

    async def delete_many(self, keys: list[RedisKey]):
        """Delete multiple keys in a pipeline."""
        async with self._client.pipeline(transaction=False) as pipe:
            for key in keys:
                await pipe.delete(key.key)
            await pipe.execute()

    # -------------------- Stream methods -------------------- #
    async def stream_add(self, data: BaseEvent, opts: StreamContext) -> str:
        """
        Add an event to a Redis stream and optional TTL on stream key.
        """
        try:
            event = {"payload": data.model_dump_json()}
            message_id = await self._client.xadd(
                opts.stream,
                event,
                maxlen=100,
                approximate=True
            )
            return message_id
        except Exception as e:
            raise StreamError(f"Unexpected error occurred while working with the Redis stream: {e}")

    async def stream_create_group(self, opts: StreamContext):
        """Create consumer group for a stream (idempotent)."""
        try:
            await self._client.xgroup_create(opts.stream, opts.group, id="0", mkstream=True)
            logger.debug(
                f"Consumer group '{opts.group}' created for stream '{opts.stream}'.")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{opts.group}' already exists.")
            else:
                raise RedisConnectionError(f"Failed to create group '{opts.group}': {e}") from e

    async def stream_read_data(
            self,
            opts: StreamContext,
            count: int = 1,
            block_ms: int = 0
    ) -> list[RawStreamData]:

        response = await self._client.xreadgroup(
            opts.group,
            opts.consumer,
            {opts.stream: ">"},
            count=count,
            block=block_ms
        )

        if not response:
            return []

        result = []
        for _, messages in response:
            for data_id, data in messages:
                try:
                    result.append(RawStreamData(data_id=data_id, payload=json.loads(data["payload"])))
                except Exception as e:
                    logger.warning(
                        f"Malformed message {data_id} in stream '{opts.stream}': {e}"
                    )
                    result.append(RawStreamData(data_id=data_id, error=True))
                    continue
        return result

    async def stream_ack(self, opts: StreamContext, data_id: str):
        try:
            await self._client.xack(opts.stream, opts.group, data_id)
        except Exception as e:
            raise StreamError(f"Unexpected error occurred while acknowledging Redis stream: {e}")
