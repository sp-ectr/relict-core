"""Async Redis client focused on Streams.
Handles events, state, and counters with safe JSON serialization.
Designed as the data backbone for distributed systems."""

import json
import logging
from contextlib import asynccontextmanager
from typing import Any

from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import ResponseError

from relict_core.config.events import BaseEvent
from relict_core.config.exceptions import RedisConnectionError, StreamError

logger = logging.getLogger(__name__)


class RedisClient:
    """
    Asynchronous Redis client with TTL for keys and auto-cleanup for streams.
    Supports JSON, flags, counters, and streams with MAXLEN/TTL.
    """

    def __init__(self, host: str, port: int, default_ttl: int | None = 3600 * 24):
        """
        Args:
            host: Redis host
            port: Redis port
            default_ttl: Default TTL in seconds for keys/streams (default: 1 day)
        """
        self._pool = ConnectionPool(host=host, port=port, db=0, decode_responses=True)
        self._client: Redis | None = None
        self.default_ttl = default_ttl

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
    async def set_json(self, key: str, data: Any, ttl_seconds: int | None = "default"):
        """Store JSON-serializable data with optional TTL."""
        ttl = self.default_ttl if ttl_seconds == "default" else ttl_seconds
        await self._client.set(key, json.dumps(data), ex=ttl)

    async def get_json(self, key: str) -> dict[str, Any] | None:
        """Retrieve and deserialize JSON data by key."""
        raw_data = await self._client.get(key)
        return json.loads(raw_data) if raw_data else None

    async def set_flag(self, key: str, ttl_seconds: int | None = "default" ):
        """
        Set a flag. If ttl_seconds is None, the flag is permanent.
        Otherwise, it expires after ttl_seconds.
        """
        ttl = self.default_ttl if ttl_seconds == "default" else ttl_seconds
        await self._client.set(key, "1", ex=ttl)

    async def has_flag(self, key: str) -> bool:
        """Check whether a flag exists."""
        return bool(await self._client.get(key))

    async def increment_counter(self, key: str, ttl_seconds: int | None = "default" ) -> int:
        """Increment a numeric counter with optional TTL. Returns new value."""
        ttl = self.default_ttl if ttl_seconds == "default" else ttl_seconds
        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.incr(key)
            if ttl is not None and ttl > 0:
                await pipe.expire(key, ttl, nx=True)
            results = await pipe.execute()
        return results[0]

    async def delete(self, key: str) -> None:
        """Delete a single key."""
        await self._client.delete(key)

    async def delete_many(self, keys: list[str]):
        """Delete multiple keys in a pipeline."""
        if not keys:
            return
        async with self._client.pipeline(transaction=False) as pipe:
            for key in keys:
                await pipe.delete(key)
            await pipe.execute()

    # -------------------- Stream methods -------------------- #
    async def stream_add(
            self,
            stream_name: str,
            event: BaseEvent,
            max_len: int | None = 1000,
            ttl_seconds: int | None = "default"
    ) -> str:
        """
        Add an event to a Redis stream with MAXLEN and optional TTL on stream key.
        """
        if not isinstance(event, BaseEvent):
            raise StreamError(f"Unexpected event received: {type(event)}")

        data = {"payload": event.model_dump_json()}
        message_id = await self._client.xadd(
            stream_name,
            data,
            maxlen=max_len,
            approximate=True if max_len else False
        )

        ttl = self.default_ttl if ttl_seconds == "default" else ttl_seconds
        if ttl is not None and ttl > 0:
            await self._client.expire(stream_name, ttl)
        return message_id

    async def stream_create_group(self, stream_name: str, group_name: str):
        """Create consumer group for a stream (idempotent)."""
        try:
            await self._client.xgroup_create(stream_name, group_name, id="0", mkstream=True)
            logger.debug(f"Consumer group '{group_name}' created for stream '{stream_name}'.")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{group_name}' already exists.")
            else:
                raise RedisConnectionError(f"Failed to create group '{group_name}': {e}") from e

    async def stream_read_group(
            self,
            streams_dict: dict[str, str],
            group_name: str,
            consumer_name: str,
            count: int = 1,
            block_ms: int = 0
    ) -> list[tuple[str, str, dict[str, Any]]] | []:
        response = await self._client.xreadgroup(
            group_name, consumer_name, streams_dict, count=count, block=block_ms
        )
        if not response:
            return []

        result = []
        for stream_name, messages in response:
            for message_id, data in messages:
                payload = json.loads(data["payload"])
                result.append((stream_name, message_id, payload))
        return result

    async def stream_ack(self, stream_name: str, group_name: str, message_id: str):
        await self._client.xack(stream_name, group_name, message_id)

    async def stream_trim(self, stream_name: str, max_len: int = 1000):
        """Trim stream to last `max_len` messages."""
        await self._client.xtrim(stream_name, maxlen=max_len, approximate=True)
