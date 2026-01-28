import logging

from relict_core.config.events import BaseEvent
from relict_core.config.exceptions import StreamError
from relict_core.databases.redis_client import RedisClient
from relict_core.config.event_registry import EVENT_MAPPING

logger = logging.getLogger(__name__)


class StreamDriver:
    """
    Driver for working with Redis Streams.
    Encapsulates reading, parsing, and acknowledgment.
    """

    def __init__(
            self,
            redis: RedisClient,
            group_name: str,
            consumer_name: str,
            consume_stream: str,
            error_threshold: int = 3
    ):
        self.redis = redis
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.consume_stream = consume_stream
        self.error_threshold = error_threshold
        self._malformed_streak = 0

    async def ensure_group(self):
        """Creates a consumer group. Idempotent."""
        await self.redis.stream_create_group(self.consume_stream, self.group_name)

    async def next_event(self) -> tuple[str, BaseEvent] | None:
        """
        Reads the next message from the stream.
        Returns a tuple (msg_id, EventObject) or None if the stream is empty.
        """
        response = await self.redis.stream_read_group(
            {self.consume_stream: ">"},
            self.group_name,
            self.consumer_name,
            count=1,
            block_ms=1000
        )

        if not response:
            return None

        stream, msg_id, data = response[0]

        event = self._deserialize(data, msg_id)

        if not event:
            self._malformed_streak += 1
            logger.warning(
                f"Malformed message detected (ID: {msg_id})."
                f"Streak: {self._malformed_streak}/{self.error_threshold}")
            if self._malformed_streak >= self.error_threshold:
                raise StreamError(f"Error streak limit ({self.error_threshold}) exceeded. "
                                  f"Possible system-wide issue.")

            await self.ack(msg_id)
            return None

        self._malformed_streak = 0
        return msg_id, event

    async def next_event_batch(
            self,
            batch_size: int = 50,
    ) -> list[tuple[str, BaseEvent]]:
        """
        Fetches a batch of messages from the Redis stream.

        Deserializes each message into a BaseEvent. Malformed messages are acked
        and counted; exceeding the error threshold raises StreamError.

        Args:
            batch_size (int): Max messages to fetch.

        Returns:
            list[tuple[str, BaseEvent]]: (msg_id, event) tuples, empty if none.
        """
        response = await self.redis.stream_read_group(
            {self.consume_stream: ">"}, self.group_name, self.consumer_name,
            count=batch_size, block_ms=200
        )
        if not response:
            return []

        events = []

        for stream, msg_id, data in response:
            event = self._deserialize(data, msg_id)

            if not event:
                self._malformed_streak += 1
                logger.warning(
                    f"Malformed message detected (ID: {msg_id}) "
                    f"Streak: {self._malformed_streak}/{self.error_threshold}"
                )
                if self._malformed_streak >= self.error_threshold:
                    raise StreamError(
                        f"Error streak limit ({self.error_threshold}) exceeded. Possible system-wide issue."
                    )

                await self.ack(msg_id)
                continue

            self._malformed_streak = 0
            events.append((msg_id, event))

        return events

    async def ack(self, msg_id: str):
        """Acknowledges successful processing."""
        await self.redis.stream_ack(self.consume_stream, self.group_name, msg_id)

    @staticmethod
    def _deserialize(data: dict, msg_id: str) -> BaseEvent | None:
        """Converts a raw dict into an event object via the registry."""
        event_type = data.get("event_type")
        event_cls: type[BaseEvent] | None = EVENT_MAPPING.get(event_type)

        if not event_cls:
            logger.warning(f"Unknown event type '{event_type}' in msg {msg_id}")
            return None
        try:
            return event_cls.model_validate(data)
        except Exception as e:
            logger.error(f"Validation error for {event_type} (msg {msg_id}): {e}")
            return None

    async def dispatch_event(self, event: BaseEvent, produce_stream: str, max_len: int | None = None):
        try:
            event_id = await self.redis.stream_add(produce_stream, event, max_len)
            logger.debug(
                f"Event {event.trace_id} ({event.event_type}) "
                f"added to stream '{produce_stream}' with ID {event_id}."
            )
        except Exception as e:
            raise StreamError(f"Failed to enqueue event for Event={event.trace_id} ({event.event_type}): {e}")
