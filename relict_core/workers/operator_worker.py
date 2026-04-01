"""
Operator worker.

Contains the OperatorWorker class, responsible for preprocessing incoming messages,
caching config and participant data, applying rate limits, and routing
messages into Redis queues based on priority.
"""
import logging

from relict_core.config.exceptions import OperatorError
from relict_core.config.logging_config import log_error

from relict_core.databases.redis_client import RedisClient
from relict_core.databases.postgre_client import AsyncPostgreManager
from relict_core.config.relict_settings import PostgreSettings, RedisSettings
from relict_core.config.schemas import Participant, WorkerIdentity, StreamContext, RedisKey, RedisData
from relict_core.config.events import RawMessage, Message

logger = logging.getLogger(__name__)


class OperatorWorker:
    def __init__(
            self,
            postgre_opts: PostgreSettings,
            redis_opts: RedisSettings,
            worker_opts: WorkerIdentity
    ):
        self.postgre_opts = postgre_opts
        self.redis_opts = redis_opts
        self.worker_opts = worker_opts

        self.db: AsyncPostgreManager | None = None
        self.redis: RedisClient | None = None
        self.main_stream: StreamContext | None = None
        self.produce_stream: StreamContext | None = None

        self.is_running = False
        logger.info(f"{self.worker_opts.consumer_name} is initialized.")

    @log_error
    async def run(self):
        """The main event loop for the worker."""
        self.is_running = True
        self.main_stream = StreamContext.raw_messages(self.worker_opts.consumer_name)
        self.db = AsyncPostgreManager(self.postgre_opts)
        self.redis = RedisClient(self.redis_opts)

        async with self.db.lifecycle(), self.redis.lifecycle():
            logger.info(f"{self.worker_opts.consumer_name} connected to PostgreSQL and Redis")
            await self.redis.stream_create_group(self.main_stream, mk_stream=True)
            logger.info(f"{self.worker_opts.consumer_name} is started. Listening for {self.main_stream.stream}...")

            while self.is_running:
                try:
                    result = await self.redis.stream_read_data(self.main_stream, count=10, block_ms=200)

                    if not result:
                        continue

                    for data in result:
                        try:
                            if data.error:
                                continue
                            raw_event_type = data.payload.get("event_type")
                            match raw_event_type:
                                case "RawMessage":
                                    event = RawMessage.model_validate(data.payload)
                                    await self._handle_message(event)
                                case _:
                                    logger.warning(f"Unexpected event type: {raw_event_type}")
                        finally:
                            await self.redis.stream_ack(self.main_stream, data.data_id)
                except Exception as e:
                    raise OperatorError(f"Critical error in OperatorWorker loop: {e}") from e
        logger.info(f"{self.worker_opts.consumer_name} gracefully stopped")

    async def _handle_message(self, event: RawMessage):
        """
        Processes an incoming raw message:
        - applies per-user rate limiting
        - loads and caches chat configuration
        - checks participant ignored status
        - resets silence state on valid activity
        """
        config_key = RedisData.bot_config(event.chat_id)
        rate_limit_key = RedisKey.rate_limit(event.user_id)
        if not (bot_config := await self.redis.get_data(config_key)):
            bot_config = await self.db.get_bot_config(event.chat_id)
            if not bot_config:
                logger.info(f"No bot config found for chat {event.chat_id}, dropping")
                return
            await self.redis.set_data(config_key, bot_config)
        silent_key = RedisKey.silence_lock(bot_config.id)
        silent_counter_key = RedisKey.silence_counter(bot_config.id)

        current_count = await self.redis.increment_counter(rate_limit_key, ttl=5)
        if current_count > 10:
            logger.warning(
                f"Rate limit burst exceeded | user={event.user_id} chat={bot_config.id} "
                f"count={current_count}/10 in 5s → dropped"
            )
            return

        participant_key = RedisData.participant_config(bot_config.id, event.user_id)
        if not (participant := await self.redis.get_data(participant_key)):
            if not (participant := await self.db.get_participant(bot_config.id, event.user_id)):
                participant_id = await self.db.insert_participant(
                    Participant(
                        config_id=bot_config.id,
                        user_id=event.user_id,
                        user_name=event.user_name
                    )
                )
                logger.debug(f"SUCCESS insert Participant user_id={participant_id} intro db.")
                participant = await self.db.get_participant(bot_config.id, event.user_id)
            await self.redis.set_data(participant_key, participant)

        if participant and participant.is_ignored:
            logger.debug(f"Ignored user {event.user_id} in chat {bot_config.id}")
            return

        await self.redis.delete_many([silent_key, silent_counter_key])

        new_event = Message(
            user_id=event.user_id,
            user_name=event.user_name,
            text=event.text,
            trace_id=event.trace_id
        )

        produce_stream = StreamContext.message_stream(bot_config.id, self.worker_opts.consumer_name)

        try:
            event_id = await self.redis.stream_add(new_event, produce_stream)
            logger.debug(f"Produced message {event_id} to {produce_stream.stream} (trace: {event.trace_id})")
        except Exception as e:
            logger.error(
            f"Failed to produce message to {produce_stream.stream} | trace={event.trace_id}: {e}",
                exc_info=True
            )


def stop(self):
    """Gracefully stop the worker loop."""
    if self.is_running:
        logger.info(f"Stopping {self.worker_opts.consumer_name}...")
        self.is_running = False
    else:
        logger.debug(f"{self.worker_opts.consumer_name} already stopped or not started")
