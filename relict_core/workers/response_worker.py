import logging

from relict_core.config.events import Response, EventClean
from relict_core.config.exceptions import AdapterError, StreamError
from relict_core.config.logging_config import log_error
from relict_core.config.relict_settings import PostgreSettings, RedisSettings, AdapterSettings
from relict_core.config.schemas import WorkerIdentity, StreamContext, Participant
from relict_core.databases.postgre_client import AsyncPostgreManager
from relict_core.databases.redis_client import RedisClient
from relict_core.drivers.telegram_adapter import TelegramAdapter

logger = logging.getLogger(__name__)


class ResponseWorker:
    def __init__(
            self,
            postgre_opts: PostgreSettings,
            redis_opts: RedisSettings,
            worker_opts: WorkerIdentity,
            bot_opts: AdapterSettings
    ):
        self.postgre_opts = postgre_opts
        self.redis_opts = redis_opts
        self.worker_opts = worker_opts
        self.bot_opts = bot_opts

        self.db: AsyncPostgreManager | None = None
        self.redis: RedisClient | None = None
        self.main_stream: StreamContext | None = None
        self.bot: TelegramAdapter | None = None

        self.is_running = False
        logger.info(f"{self.worker_opts.consumer_name} is initialized.")

    @log_error
    async def run(self):
        """The main event loop for the worker."""
        self.is_running = True
        self.main_stream = StreamContext.brain_stream(self.worker_opts.consumer_name)

        self.db = AsyncPostgreManager(self.postgre_opts)
        self.redis = RedisClient(self.redis_opts)
        self.bot = TelegramAdapter(self.bot_opts)

        async with self.db.lifecycle(), self.redis.lifecycle():
            logger.info(f"{self.worker_opts.consumer_name} connected to PostgreSQL and Redis")
            await self.redis.stream_create_group(self.main_stream, mk_stream=True)
            logger.info(f"{self.worker_opts.consumer_name} is started. Listening for {self.main_stream.stream}...")
            while self.is_running:
                try:
                    result = await self.redis.stream_read_data(self.main_stream, block_ms=100)
                    if not result:
                        continue
                    for data in result:
                        try:
                            if data.error:
                                raise AdapterError(f"Error while processing a key event for the system. {data.data_id}")
                            raw_event_type = data.payload.get("event_type")
                            match raw_event_type:
                                case "Response":
                                    event = Response.model_validate(data.payload)
                                    await self._handle_response(event)
                                case "EventClean":
                                    event = EventClean.model_validate(data.payload)
                                    await self._handle_clean(event)
                                case _:
                                    logger.warning(f"Unexpected event type: {raw_event_type}")
                        finally:
                            await self.redis.stream_ack(self.main_stream, data.data_id)
                except StreamError as e:
                    raise AdapterError(f"Stream logic failed: {e}")
                except Exception as e:
                    raise AdapterError(f"Critical error in ResponseWorker loop: {e}")

        await self.bot.close()
        logger.info(f"{self.worker_opts.consumer_name} gracefully stopped.")

    async def _handle_response(self, event: Response):
        try:
            logger.debug(f"[START] handle_response config_id={event.config_id}")

            config = await self.db.get_bot_config_by_id(event.config_id)
            if not config:
                logger.warning(f"[CONFIG] NOT FOUND config_id={event.config_id} -> skip")
                return
            logger.debug(f"[CONFIG] OK config_id={event.config_id} chat_id={config.chat_id}")

            content = event.content

            if content.text_reply:
                logger.debug(f"[TEXT] TRY SEND chat_id={config.chat_id}")
                await self.bot.send_typing(config.chat_id)
                await self.bot.send_message(config.chat_id, content.text_reply)
                logger.debug(f"[TEXT] SUCCESS chat_id={config.chat_id}")
            else:
                logger.debug(f"[TEXT] SKIP (empty) config_id={event.config_id}")


            if content.new_memories:
                for user_id, memory in content.new_memories.items():
                    logger.debug(f"[MEMORY] TRY ADD user_id={user_id}")
                    participant = await self.db.get_participant(event.config_id, user_id)

                    if participant:
                        await self.db.add_long_term_memory(participant.id, memory)
                        logger.debug(f"[MEMORY] SUCCESS user_id={user_id} participant_id={participant.id}")
                    else:
                        logger.debug(f"[MEMORY] SKIP user_id={user_id} (no participant)")
            else:
                logger.debug(f"[MEMORY] SKIP (no new_memories)")

            if content.respect_updates:
                for user_id, score_delta in content.respect_updates.items():
                    logger.debug(f"[RESPECT] TRY UPDATE user_id={user_id} delta={score_delta}")
                    participant = await self.db.get_participant(event.config_id, user_id)

                    if participant:
                        await self.db.update_relationship_score(participant, score_delta)
                        logger.debug(f"[RESPECT] SUCCESS user_id={user_id} new_delta={score_delta}")
                    else:
                        logger.debug(f"[RESPECT] SKIP user_id={user_id} (no participant)")
            else:
                logger.debug(f"[RESPECT] SKIP (no updates)")

            if content.set_block:
                for user_id in content.set_block:
                    logger.debug(f"[BLOCK] TRY SET user_id={user_id}")
                    participant = await self.db.get_participant(event.config_id, user_id)

                    if participant:
                        await self.db.set_ignore_status(participant.id, True)
                        logger.debug(f"[BLOCK] SUCCESS user_id={user_id}")
                    else:
                        logger.debug(f"[BLOCK] SKIP user_id={user_id} (no participant)")
            else:
                logger.debug(f"[BLOCK] SKIP (no users)")

            logger.debug(f"[END] handle_response config_id={event.config_id}")

        except Exception as e:
            logger.exception(f"[ERROR] handle_response config_id={event.config_id}: {e}")

    async def _handle_clean(self, event: EventClean) -> None:
        """
        Cleans up config from PostgreSQL by config_id.
        """
        logger.info(f"Received EventClean for config_id={event.config_id}, cleaning up...")
        deleted = await self.db.delete_bot_config_by_id(event.config_id)
        if deleted:
            logger.info(f"Config {event.config_id} deleted from PostgreSQL.")
        else:
            logger.warning(f"Config {event.config_id} not found in PostgreSQL, nothing to delete.")

    def stop(self) -> None:
        """Gracefully stop the worker loop."""
        if self.is_running:
            logger.info(f"Stopping {self.worker_opts.consumer_name}...")
            self.is_running = False
        else:
            logger.debug(f"{self.worker_opts.consumer_name} already stopped or not started")


