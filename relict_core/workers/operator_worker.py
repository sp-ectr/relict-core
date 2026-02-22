"""
Operator worker.

Contains the OperatorWorker class, responsible for preprocessing incoming messages,
caching config and participant data, applying rate limits, and routing
messages into Redis queues based on priority.
"""
import logging
from relict_core.config.logging_config import log_error

from relict_core.databases.redis_client import RedisClient
from relict_core.databases.postgre_client import AsyncPostgreManager
from relict_core.config.relict_settings import PostgreSettings, RedisSettings
from relict_core.config.schemas import WorkerIdentety, StreamContext, RedisKey
from relict_core.config.events import RawMessage, Message

logger = logging.getLogger(__name__)


class OperatorWorker:
    def __init__(
            self,
            postgre_opts: PostgreSettings,
            redis_opts: RedisSettings,
            worker_opts: WorkerIdentety
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
        self.main_stream = StreamContext(
            stream="raw_messages",
            group="operators",
            consumer=self.worker_opts.consumer_name,
        )
        self.db = AsyncPostgreManager(self.postgre_opts)
        self.redis = RedisClient(self.redis_opts)
        logger.info(f"{self.worker_opts.consumer_name} is started. Listening for {self.main_stream.stream}...")

        await self.redis.stream_create_group(self.main_stream)

        while self.is_running:
            try:
                result = await self.redis.stream_read_data(self.main_stream, 10, 200)

                if not result:
                    continue

                for data in result:
                    if data.error:
                        await self.redis.stream_ack(self.main_stream, data.data_id)
                        continue
                    raw_event = data.payload.get("event_type")
                    match raw_event:
                        case "RawMessage":
                            event = RawMessage.model_validate(data.payload)
                            await self._handle_event(event)
                            await self.redis.stream_ack(self.main_stream, data.message_id)

    async def _handle_event(self, event):
        key = RedisKey(key=f"bot_config:{event.chat_id}")
        silent_key = RedisKey(key=f"silence_lock:{event.chat_id}")
        silent_counter_key = RedisKey(key=f"silence_counter:{event.chat_id}")
        if not (bot_config := await self.redis.get_data(key)):
            if not (bot_config := await self.db.get_bot_config(event.chat_id)):
                return
            else:
                await self.redis.delete_many([silent_key, silent_counter_key])
                await self.redis.set_data(key, bot_config)

        config_id = bot_config["id"]



