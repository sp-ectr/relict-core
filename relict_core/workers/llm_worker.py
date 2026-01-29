import logging

from relict_core.config.schemas import ClientsLLM
from relict_core.databases.redis_client import RedisClient
from relict_core.drivers.stream_driver import StreamDriver
from relict_core.config.logging_config import log_error
from relict_core.config.events import LLMRequestStart, LLMRequestPulse, CommandDayEnd

logger = logging.getLogger(__name__)


class LLMWorker:
    def __init__(
            self,
            clients: ClientsLLM,
            redis: RedisClient,
            worker_name: str,
            consume_stream: str,
            produce_stream: str
    ):
        self.redis = redis
        self.worker_name = worker_name
        self.consume_stream = consume_stream
        self.produce_stream = produce_stream
        self.stream = StreamDriver(
            redis=redis,
            consume_stream=self.consume_stream,
            group_name="llm_workers",
            consumer_name=self.worker_name
        )
        self.is_running = False
        logger.info(f"{self.worker_name} is initialized.")

    @log_error
    async def run(self):
        """The main event loop for the worker."""
        self.is_running = True
        logger.info(f"{self.worker_name}. Listening for {self.consume_stream}...")

        await self.stream.ensure_group()

        while self.is_running:
            try:
                result = await self.stream.next_event()

                if not result:
                    continue

                msg_id, event = result

                match event:
                    case LLMRequestStart
