"""
BrainWorker

The main class that centralizes incoming signals.
It listens to the session_stream and produces the brain_stream.
"""
import logging

from relict_core.config.llm_interface import BaseLLMClient
from relict_core.config.relict_settings import PostgreSettings, RedisSettings, LLMSettings
from relict_core.databases.redis_client import RedisClient
from relict_core.databases.postgre_client import AsyncPostgreManager
from relict_core.config.logging_config import log_error
from relict_core.config.events import Message, EventClean, CommandPulse, CommandDayEnd
from relict_core.config.schemas import PersonalityManifest, Participant, WorkerIdentety, StreamContext, RedisKey, LLMRequest, LLMResponse
from relict_core.config.exceptions import BrainError, StreamError
from relict_core.drivers.gemeni_client import GeminiClient

logger = logging.getLogger(__name__)


class BrainWorker:
    def __init__(
            self,
            postgre_opts: PostgreSettings,
            redis_opts: RedisSettings,
            worker_opts: WorkerIdentety,
            persona: PersonalityManifest,
            llm_opts: LLMSettings
    ):
        self.postgre_opts = postgre_opts
        self.redis_opts = redis_opts
        self.worker_opts = worker_opts
        self.persona = persona
        self.llm_opts = llm_opts

        self.db: AsyncPostgreManager | None = None
        self.redis: RedisClient | None = None
        self.llm: BaseLLMClient | None = None
        self.main_stream: StreamContext | None = None
        self.message_stream: StreamContext | None = None
        self.produce_stream: StreamContext | None = None

        self.is_running = False
        logger.info(f"{self.worker_opts.consumer_name} is initialized.")

    @log_error
    async def run(self):
        """The main event loop for the worker."""
        self.is_running = True
        self.main_stream = StreamContext(
            stream=f"session_stream:shard_{self.worker_opts.index}",
            group="brain_workers",
            consumer=self.worker_opts.consumer_name
        )

        self.db = AsyncPostgreManager(self.postgre_opts)
        self.redis = RedisClient(self.redis_opts)
        self.llm = GeminiClient(self.llm_opts)

        async with self.db.lifecycle(), self.redis.lifecycle():
            logger.info(f"{self.worker_opts.consumer_name} connected to PostgreSQL and Redis")
            await self.redis.stream_create_group(self.main_stream)
            logger.info(f"{self.worker_opts.consumer_name} is started. Listening for {self.main_stream.stream}...")
            while self.is_running:
                try:
                    result = await self.redis.stream_read_data(self.main_stream, block_ms=100)

                    if not result:
                        continue

                    for data in result:
                        if data.error:
                            await self.redis.stream_ack(self.main_stream, data.data_id)
                            raise BrainError(f"Error while processing a key event for the system. {data.data_id}")

                        raw_event_type = data.payload.get("event_type")
                        match raw_event_type:
                            case "CommandPulse":
                                event = CommandPulse.model_validate(data.payload)
                                if event.config_id in self.llm.sessions:
                                    await self._handle_slot_start(event)
                                else:
                                    await self._handle_pulse(event)
                            case "CommandDayEnd":
                                event = CommandDayEnd.model_validate(data.payload)
                                await self._handle_day_end(event)
                            case "EventClean":
                                event = EventClean.model_validate(data.payload)
                                await self._handle_clean(event)
                            case _:
                                logger.warning(f"Unexpected event type: {raw_event_type}")
                                await self.redis.stream_ack(self.main_stream, data.data_id)
                except StreamError as e:
                    raise BrainError(f"Stream logic failed: {e}")
                except Exception as e:
                    raise BrainError(f"Critical error in SchedulerWorker loop: {e}")

    async def _handle_slot_start(self, event: CommandPulse):
        """
        Handle the start of the day by gathering participant context and dispatching an LLM request.
        """
        bot_config = await self.db.get_bot_config_by_id(command.config_id)

        try:
            bot_config = await self.db.get_bot_config_by_id(command.config_id)
            silent_key = RedisKey(key=f"silence_lock:{bot_config.chat_id}")
            if await self.redis.has_key(silent_key):
                logger.warning(f"There is a block in chat {bot_config.config_id}, skipping event {command.trace_id}.")
                return
            logger.debug(
                f"Starting the beginning of the day for config_id={config_id}. Gathering context..."
            )

            participants: dict[int, Participant] = {}

            participants_rows = await self.db.get_all_participants_with_memories(config_id)
            for row in participants_rows or []:
                user_id = row["user_id"]

                participant_data = {
                    "custom_name": row["custom_name"],
                    "gender": row["gender"],
                    "relationship_score": row["relationship_score"],
                    "memories": row["memories"] or None,
                }
                participant = Participant.model_validate(participant_data)
                participants[user_id] = participant

            llm_request_start = {
                "trace_id": trace_id,
                "config_id": config_id,
                "system_prompt": self.personality,
                "participants_info": participants,
            }
            event = LLMRequestStart.model_validate(llm_request_start)
            await self.stream.dispatch_event(event, self.produce_stream)

            logger.debug(
                f"Successfully built LLMRequest({trace_id}) for {self.produce_stream} "
                f"with config_id={config_id} and {len(participants)} participants"
            )

        except Exception as e:
            raise BrainError(
                f"Error in _handle_day_start for config_id={config_id}, trace_id={trace_id}. Cause: {e}."
            ) from e

    async def _handle_pulse(self, event: CommandPulse):





def stop(self):
    self.is_running = False
