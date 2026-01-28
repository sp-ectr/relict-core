"""
BrainWorker

The main class that centralizes incoming signals.
It listens to the pulse_stream and produces the brain_stream.
"""
import logging

from relict_core.databases.redis_client import RedisClient
from relict_core.databases.postgre_client import AsyncPostgresManager
from relict_core.drivers.stream_driver import StreamDriver
from relict_core.config.logging_config import log_error
from relict_core.config.events import Message, CommandDayStart, CommandDayEnd, CommandClean, Pulse, \
    LLMRequestPulse, LLMRequestStart
from relict_core.config.schemas import PersonalityManifest, Participant
from relict_core.config.exceptions import BrainError, StreamError

logger = logging.getLogger(__name__)


class BrainWorker:
    def __init__(
            self,
            redis: RedisClient,
            data_base: AsyncPostgresManager,
            personality: PersonalityManifest,
            worker_name: str,
            consume_stream: str,
            produce_stream: str
    ):
        self.db = data_base
        self.worker_name = worker_name
        self.consume_stream = consume_stream
        self.produce_stream = produce_stream
        self.personality = personality
        self.redis = redis
        self.stream = StreamDriver(
            redis=redis,
            consume_stream=self.consume_stream,
            group_name="brain_workers",
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
                    case CommandDayStart(config_id=config_id):
                        await self._handle_day_start(config_id, event.trace_id)
                    case CommandDayEnd():
                        await self.stream.dispatch_event(event, self.produce_stream)
                    case CommandClean(config_id=config_id):
                        await self._handle_clean(config_id, event.trace_id)
                        await self.stream.dispatch_event(event, self.produce_stream)
                    case Pulse(config_id=config_id, is_first_of_day=is_first_of_day, is_last_of_day=is_last_of_day,
                               label=label):
                        await self._handle_pulse(config_id, event.trace_id, is_first_of_day, is_last_of_day, label)

                    case _:
                        logger.warning(f"Unexpected event in system_stream: {type(event).__name__}")
                await self.stream.ack(msg_id)
            except StreamError as e:
                raise BrainError(f"Stream logic failed: {e}")
            except Exception as e:
                raise BrainError(f"Critical error in SchedulerWorker loop: {e}")

    async def _handle_day_start(self, config_id: int, trace_id: str):
        """
        Handle the start of the day by gathering participant context and dispatching an LLM request.
        """
        silence_lock_key = f"silence_lock:{config_id}"
        try:
            if await self.redis.has_flag(silence_lock_key):
                logger.warning(f"There is a block in chat {config_id}, skipping event {trace_id}.")
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

    async def _handle_clean(self, config_id: int, trace_id: str):

        """
        Performs the final cleanup for a single configuration.
        """
        logger.info(f"Starting final cleanup trace_id{trace_id} for config_id={config_id} . Wiping data...")
        try:
            keys_to_delete = [
                f"messages_stream:{config_id}",
                f"config_exists:{config_id}",
                f"config_data:{config_id}",
                f"silence_counter:{config_id}",
                f"silence_lock:{config_id}"
            ]

            await self.redis.delete_many(keys_to_delete)
            await self.db.delete_bot_config_by_id(config_id)
            logger.info(
                f"Final cleanup completed trace_id {trace_id} for config_id={config_id}.\
                All related data removed.")
        except Exception as e:
            raise BrainError(
                f"Error in _handle_clean for config_id={config_id}, trace_id={trace_id}. Cause: {e}."
            ) from e

    async def _handle_pulse(self, config_id: int, trace_id: str, is_first_of_day: bool, is_last_of_day: bool,
                            label: str):
        """
        Handles a Pulse event:
        1. Creates a temporary consumer for the chat-specific message stream.
        2. Fetches a batch of messages.
        3. Gathers current participant context.
        4. Dispatches an LLMRequest with all the data.
        5. Acknowledges the processed messages.
        """
        silence_lock_key = f"silence_lock:{config_id}"
        silence_counter_key = f"silence_counter:{config_id}"
        try:
            if await self.redis.has_flag(silence_lock_key):
                logger.warning(f"There is a block in chat {config_id}, skipping Pulse {trace_id}.")
                return
            if await self.redis.has_flag(silence_counter_key):
                logger.warning(f"No messages from users, setting a block on chat {config_id}.")
                await self.redis.set_flag(silence_lock_key, None)

            message_driver = StreamDriver(
                redis=self.redis,
                consume_stream=f"messages_stream:{config_id}",
                group_name="brain_messages_workers",
                consumer_name=f"{self.worker_name}_pulse_{trace_id}"
            )

            messages: dict[int, str] = {}

            await message_driver.ensure_group()
            message_batch = await message_driver.next_event_batch()

            if not message_batch:
                logger.warning(
                    f"No messages in {config_id}, setting {silence_counter_key}. Pulse_{trace_id}"
                )
                await self.redis.set_flag(silence_counter_key, None)

            logger.debug(f"Processing messages for {config_id}, pulse_{trace_id}.")
            for message in message_batch or []:
                msg_id, event = message
                match event:
                    case Message(user_id=user_id, user_name=user_name, text=text):
                        messages[user_id] = f"{user_name}: {text}"
                        await message_driver.ack(msg_id)
                    case _:
                        logger.warning(
                            f"Unexpected event in system_stream: {type(event).__name__} trace_id:{event.trace_id}")
                        await message_driver.ack(msg_id)

            llm_request_pulse = {
                "config_id": config_id,
                "is_first_of_day": is_first_of_day,
                "is_last_of_day": is_last_of_day,
                "label": label,
                "messages": messages
            }
            event = LLMRequestPulse.model_validate(llm_request_pulse)
            await self.stream.dispatch_event(event, self.produce_stream)
        except Exception as e:
            raise BrainError(
                f"Error in _handle_pulse for config_id={config_id}, trace_id={trace_id}. Cause: {e}."
            ) from e

    def stop(self):
        self.is_running = False
