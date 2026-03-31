"""
Consumes session events, processes messages, interacts with LLM,
and publishes responses to the brain stream.

Handles:
- CommandPulse → LLM request/response
- EventClean → state cleanup
- Silence detection via Redis

Uses:
Redis (streams, locks), PostgreSQL (participants), LLM client.
"""
import logging

from pydantic import ValidationError

from relict_core.config.llm_interface import BaseLLMClient
from relict_core.config.relict_settings import PostgreSettings, RedisSettings, LLMSettings
from relict_core.databases.redis_client import RedisClient
from relict_core.databases.postgre_client import AsyncPostgreManager
from relict_core.config.logging_config import log_error
from relict_core.config.events import Message, EventClean, CommandPulse, Response
from relict_core.config.schemas import PersonalityManifest, WorkerIdentity, StreamContext, RedisKey, \
    LLMRequest, ParticipantInfo, RedisData, UserMessages
from relict_core.config.exceptions import BrainError, StreamError
from relict_core.drivers.gemeni_client import GeminiClient

logger = logging.getLogger(__name__)


class BrainWorker:
    """
    Consumes session events from Redis stream, drives LLM sessions,
    and publishes responses to the brain stream.

    Attributes:
        postgre_opts: PostgreSQL connection settings.
        redis_opts: Redis connection settings.
        worker_opts: Worker identity and consumer name.
        persona: Personality manifest passed to LLM on session start.
        llm_opts: LLM client configuration.
        db: PostgreSQL manager instance. Initialized on run().
        redis: Redis client instance. Initialized on run().
        llm: LLM client instance. Initialized on run().
        main_stream: Input stream context for this worker.
        produce_stream: Output stream context for responses.
        is_running: Worker loop control flag.
    """
    def __init__(
            self,
            postgre_opts: PostgreSettings,
            redis_opts: RedisSettings,
            worker_opts: WorkerIdentity,
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
        self.produce_stream: StreamContext | None = None

        self.is_running = False
        logger.info(f"{self.worker_opts.consumer_name} is initialized.")

    @log_error
    async def run(self):
        """The main event loop for the worker."""
        self.is_running = True
        self.main_stream = StreamContext.session_stream(self.worker_opts.index, self.worker_opts.consumer_name)
        self.produce_stream = StreamContext.brain_stream(self.worker_opts.consumer_name)

        self.db = AsyncPostgreManager(self.postgre_opts)
        self.redis = RedisClient(self.redis_opts)
        self.llm = GeminiClient(self.llm_opts)

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
                                raise BrainError(f"Error while processing a key event for the system. {data.data_id}")

                            config_id, trace_id = data.payload.get("config_id"), data.payload.get("trace_id")
                            if await self._check_silence_block(config_id, trace_id):
                                continue

                            raw_event_type = data.payload.get("event_type")
                            match raw_event_type:
                                case "CommandPulse":
                                    event = CommandPulse.model_validate(data.payload)
                                    await self._handle_pulse(event)
                                case "EventClean":
                                    event = EventClean.model_validate(data.payload)
                                    await self._handle_clean(event)
                                case _:
                                    logger.warning(f"Unexpected event type: {raw_event_type}")
                        finally:
                            await self.redis.stream_ack(self.main_stream, data.data_id)

                except StreamError as e:
                    raise BrainError(f"Stream logic failed: {e}")
                except Exception as e:

                    raise BrainError(f"Critical error in BrainWorker loop: {e}")

    async def _check_silence_block(self, config_id: int, trace_id: str) -> bool:
        """
        Checks if config_id is blocked by silence lock.

        Returns True → skip processing.
        """
        try:
            silent_key = RedisKey.silence_lock(config_id)
            is_blocked = await self.redis.has_key(silent_key)
            if is_blocked:
                logger.warning(f"Chat {config_id} is blocked, skipping event {trace_id}")
            return is_blocked
        except Exception as e:
            raise BrainError(
                f"Error in _check_silence_block for config_id={config_id}, trace_id={trace_id}. Cause: {e}."
            ) from e

    async def _handle_pulse(self, event: CommandPulse):
        """
        Processes CommandPulse:
        - collects messages
        - builds LLM request
        - starts/reuses session
        - sends to LLM
        - publishes response

        Ends session if slot is finished.
        """
        messages = await self._handle_messages(event.config_id)

        if await self._check_silence_block(event.config_id, event.trace_id):
            return

        llm_request = LLMRequest(
            timestamp=event.timestamp,
            label=event.label,
            participants_info={},
            messages=messages,
            is_first_of_slot=event.is_first_of_slot,
            is_last_of_slot=event.is_last_of_slot
        )

        try:
            if event.config_id in self.llm.sessions:
                llm_response = await self.llm.send_in_session(event.config_id, llm_request)
            else:
                participant_list = await self.db.get_all_participants_with_memories(event.config_id)

                llm_request.participants_info = {
                    p.user_id: ParticipantInfo(
                        user_id=p.user_id,
                        custom_name=p.custom_name,
                        relationship_score=p.relationship_score,
                        memories=p.memories
                    )
                    for p in (participant_list or [])
                }

                llm_response = await self.llm.start_session(event.config_id, self.persona, llm_request)
                logger.debug(f"--- BRAIN OUTPUT (config_id={event.config_id}) ---\n"
                             f"{llm_response.model_dump_json(indent=2)}\n"
                             "-----------------")

            await self.redis.stream_add(
                Response(config_id=event.config_id, content=llm_response, trace_id=event.trace_id),
                self.produce_stream
                )

        except Exception as e:
            logger.error(
                f"CRITICAL ERROR in _handle_pulse for config_id={event.config_id}",
                exc_info=True
            )
            raise BrainError(f"Failed _handle_pulse for config_id={event.config_id}: {e}") from e
        finally:
            if event.is_last_of_slot:
                await self.llm.end_session(event.config_id)

    async def _handle_clean(self, event: EventClean):
        """
        Cleans all state for config_id:
        - stops LLM session
        - deletes Redis keys and message stream
        - forwards clean event downstream
        """
        logger.info(f"Cleaning up resources for config_id={event.config_id}")

        await self.llm.end_session(event.config_id)

        keys_to_delete = [
            RedisKey.silence_lock(event.config_id),
            RedisKey.silence_counter(event.config_id),
            RedisKey.messages_stream(event.config_id),
            RedisData.bot_config(event.config_id)
        ]

        await self.redis.delete_many(keys_to_delete)
        logger.debug(f"Redis keys and streams deleted for config_id={event.config_id}")

        await self.redis.stream_add(
            event,
            self.produce_stream
        )

    async def _handle_messages(self, config_id: int) -> list[UserMessages] | None:
        """
        Reads messages from Redis, validates them, and groups them into UserMessages objects.
        """
        buffer: dict[int, dict] = {}

        silence_counter_key = RedisKey.silence_counter(config_id)
        silence_lock_key = RedisKey.silence_lock(config_id)
        stream = StreamContext.message_stream(config_id, self.worker_opts.consumer_name)

        await self.redis.stream_create_group(stream, mk_stream=True)
        messages = await self.redis.stream_read_data(stream, count=30, block_ms=0)

        if not messages:
            if await self.redis.has_key(silence_counter_key):
                logger.warning(f"Stream empty twice, locking config {config_id}")
                await self.redis.set_key(silence_lock_key)
            else:
                await self.redis.set_key(silence_counter_key)
            return []

        for msg in messages:
            if msg.error:
                continue

            payload = msg.payload
            event_type = payload.get("event_type")

            if event_type != "Message":
                logger.error(f"INTEGRITY ERROR: Unexpected '{event_type}' in stream {config_id}")
                await self.redis.stream_ack(stream, msg.data_id)
                continue

            try:
                m = Message.model_validate(payload)

                if m.user_id not in buffer:
                    buffer[m.user_id] = {
                        "user_name": m.user_name,
                        "texts": []
                    }

                buffer[m.user_id]["texts"].append(m.text)

            except ValidationError as e:
                logger.warning(f"SCHEMA ERROR: Broken message in {msg.data_id}: {e.json()}")
            finally:
                await self.redis.stream_ack(stream, msg.data_id)

        return [
            UserMessages(
                user_id=uid,
                user_name=data["user_name"],
                texts=data["texts"]
            ) for uid, data in buffer.items()
        ]

    def stop(self):
        """Gracefully stop the worker loop."""
        if self.is_running:
            logger.info(f"Stopping {self.worker_opts.consumer_name}...")
            self.is_running = False
        else:
            logger.debug(f"{self.worker_opts.consumer_name} already stopped or not started")
