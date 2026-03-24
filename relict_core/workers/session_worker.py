import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler import AsyncScheduler, ConflictPolicy
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from relict_core.databases.postgre_client import AsyncPostgreManager
from relict_core.databases.redis_client import RedisClient
from relict_core.config.events import (EventStart,
                                       EventClean,
                                       CommandDayStart,
                                       CommandDayEnd,
                                       CommandPulse
                                       )
from relict_core.config.relict_settings import PostgreSettings, RedisSettings
from relict_core.config.logging_config import log_error
from relict_core.config.schemas import SchedulerSettings, WorkerIdentity, StreamContext
from relict_core.config.exceptions import SchedulerError, StreamError
from relict_core.drivers.pulse_planner import PulsePlanner

logger = logging.getLogger(__name__)


class SessionWorker:
    """
    Listens to system events to create or destroy the main daily cycle jobs for in APScheduler.
    """

    def __init__(
            self,
            postgre_opts: PostgreSettings,
            redis_opts: RedisSettings,
            worker_opts: WorkerIdentity,
            scheduler_opts: SchedulerSettings
    ):
        self.postgre_opts = postgre_opts
        self.redis_opts = redis_opts
        self.worker_opts = worker_opts
        self.scheduler_opts = scheduler_opts

        self.db: AsyncPostgreManager | None = None
        self.redis: RedisClient | None = None
        self.main_stream: StreamContext | None = None
        self.produce_stream: StreamContext | None = None
        self.scheduler: AsyncScheduler | None = None

        self.is_running = False
        logger.info(f"{self.worker_opts.consumer_name} is initialized.")

    @log_error
    async def run(self):
        """The main event loop for the worker."""
        self.is_running = True
        self.main_stream = StreamContext.system_stream(self.worker_opts.consumer_name)

        self.db = AsyncPostgreManager(self.postgre_opts)
        self.redis = RedisClient(self.redis_opts)
        self.scheduler = AsyncScheduler()

        async with self.scheduler, self.db.lifecycle(), self.redis.lifecycle():
            logger.info(f"{self.worker_opts.consumer_name} connected to PostgreSQL and Redis")
            await self.redis.stream_create_group(self.main_stream)
            logger.info(f"{self.worker_opts.consumer_name} is started. Listening for {self.main_stream.stream}...")
            while self.is_running:
                try:
                    result = await self.redis.stream_read_data(self.main_stream, block_ms=100)

                    if not result:
                        continue

                    try:
                        for data in result:
                            if data.error:
                                raise SchedulerError(
                                    f"Error while processing a key event for the system. {data.data_id}:{data.payload}")

                            raw_event_type = data.payload.get("event_type")
                            match raw_event_type:
                                case "EventStart":
                                    event = EventStart.model_validate(data.payload)
                                    await self._handle_start(event)
                                case "EventClean":
                                    event = EventClean.model_validate(data.payload)
                                    await self._handle_clean(event)
                                case _:
                                    logger.warning(f"Unexpected event type: {raw_event_type}")
                    finally:
                        await self.redis.stream_ack(self.main_stream, data.data_id)
                except StreamError as e:
                    raise SchedulerError(f"Stream logic failed: {e}")
                except Exception as e:
                    raise SchedulerError(f"Critical error in SchedulerWorker loop: {e}")

    async def _handle_start(self, event: EventStart):
        """
        Handler a new config: deletes any old jobs and creates new,
        perpetual DayStart/DayEnd Cron jobs.
        """
        logger.debug(f"Handling EventStart for config_id={event.config_id}. Setting up daily cycle.")

        try:
            bot_config = await self.db.get_bot_config_by_id(event.config_id)
            timezone = ZoneInfo(bot_config.timezone)
            await self._remove_jobs_for_config(event.config_id)

            start_command = CommandDayStart(config_id=event.config_id, trace_id=event.trace_id)
            await self.scheduler.add_schedule(
                self._handle_day_start,
                trigger=CronTrigger(hour=self.scheduler_opts.day_start_hour, minute=0, timezone=timezone),
                kwargs={
                    "command": start_command
                },
                id=f"day_start_{event.config_id}",
                conflict_policy=ConflictPolicy.replace
            )
            end_command = CommandDayEnd(config_id=event.config_id, trace_id=event.trace_id)
            await self.scheduler.add_schedule(
                self._handle_day_end,
                trigger=CronTrigger(hour=self.scheduler_opts.day_end_hour, minute=0, timezone=timezone),
                kwargs={
                    "command": end_command
                },
                id=f"day_end_{event.config_id}",
                conflict_policy=ConflictPolicy.replace
            )

            now_in_tz = datetime.now(timezone)

            day_end_dt = now_in_tz.replace(
                hour=self.scheduler_opts.day_end_hour,
                minute=0, second=0, microsecond=0
            )

            time_left = day_end_dt - now_in_tz
            min_duration = timedelta(minutes=self.scheduler_opts.min_session_duration_min)

            if time_left < min_duration:
                logger.info(f"Too late to start. Left: {time_left}, required: {min_duration}. Skipping.")
                return

            logger.info(f"Active hours: triggering initial DayStart for config_id={event.config_id}")
            await self._handle_day_start(start_command)

        except ZoneInfoNotFoundError:
            await self._remove_jobs_for_config(event.config_id)
            raise SchedulerError(
                f"Invalid timezone for config_id={event.config_id}.")
        except Exception as e:
            await self._remove_jobs_for_config(event.config_id)
            raise SchedulerError(f"Failed to schedule jobs for config {event.config_id}: {e}")

    async def _handle_day_start(self, command: CommandDayStart, now: datetime | None = None):
        logger.debug(f"Handling СommandDayStart for config_id={command.config_id}. Planning pulses...")
        try:
            bot_config = await self.db.get_bot_config_by_id(command.config_id)
            await self._remove_pulse_for_config(command.config_id)
            timezone = ZoneInfo(bot_config.timezone)
            pulse_planner = PulsePlanner(timezone, self.scheduler_opts, now=now)
            pulses = pulse_planner.plan_pulses_for_today()

            for pulse in pulses:
                pulse_command = CommandPulse(
                    config_id=command.config_id,
                    label=pulse.label,
                    timestamp=pulse.timestamp,
                    is_first_of_slot=pulse.is_first_of_slot,
                    is_last_of_slot=pulse.is_last_of_slot
                )
                await self.scheduler.add_schedule(
                    self.redis.stream_add,
                    trigger=DateTrigger(pulse.timestamp),
                    kwargs={
                        "data": pulse_command,
                        "opts": StreamContext.session_stream(bot_config.shard_id, self.worker_opts.consumer_name)
                    },
                    id=f"pulse_{pulse.timestamp.isoformat()}_{command.config_id}",
                    conflict_policy=ConflictPolicy.replace
                )
            logger.info(f"Scheduled {len(pulses)} pulses for config_id={command.config_id}.")
        except ZoneInfoNotFoundError:
            await self._remove_pulse_for_config(command.config_id)
            raise SchedulerError(
                f"Invalid timezone for config_id={command.config_id}.")
        except Exception as e:
            await self._remove_pulse_for_config(command.config_id)
            raise SchedulerError(f"Failed to plan pulses for config_id={command.config_id}: {e}")

    async def _handle_day_end(self, command: CommandDayEnd):
        logger.debug(f"Handling Clean for config_id={command.config_id}. Removing pulse jobs.")
        try:
            await self._remove_pulse_for_config(command.config_id)
            bot_config = await self.db.get_bot_config_by_id(command.config_id)
            await self.redis.stream_add(
                command,
                StreamContext.session_stream(bot_config.shard_id, self.worker_opts.consumer_name)
            )
        except Exception as e:
            raise SchedulerError(
                f"Failed _handle_clean for config {command.config_id} with event {command.trace_id}: {e}")

    async def _handle_clean(self, event: EventClean):
        """
        Handles a clean event: deletes scheduler jobs and forwards the command.
        """
        logger.debug(
            f"Handling EventClean for config_id={event.config_id} with trace_id {event.trace_id}. Removing all jobs.")
        bot_config = await self.db.get_bot_config_by_id(event.config_id)
        try:
            await self._remove_jobs_for_config(event.config_id)
            await self._remove_pulse_for_config(event.config_id)
            await self.redis.stream_add(
                event,
                StreamContext.session_stream(bot_config.shard_id, self.worker_opts.consumer_name)
            )
        except Exception as e:
            raise SchedulerError(f"Failed _handle_clean for config {event.config_id} with event {event.trace_id}: {e}")

    async def _remove_jobs_for_config(self, config_id: int):
        """Safely removes all APScheduler jobs for a specific config_id."""
        for job_id in (f"day_start_{config_id}", f"day_end_{config_id}"):
            try:
                await self.scheduler.remove_schedule(job_id)
                logger.debug(f"Removed schedule {job_id}.")
            except KeyError:
                logger.debug(f"Schedule {job_id} not found, skipping.")
            except Exception as e:
                raise SchedulerError(f"Failed to remove schedule {job_id}: {e}")

    async def _remove_pulse_for_config(self, config_id: int):
        """Safely removes all pulse-related APScheduler jobs for a config."""
        schedules = await self.scheduler.get_schedules()
        for schedule in schedules:
            if schedule.id.startswith("pulse_") and schedule.id.endswith(f"_{config_id}"):
                try:
                    await self.scheduler.remove_schedule(schedule.id)
                except KeyError:
                    pass
        logger.debug(f"Removed all pulse schedules for config_id={config_id}.")

    def stop(self):
        if self.is_running:
            logger.info(f"Stopping {self.worker_opts.consumer_name}...")
            self.is_running = False
        else:
            logger.debug(f"{self.worker_opts.consumer_name} already stopped or not started")

