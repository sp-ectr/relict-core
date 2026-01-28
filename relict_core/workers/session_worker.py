import logging
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler import AsyncScheduler, ConflictPolicy
from apscheduler.triggers.cron import CronTrigger

from relict_core.databases.postgre_client import AsyncPostgresManager
from relict_core.databases.redis_client import RedisClient
from relict_core.drivers.stream_driver import StreamDriver
from relict_core.config.events import (EventStart,
                                       EventClean,
                                       CommandDayStart,
                                       CommandDayEnd,
                                       CommandClean
                                       )
from relict_core.config.logging_config import log_error
from relict_core.config.relict_settings import SchedulerSettings
from relict_core.config.exceptions import SchedulerError, StreamError

logger = logging.getLogger(__name__)


class SessionWorker:
    """
    Listens to system events to create or destroy the main daily cycle jobs for in APScheduler.
    """

    def __init__(
            self,
            scheduler: AsyncScheduler,
            redis: RedisClient,
            data_base: AsyncPostgresManager,
            opts: SchedulerSettings,
            worker_name: str,
            consume_stream: str,
            produce_stream: str
    ):
        self.scheduler = scheduler
        self.worker_name = worker_name
        self.opts = opts
        self.consume_stream = consume_stream
        self.produce_stream = produce_stream
        self.db = data_base
        self.stream = StreamDriver(
            redis=redis,
            consume_stream=self.consume_stream,
            group_name="scheduler_sessions_workers",
            consumer_name=self.worker_name
        )
        self.is_running = False
        logger.info(f"{self.worker_name} is initialized.")

    @log_error
    async def run(self):
        """The main event loop for the worker."""
        self.is_running = True
        logger.info(f"{self.worker_name} is started. Listening for {self.consume_stream}...")

        await self.stream.ensure_group()

        while self.is_running:
            try:
                result = await self.stream.next_event()

                if not result:
                    continue

                msg_id, event = result

                match event:
                    case EventStart(config_id=config_id):
                        await self._handle_start(config_id, event.trace_id)
                    case EventClean(config_id=config_id):
                        await self._handle_clean(config_id, event.trace_id)
                    case _:
                        logger.warning(f"Unexpected event in system_stream: {type(event).__name__}")

                await self.stream.ack(msg_id)

            except StreamError as e:
                raise SchedulerError(f"Stream logic failed: {e}")
            except Exception as e:
                raise SchedulerError(f"Critical error in SchedulerWorker loop: {e}")

    async def _handle_start(self, config_id: int, trace_id: str):
        """
        Handler a new config: deletes any old jobs and creates new,
        perpetual DayStart/DayEnd Cron jobs.
        """
        logger.debug(f"Handling EventStart for config_id={config_id}. Setting up daily cycle.")

        try:
            timezone = ZoneInfo(await self.db.get_timezone_by_config_id(config_id))
            await self._remove_jobs_for_config(config_id)

            start_command = CommandDayStart(config_id=config_id, trace_id=trace_id)
            await self.scheduler.add_schedule(
                self.stream.dispatch_event,
                trigger=CronTrigger(hour=self.opts.DAY_START_HOUR, minute=0, timezone=timezone),
                kwargs={
                    "event": start_command,
                    "stream_name": self.produce_stream
                },
                id=f"day_start_{config_id}",
                conflict_policy=ConflictPolicy.replace
            )
            end_command = CommandDayEnd(config_id=config_id, trace_id=trace_id)
            await self.scheduler.add_schedule(
                self.stream.dispatch_event,
                trigger=CronTrigger(hour=self.opts.DAY_END_HOUR, minute=0, timezone=timezone),
                kwargs={
                    "event": end_command,
                    "stream_name": self.produce_stream
                },
                id=f"day_end_{config_id}",
                conflict_policy=ConflictPolicy.replace
            )

            now_in_tz = datetime.now(timezone)
            if self.opts.DAY_START_HOUR <= now_in_tz.hour < self.opts.DAY_END_HOUR:
                logger.info(f"Active hours: triggering initial DayStart for config_id={config_id}")
                await self.stream.dispatch_event(start_command, self.produce_stream)
            else:
                logger.info(f"Off-hours: initial DayStart for config_id={config_id} will be triggered by cron.")


        except ZoneInfoNotFoundError:
            await self._remove_jobs_for_config(config_id)
            raise SchedulerError(
                f"Invalid timezone for config_id={config_id}.")
        except Exception as e:
            await self._remove_jobs_for_config(config_id)
            raise SchedulerError(f"Failed to schedule jobs for config {config_id}: {e}")

    async def _handle_clean(self, config_id: int, trace_id: str):
        """
        Handles a clean command: deletes scheduler jobs and forwards the command.
        """
        logger.debug(f"Handling CommandClean for config_id={config_id} with trace_id {trace_id}. Removing all jobs.")
        command = CommandClean(config_id=config_id, trace_id=trace_id)
        try:
            await self._remove_jobs_for_config(config_id)
            await self.stream.dispatch_event(command, self.produce_stream)
        except Exception as e:
            raise SchedulerError(f"Failed _handle_clean for config {config_id} with event {trace_id}: {e}")

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

    def stop(self):
        self.is_running = False
