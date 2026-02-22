"""
Pulse Worker

Listens for high-level day cycle commands (DayStart, DayEnd, Clean) and
manages the micro-schedule of "pulses" within APScheduler.
"""
import logging
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler import AsyncScheduler, ConflictPolicy
from apscheduler.triggers.date import DateTrigger

from relict_core.databases.postgre_client import AsyncPostgreManager
from relict_core.databases.redis_client import RedisClient
from relict_core.config.events import (CommandDayStart,
                                CommandDayEnd,
                                CommandClean,
                                Pulse
                                )
from relict_core.drivers.pulse_planner import PulsePlanner
from relict_core.drivers.stream_driver import StreamDriver
from relict_core.config.logging_config import log_error
from relict_core.config.relict_settings import SchedulerSettings
from relict_core.config.exceptions import SchedulerError, StreamError

logger = logging.getLogger(__name__)


class PulsesWorker:
    """
    Listens to the session_stream and manages the micro-schedule (pulses) in APScheduler.
    """

    def __init__(
            self,
            scheduler: AsyncScheduler,
            redis: RedisClient,
            data_base: AsyncPostgreManager,
            pulse_planner: type[PulsePlanner],
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
        self.pulse_planner = pulse_planner
        self.stream = StreamDriver(
            redis=redis,
            consume_stream=self.consume_stream,
            group_name="scheduler_pulses_workers",
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
                        await self._handle_day_start(config_id)
                        await self.stream.dispatch_event(event, self.produce_stream)
                    case CommandDayEnd(config_id=config_id):
                        logger.debug(f"Handling Clean for config_id={config_id}. Removing pulse jobs.")
                        await self._remove_pulse_for_config(config_id)
                        await self.stream.dispatch_event(event, self.produce_stream)
                    case CommandClean(config_id=config_id):
                        logger.debug(f"Handling Clean for config_id={config_id}. Removing pulse jobs.")
                        await self._remove_pulse_for_config(config_id)
                        await self.stream.dispatch_event(event, self.produce_stream)
                    case _:
                        logger.warning(f"Unexpected event in session_stream: {type(event).__name__}")

                await self.stream.ack(msg_id)

            except StreamError as e:
                raise SchedulerError(f"Stream logic failed: {e}")
            except Exception as e:
                raise SchedulerError(f"Critical error in PulseWorker loop: {e}")

    async def _handle_day_start(self, config_id: int):
        """
        On DayStart, plan and schedule all pulses for the current day.
        Then, forward the DayStart command to the redis.
        """
        logger.debug(f"Handling СommandDayStart for config_id={config_id}. Planning pulses...")
        try:
            timezone = ZoneInfo(await self.db.get_timezone_by_config_id(config_id))
            await self._remove_pulse_for_config(config_id)

            planner = self.pulse_planner(timezone=timezone, opts=self.opts)
            pulses = planner.plan_pulses_for_today()

            for pulse in pulses:
                pulse_command = Pulse(
                    config_id=config_id,
                    is_first_of_day=pulse.is_first_of_day,
                    is_last_of_day=pulse.is_last_of_day,
                    label=pulse.label
                )
                await self.scheduler.add_schedule(
                    self.stream.dispatch_event,
                    trigger=DateTrigger(pulse.timestamp),
                    kwargs={
                        "event": pulse_command,
                        "stream_name": self.produce_stream
                    },
                    id=f"pulse_{pulse.timestamp.isoformat()}_{config_id}",
                    conflict_policy=ConflictPolicy.replace
                )
            logger.info(f"Scheduled {len(pulses)} pulses for config_id={config_id}.")

        except ZoneInfoNotFoundError:
            await self._remove_pulse_for_config(config_id)
            raise SchedulerError(
                f"Invalid timezone for config_id={config_id}.")
        except Exception as e:
            await self._remove_pulse_for_config(config_id)
            raise SchedulerError(f"Failed to plan pulses for config_id={config_id}: {e}")

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
        self.is_running = False
