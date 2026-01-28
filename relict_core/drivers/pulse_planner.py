"""
This module defines the PulsePlanner, responsible for generating a multi-layered,
human-like schedule of bot activity. It combines macro-level planning (random activity windows)
with micro-level rhythm (random pulses within those windows).
"""
import logging
import random
from datetime import datetime, timedelta
from pydantic import BaseModel
from zoneinfo import ZoneInfo

from relict_core.config.relict_settings import SchedulerSettings

logger = logging.getLogger(__name__)


class Pulse(BaseModel):
    """Represents a single, precise moment for the bot to act."""
    timestamp: datetime
    label: str
    is_first_of_day: bool = False
    is_last_of_day: bool = False


class SessionSlot(BaseModel):
    """Represents a macro-level window of bot activity."""
    start: datetime
    end: datetime


class PulsePlanner:
    """Generates a schedule of pulses within random activity windows."""

    def __init__(self, timezone: ZoneInfo, opts: SchedulerSettings):
        self.tz = timezone
        self.opts = opts
        self.now = datetime.now(self.tz)

        self.day_start = self.now.replace(hour=self.opts.DAY_START_HOUR, minute=0, second=0, microsecond=0)
        self.day_end = self.now.replace(hour=self.opts.DAY_END_HOUR, minute=0, second=0, microsecond=0)

        self.min_sessions_per_day = self.opts.MIN_SESSIONS_PER_DAY
        self.max_sessions_per_day = self.opts.MAX_SESSIONS_PER_DAY
        self.min_session_duration_min = self.opts.MIN_SESSION_DURATION_MIN
        self.max_session_duration_min = self.opts.MAX_SESSION_DURATION_MIN

        self.min_pulse_interval_sec = self.opts.MIN_PULSE_INTERVAL_SEC
        self.max_pulse_interval_sec = self.opts.MAX_PULSE_INTERVAL_SEC

        self.jitter_first = random.randint(10, 120)

    def plan_pulses_for_today(self) -> list[Pulse]:
        """Generates all pulses for the rest of the day."""
        session_slots = self._plan_session_slots()
        if not session_slots:
            logger.debug("No session slots could be planned for the rest of the day.")
            return []

        all_pulses: list[Pulse] = []

        for i, slot in enumerate(session_slots):
            pulses_in_slot = self._plan_pulses_within_slot(slot)
            if not pulses_in_slot:
                continue

            if i == 0:
                pulses_in_slot[0].is_first_of_day = True


            all_pulses.extend(pulses_in_slot)

        if all_pulses:
            all_pulses.sort(key=lambda p: p.timestamp)
            all_pulses[-1].is_last_of_day = True

        logger.info(f"Planned {len(session_slots)} sessions with a total of {len(all_pulses)} pulses.")
        return all_pulses

    def _plan_session_slots(self) -> list[SessionSlot]:
        """Plan macro-level activity windows."""
        start = max(self.now, self.day_start)
        if start >= self.day_end:
            return []

        num_sessions = random.randint(self.min_sessions_per_day, self.max_sessions_per_day)
        slots: list[SessionSlot] = []

        max_start_sec = int((self.day_end - self.day_start).total_seconds() - self.min_session_duration_min * 60)
        first_slot_start = self.day_start + timedelta(seconds=random.randint(0, max_start_sec))
        first_slot_duration = timedelta(
            minutes=random.randint(self.min_session_duration_min, self.max_session_duration_min))
        first_slot_end = min(first_slot_start + first_slot_duration, self.day_end)
        slots.append(SessionSlot(start=first_slot_start, end=first_slot_end))

        remaining_slots = self._plan_random_slots(num_sessions - 1, first_slot_end, self.day_end)
        slots.extend(remaining_slots)

        return sorted(slots, key=lambda s: s.start)

    def _plan_random_slots(self, count: int, start_time: datetime, end_time: datetime) -> list[SessionSlot]:
        """Generates non-overlapping random slots of random duration."""
        if count <= 0 or start_time >= end_time:
            return []

        slots = []
        total_seconds = (end_time - start_time).total_seconds()
        segment = total_seconds / max(count, 1)

        for i in range(count):
            offset = i * segment + random.randint(0, int(segment / 2))
            slot_start = start_time + timedelta(seconds=offset)
            duration = timedelta(minutes=random.randint(self.min_session_duration_min, self.max_session_duration_min))
            slot_end = min(slot_start + duration, end_time)
            if slot_start < slot_end:
                slots.append(SessionSlot(start=slot_start, end=slot_end))

        return slots

    def _plan_pulses_within_slot(self, slot: SessionSlot) -> list[Pulse]:
        """Generates pulses inside one slot with contextual label."""
        pulses = []
        current_time = slot.start
        current_time += timedelta(seconds=self.jitter_first)

        while current_time < slot.end:
            label = self._get_label_for_time(current_time)
            pulses.append(Pulse(timestamp=current_time, label=label))
            interval = random.randint(self.min_pulse_interval_sec, self.max_pulse_interval_sec)
            next_time = current_time + timedelta(seconds=interval)
            if next_time >= slot.end:
                pulses.append(Pulse(timestamp=slot.end, label=self._get_label_for_time(slot.end)))
                break
            current_time = next_time

        return sorted(pulses, key=lambda p: p.timestamp)

    @staticmethod
    def _get_label_for_time(time: datetime) -> str:
        """Maps a timestamp to a time-of-day label."""
        hour = time.hour
        if 6 <= hour < 12:
            return "morning"
        if 12 <= hour < 18:
            return "day"
        if 18 <= hour < 23:
            return "evening"
        return "night"
