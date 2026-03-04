"""
PulsePlanner — generates a human-like schedule of bot activity.

Two-level planning:
  1. Session slots  — macro windows of activity distributed across the day
  2. Pulses         — precise action moments within each slot
"""
import logging
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from relict_core.config.schemas import SchedulerSettings, Pulse, SessionSlot

logger = logging.getLogger(__name__)


class PulsePlanner:
    """Generates a schedule of pulses within random activity windows."""

    def __init__(self, timezone: ZoneInfo, opts: SchedulerSettings):
        self.tz = timezone
        self.opts = opts

        now = datetime.now(self.tz)
        self.now = now
        self.day_start = now.replace(hour=opts.day_start_hour, minute=0, second=0, microsecond=0)
        self.day_end = now.replace(hour=opts.day_end_hour, minute=0, second=0, microsecond=0)

    def plan_pulses_for_today(self) -> list[Pulse]:
        """Generates all pulses from now until end of day."""
        slots = self._plan_session_slots()
        if not slots:
            logger.debug("No session slots could be planned for the rest of the day.")
            return []

        all_pulses: list[Pulse] = []

        for slot in slots:
            pulses_in_slot = self._plan_pulses_within_slot(slot)
            all_pulses.extend(pulses_in_slot)

        if not all_pulses:
            return []

        all_pulses.sort(key=lambda p: p.timestamp)
        all_pulses[0].is_first_of_day = True
        all_pulses[-1].is_last_of_day = True

        logger.info(f"Planned {len(slots)} sessions with {len(all_pulses)} pulses total.")
        return all_pulses

    def _plan_session_slots(self) -> list[SessionSlot]:
        """
        Divides the remaining day into N equal segments,
        then places one slot at a random position within each segment.
        Guarantees no overlaps and human-like unpredictability.
        """
        effective_start = max(self.now, self.day_start)
        if effective_start >= self.day_end:
            return []

        num_sessions = random.randint(self.opts.min_sessions_per_day, self.opts.max_sessions_per_day)
        total_seconds = (self.day_end - effective_start).total_seconds()
        segment_sec = total_seconds / num_sessions

        slots = []
        for i in range(num_sessions):
            segment_start = effective_start + timedelta(seconds=i * segment_sec)
            segment_end = effective_start + timedelta(seconds=(i + 1) * segment_sec)

            # Slot can start anywhere in the first half of the segment
            max_offset = (segment_end - segment_start).total_seconds() / 2
            slot_start = segment_start + timedelta(seconds=random.randint(0, int(max_offset)))

            duration_min = random.randint(self.opts.min_session_duration_min, self.opts.max_session_duration_min)
            slot_end = min(slot_start + timedelta(minutes=duration_min), self.day_end)

            if slot_start < slot_end:
                slots.append(SessionSlot(start=slot_start, end=slot_end))

        return slots

    def _plan_pulses_within_slot(self, slot: SessionSlot) -> list[Pulse]:
        """
        Fills a slot with pulses at random intervals.
        Each slot gets its own jitter at the start to spread load across many configs.
        """
        pulses = []
        jitter = random.randint(10, 120)
        current_time = slot.start + timedelta(seconds=jitter)

        while current_time < slot.end:
            pulses.append(Pulse(
                timestamp=current_time,
                label=self._label_for_hour(current_time.hour)
            ))

            interval = random.randint(self.opts.min_pulse_interval_sec, self.opts.max_pulse_interval_sec)
            next_time = current_time + timedelta(seconds=interval)

            if next_time >= slot.end:
                # Always close the slot with a final pulse at slot.end
                pulses.append(Pulse(
                    timestamp=slot.end,
                    label=self._label_for_hour(slot.end.hour)
                ))
                break

            current_time = next_time

        return pulses

    @staticmethod
    def _label_for_hour(hour: int) -> str:
        if 6 <= hour < 12:
            return "morning"
        if 12 <= hour < 18:
            return "day"
        if 18 <= hour < 23:
            return "evening"
        return "night"
