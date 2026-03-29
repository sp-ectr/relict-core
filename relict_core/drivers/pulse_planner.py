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

    def __init__(self, timezone: ZoneInfo, opts: SchedulerSettings, now: datetime | None = None):
        self.tz = timezone
        self.opts = opts

        now = now or datetime.now(self.tz)
        self.now = now
        self.day_start = now.replace(hour=opts.day_start_hour, minute=0, second=0, microsecond=0)
        self.day_end = now.replace(hour=opts.day_end_hour, minute=0, second=0, microsecond=0)

    def plan_pulses_for_today(self) -> list[Pulse]:
        """Plan all pulses for the current day across all session slots."""
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
        logger.info(f"Planned {len(slots)} sessions with {len(all_pulses)} pulses total.")
        return all_pulses

    @staticmethod
    def _random_delay_min(min_min: int, max_min: int) -> timedelta:
        """Random delay — simulates human unpredictability."""
        return timedelta(minutes=random.randint(min_min, max_min))

    def _plan_session_slots(self) -> list[SessionSlot]:
        """ТЕСТОВЫЙ РЕЖИМ: только 1 слот, чтобы быстро проверить все пульсы до конца."""
        effective_start = max(self.now, self.day_start)
        if effective_start >= self.day_end:
            return []

        # === ТЕСТ: первый (и единственный) слот начинается ОЧЕНЬ СКОРО ===
        test_delay_sec = random.randint(60, 120)  # 1–2 минуты от сейчас
        effective_start += timedelta(seconds=test_delay_sec)

        if effective_start >= self.day_end:
            return []

        # Делаем слот максимально длинным, чтобы все пульсы успели отработать
        duration_min = self.opts.max_session_duration_min

        slot_start = effective_start
        slot_end = min(slot_start + timedelta(minutes=duration_min), self.day_end)

        if slot_start >= slot_end:
            return []

        logger.info(f"ТЕСТОВЫЙ РЕЖИМ: создан 1 слот на {duration_min} минут "
                    f"(с {slot_start.strftime('%H:%M')} до {slot_end.strftime('%H:%M')})")

        return [SessionSlot(start=slot_start, end=slot_end)]

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
                label=self._label_for_hour(current_time.hour),
                is_first_of_slot=len(pulses) == 0
            ))

            interval = random.randint(self.opts.min_pulse_interval_sec, self.opts.max_pulse_interval_sec)
            next_time = current_time + timedelta(seconds=interval)

            if next_time >= slot.end:
                pulses.append(Pulse(
                    timestamp=slot.end,
                    label=self._label_for_hour(slot.end.hour),
                    is_last_of_slot=True
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
