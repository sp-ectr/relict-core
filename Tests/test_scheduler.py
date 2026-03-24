import asyncio

from relict_core.config.events import EventStart, EventClean
from relict_core.config.schemas import BotConfig, StreamContext, SchedulerSettings

from datetime import datetime
from zoneinfo import ZoneInfo

from relict_core.drivers.pulse_planner import PulsePlanner


def test_pulse_planner_distribution():
    settings = SchedulerSettings()
    tz = ZoneInfo("Europe/Moscow")
    fake_now = datetime.now(tz).replace(hour=9, minute=0, second=0, microsecond=0)

    print("\nРаспределение пульсов (симуляция старта в 09:00):")
    for i in range(10):
        planner = PulsePlanner(timezone=tz, opts=settings, now=fake_now)
        pulses = planner.plan_pulses_for_today()

        if pulses:
            first = min(p.timestamp for p in pulses).strftime('%H:%M')
            last = max(p.timestamp for p in pulses).strftime('%H:%M')
            total = len(pulses)
            print(f"Run {i + 1}: первый {first} | последний {last} | пульсов {total}")
        else:
            print(f"Run {i + 1}: нет пульсов")


async def test_scheduler_too_late(redis_test, db_test, scheduler_test):
    now = datetime.now(ZoneInfo("Europe/Moscow"))

    settings = SchedulerSettings(
        day_end_hour=now.hour,
        min_session_duration_min=5,
        min_sessions_per_day=1,
        max_sessions_per_day=2,
        min_pulse_interval_sec=10,
        max_pulse_interval_sec=30,
    )

    async with db_test.lifecycle(), redis_test.lifecycle():
        config_id = await db_test.upsert_bot_config(
            BotConfig(
                chat_id=123,
                admin_id=123,
                timezone="Europe/Moscow"
            ))
        await redis_test.stream_add(
            data=EventStart(config_id=config_id),
            opts=StreamContext(stream="system_stream", group="schedulers")
        )

    worker = scheduler_test(index=0, scheduler_settings=settings)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(2)

    schedules = await worker.scheduler.get_schedules()
    ids = [s.id for s in schedules]
    print(f"\nSchedules: {ids}")
    assert f"day_start_{config_id}" in ids
    assert f"day_end_{config_id}" in ids
    assert not any(s.id.startswith("pulse_") for s in schedules)

    worker.stop()
    await task


async def test_scheduler_active(redis_test, db_test, scheduler_test):
    settings = SchedulerSettings()

    async with db_test.lifecycle(), redis_test.lifecycle():
        config_id = await db_test.upsert_bot_config(
            BotConfig(
                chat_id=123,
                admin_id=123,
                timezone="Europe/Moscow"
            ))
        await redis_test.stream_add(
            data=EventStart(config_id=config_id),
            opts=StreamContext(stream="system_stream", group="schedulers")
        )

    worker = scheduler_test(index=0, scheduler_settings=settings)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(2)

    schedules = await worker.scheduler.get_schedules()
    pulse_schedules = [s for s in schedules if s.id.startswith("pulse_")]

    print(f"\nAll schedules:")
    for s in sorted(schedules, key=lambda x: x.id):
        print(f"  {s.id} -> {s.next_fire_time}")

    assert f"day_start_{config_id}" in [s.id for s in schedules]
    assert f"day_end_{config_id}" in [s.id for s in schedules]
    assert len(pulse_schedules) > 0

    worker.stop()
    await task


async def test_scheduler_distribution(redis_test, db_test, scheduler_test):
    settings = SchedulerSettings()
    last_sessions = []

    for i in range(10):
        async with db_test.lifecycle(), redis_test.lifecycle():
            config_id = await db_test.upsert_bot_config(
                BotConfig(chat_id=123 + i, admin_id=123, timezone="Europe/Moscow")
            )
            await redis_test.stream_add(
                data=EventStart(config_id=config_id),
                opts=StreamContext(stream="system_stream", group="schedulers")
            )

        worker = scheduler_test(index=0, scheduler_settings=settings)
        task = asyncio.create_task(worker.run())
        await asyncio.sleep(2)

        schedules = await worker.scheduler.get_schedules()
        pulse_schedules = sorted(
            [s for s in schedules if s.id.startswith("pulse_")],
            key=lambda x: x.next_fire_time
        )

        if pulse_schedules:
            last = pulse_schedules[-1].next_fire_time
            first = pulse_schedules[0].next_fire_time
            last_sessions.append(last)
            print(f"Run {i + 1}: первый пульс {first.strftime('%H:%M')} | последний пульс {last.strftime('%H:%M')}")

        worker.stop()
        await task

    print(f"\nПоследние пульсы: {[s.strftime('%H:%M') for s in last_sessions]}")


async def test_scheduler_clean(redis_test, db_test, scheduler_test):
    now = datetime.now(ZoneInfo("Europe/Moscow"))

    settings = SchedulerSettings(
        day_end_hour=(now.hour + 2) % 24,
        min_session_duration_min=5,
        min_sessions_per_day=1,
        max_sessions_per_day=2,
        min_pulse_interval_sec=10,
        max_pulse_interval_sec=30,
    )

    async with db_test.lifecycle(), redis_test.lifecycle():
        config_id = await db_test.upsert_bot_config(
            BotConfig(
                chat_id=123,
                admin_id=123,
                timezone="Europe/Moscow"
            ))
        await redis_test.stream_add(
            data=EventStart(config_id=config_id),
            opts=StreamContext(stream="system_stream", group="schedulers")
        )
        await redis_test.stream_add(
            data=EventClean(config_id=config_id),
            opts=StreamContext(stream="system_stream", group="schedulers")
        )

    worker = scheduler_test(index=0, scheduler_settings=settings)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(2)

    schedules = await worker.scheduler.get_schedules()
    ids = [s.id for s in schedules]
    print(f"\nSchedules after clean: {ids}")

    assert not any(s.id.startswith("pulse_") for s in schedules)
    assert f"day_start_{config_id}" not in ids
    assert f"day_end_{config_id}" not in ids

    worker.stop()
    await task
