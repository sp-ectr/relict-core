import asyncio
from datetime import datetime, timezone
import pytest

from relict_core.config.events import CommandPulse, Response
from relict_core.config.schemas import StreamContext, BotConfig, LLMRequest, RedisKey


async def test_brain_worker_start_session_success(
        redis_test, db_test, brain_test, handler_test, operator_test
):
    async with db_test.lifecycle():
        config_id = await db_test.upsert_bot_config(
            BotConfig(
                chat_id=123,
                admin_id=123,
                shard_id=0,
                timezone="Europe/Moscow"
            )
        )
    print(f"\nКОНФИГУРАЦИЯ: {config_id}")
    assert config_id == 1, f"Ожидали config_id=1, а получили {config_id}"

    await handler_test(count=1)
    operator = operator_test(index=0)
    task = asyncio.create_task(operator.run())
    await asyncio.sleep(2)
    operator.stop()
    await task

    async with redis_test.lifecycle():
        await redis_test.stream_add(
            CommandPulse(
                config_id=config_id,
                label="Morning",
                timestamp=datetime.now(timezone.utc)
            ),
            StreamContext.session_stream(0, "consumer_0")
        )
    brain_worker = brain_test(index=0)

    task_0 = asyncio.create_task(brain_worker.run())
    await asyncio.sleep(2)
    brain_worker.stop()
    await task_0

    async with redis_test.lifecycle():
        await redis_test.stream_create_group(
            StreamContext.brain_stream("brain_worker_0")
        )

        result = await redis_test.stream_read_data(
            StreamContext.brain_stream("brain_worker_0"),
            block_ms=5000,
            count=5
        )

    assert result and len(result) > 0, f"brain_stream пустой, получено {len(result) if result else 0} сообщений"

    data = result[0]
    response_event = Response.model_validate(data.payload)

    print("\n>>> Пришло в brain_stream:")
    print(f"\n>>>{response_event.config_id}")
    print(f"\n>>>{response_event.trace_id}")
    print(f"\n>>>{response_event.content.new_participants}")
    print(f"\n>>>{response_event.content.new_memories}")
    print(f"\n>>>{response_event.content.text_reply}")

    assert response_event.config_id == config_id


async def test_brain_worker_silent_mode_on_no_messages(redis_test, db_test, brain_test, handler_test, operator_test):
    async with db_test.lifecycle():
        config_id = await db_test.upsert_bot_config(
            BotConfig(
                chat_id=123,
                admin_id=123,
                shard_id=0,
                timezone="Europe/Moscow"
            )
        )
    print(f"\nКОНФИГУРАЦИЯ: {config_id}")
    assert config_id == 1, f"Ожидали config_id=1, а получили {config_id}"

    async with redis_test.lifecycle():
        await redis_test.stream_create_group(
            StreamContext.message_stream(config_id, "brain_worker_0"), True
        )

    async with redis_test.lifecycle():
        await redis_test.stream_add(
            CommandPulse(
                config_id=config_id,
                label="Morning",
                timestamp=datetime.now(timezone.utc),
                is_first_of_slot=True
            ),
            StreamContext.session_stream(0, "consumer_0")
        )

    brain_worker = brain_test(index=0)

    task_0 = asyncio.create_task(brain_worker.run())
    await asyncio.sleep(2)
    print(">>> stopping worker")
    brain_worker.stop()
    print(">>> worker stopped, awaiting task")
    await task_0
    print(">>> task done")

    async with redis_test.lifecycle():
        await redis_test.stream_create_group(
            StreamContext.brain_stream("brain_worker_0")
        )

        result = await redis_test.stream_read_data(
            StreamContext.brain_stream("brain_worker_0"),
            block_ms=5000,
            count=5
        )

    assert result and len(result) > 0, f"brain_stream пустой, получено {len(result) if result else 0} сообщений"

    data = result[0]
    response_event = Response.model_validate(data.payload)

    print("\n>>> Пришло в brain_stream:")
    print(f"\n>>>{response_event.config_id}")
    print(f"\n>>>{response_event.trace_id}")
    print(f"\n>>>{response_event.content.new_participants}")
    print(f"\n>>>{response_event.content.new_memories}")
    print(f"\n>>>{response_event.content.text_reply}")

    assert response_event.config_id == config_id
    async with redis_test.lifecycle():
        silent_counter_key = RedisKey.silence_counter(config_id)

        assert await redis_test.has_key(silent_counter_key) is True, "silence_counter не установлен!"

        silent_lock_key = RedisKey.silence_lock(config_id)
        assert await redis_test.has_key(silent_lock_key) is False, "silence_lock уже стоит, хотя не должен!"
