import asyncio

from relict_core.config.events import RawMessage
from relict_core.config.schemas import BotConfig, Participant, RedisKey, RedisData, StreamContext


async def test_no_bot_config(redis_test, handler_test, operator_test):
    operator = operator_test(index=1)  # фабрика
    await handler_test(count=1)  # добавляем 1 сообщение

    task = asyncio.create_task(operator.run())  # запускаем worker

    await asyncio.sleep(2)  # даем время обработать

    await operator.stop()  # останавливаем worker
    await task  # ждём завершения задачи

    msg_count = await redis_test._client.xlen(f"messages_stream:{123}")  # чекаем создал ли воркер стрим
    assert msg_count == 0  # успех?


async def test_bot_config_success_and_no_silent_key(
        db_test, redis_test, handler_test, operator_test
):
    operator = operator_test(index=0)

    async with db_test.lifecycle():
        await db_test.upsert_bot_config(
            BotConfig(
                chat_id=123,
                admin_id=123,
                timezone="Europe/Moscow"
            )
        )

    async with redis_test.lifecycle():
        await redis_test.set_key(RedisKey(key="silence_lock:123"), ttl=0)
        await redis_test.set_key(RedisKey(key="silence_counter:123"), ttl=0)

    await handler_test(count=1)

    task = asyncio.create_task(operator.run())
    await asyncio.sleep(2)
    await operator.stop()
    await task

    async with redis_test.lifecycle():
        msg_count = await redis_test._client.xlen("messages_stream:123")
        assert msg_count > 0

        assert not await redis_test.has_key(RedisKey(key="silence_lock:123"))
        assert not await redis_test.has_key(RedisKey(key="silence_counter:123"))


async def test_bot_config_participant_in_redis_cash_and_rate_limit_works(
        db_test, redis_test, handler_test, operator_test
):
    operator = operator_test(index=0)
    async with redis_test.lifecycle(), db_test.lifecycle():
        assert not await redis_test.get_data(RedisData.bot_config(123))  # чекаем что конфига нету в кэше
        assert not await db_test.get_bot_config(123)  # и в дб

        config_id = await db_test.upsert_bot_config(
            BotConfig(
                chat_id=123,
                admin_id=123,
                timezone="Europe/Moscow"
            )
        )
    await handler_test(count=15)  # создаем сообщений больше захардкореного лимита в 10

    task = asyncio.create_task(operator.run())
    await asyncio.sleep(2)
    async with redis_test.lifecycle():
        assert await redis_test.get_data(RedisData.bot_config(123))  # чекаем что кеш конфига появился
        assert await redis_test.has_key(RedisKey(key=f"rate_limit_user:123"))  # и рейт лимит установлен

    async with redis_test.lifecycle():
        msg_count = await redis_test._client.xlen("messages_stream:123")
        assert msg_count == 10  # все что выше лимита должно быть проигнорировано

    async with db_test.lifecycle():
        await db_test.insert_participant(
            Participant(
                config_id=config_id,
                user_id=123,
                custom_name="User",
                gender="Male",
            )
        )  # добавляем пользователя чтоб он попал в кеш

    # динамически ждем пока рейт лимит исчезнет
    while True:
        async with redis_test.lifecycle():
            exists = await redis_test.has_key(RedisKey(key="rate_limit_user:123"))

        if not exists:
            break

        await asyncio.sleep(0.2)  # проверяем что исчез

    await handler_test(count=15)  # закидываем еще 15 сообщений, пользователь уже в бд
    await asyncio.sleep(2)
    await operator.stop()
    await task

    async with redis_test.lifecycle():
        msg_count = await redis_test._client.xlen("messages_stream:123")
        assert msg_count == 20  # чекаем что все предсказуемо + 10, а не 15
        config = await redis_test.get_data(RedisData.bot_config(123))
        assert config  # чекаем что кеш есть
        participant = await redis_test.get_data(RedisData.participant_config(config.id, 123))
        assert participant  # чекаем что участник в кеше

async def test_black_mark_ignored_participant(db_test, redis_test, operator_test):
    operator = operator_test(index=0)
    async with db_test.lifecycle():
        config_id = await db_test.upsert_bot_config(BotConfig(chat_id=123, admin_id=123, timezone="Europe/Moscow"))
        participant_id = await db_test.insert_participant(
            Participant(
                config_id=config_id,
                user_id=123,
                custom_name="IgnoredUser",
                gender="Male"
            )
        )
        await db_test.set_ignore_status(participant_id, True) # ключевой флаг

    task = asyncio.create_task(operator.run())
    await asyncio.sleep(1)

    # отправляем сообщение от игнорированного пользователя
    async with redis_test.lifecycle():
        raw_event = RawMessage(
            chat_id=123,
            user_id=123,
            user_name="IgnoredUser",
            text="Hello ignored"
        )
        await redis_test.stream_add(raw_event, StreamContext(stream="raw_messages", group="operators"))

    await asyncio.sleep(2)

    async with redis_test.lifecycle():
        msg_count = await redis_test._client.xlen("messages_stream:123")
        assert msg_count == 0  # все сообщения должны быть отброшены

    await operator.stop()
    await task


async def test_poison_pill_invalid_json(redis_test, operator_test):
    operator = operator_test(index=0)
    task = asyncio.create_task(operator.run())
    await asyncio.sleep(1)

    async with redis_test.lifecycle():
        await redis_test._client.xadd("raw_messages", {"eue": "awdwa"})
        await redis_test._client.xadd("raw_messages", {"payload": "not_a_json"})

    await asyncio.sleep(2)  # даем время на обработ
    # проверяем, что воркер жив
    assert task.done() is False  # таска не упала

    async with redis_test.lifecycle():
        # stream должен быть очищен от обработанных сообщений
        pending = await redis_test._client.xpending("raw_messages", "operators")
        assert pending["pending"] == 0  # все мусорные сообщения ack'ed

    await operator.stop()
    await task


