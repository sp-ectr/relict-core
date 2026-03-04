import pytest
import pytest_asyncio

from relict_core.config.events import RawMessage
from relict_core.config.relict_settings import PostgreSettings, RedisSettings
from relict_core.config.schemas import StreamContext, WorkerIdentety
from relict_core.databases.postgre_client import AsyncPostgreManager
from relict_core.databases.redis_client import RedisClient
from relict_core.workers.operator_worker import OperatorWorker


@pytest.fixture
def db_test():
    return AsyncPostgreManager(
        PostgreSettings(
            db_user="test_user",
            db_password="test_password",
            db_host="localhost",
            db_port=5433,
            db_name="test_db"
        ))


@pytest.fixture
def redis_test():
    return RedisClient(
        RedisSettings(
            redis_host="localhost",
            redis_port=6380
        ))


@pytest.fixture
def operator_test():
    def _create(index: int):
        return OperatorWorker(
            PostgreSettings(
                db_user="test_user",
                db_password="test_password",
                db_host="localhost",
                db_port=5433,
                db_name="test_db"
            ),
            RedisSettings(
                redis_host="localhost",
                redis_port=6380
            ),
            WorkerIdentety(
                worker_name="operator",
                index=index
            )
        )

    return _create


@pytest_asyncio.fixture(autouse=True)
async def clean_all_db(db_test, redis_test):
    async with redis_test.lifecycle(), db_test.lifecycle():
        await redis_test._client.flushdb()
        async with db_test._pool_acquire() as conn:
            await conn.execute("TRUNCATE bot_configs, participants CASCADE;")
    yield


@pytest_asyncio.fixture
async def handler_test(redis_test):
    async def _create(count: int):
        stream_context = StreamContext(
            stream="raw_messages",
            group="test",
        )
        test_messages = RawMessage(
            chat_id=123,
            user_id=123,
            user_name="Test",
            text="Test message"
        )

        async with redis_test.lifecycle():
            for _ in range(count):
                await redis_test.stream_add(test_messages, stream_context)

    return _create


