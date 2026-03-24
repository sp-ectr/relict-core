import pytest
import pytest_asyncio

from relict_core.config.events import RawMessage
from relict_core.config.relict_settings import PostgreSettings, RedisSettings, LLMSettings
from relict_core.config.schemas import StreamContext, WorkerIdentity, SchedulerSettings, PersonalityManifest
from relict_core.databases.postgre_client import AsyncPostgreManager
from relict_core.databases.redis_client import RedisClient
from relict_core.workers.brain_worker import BrainWorker
from relict_core.workers.operator_worker import OperatorWorker
from relict_core.workers.session_worker import SessionWorker


@pytest.fixture
def pg_settings():
    return PostgreSettings(
        db_user="test_user",
        db_password="test_password",
        db_host="localhost",
        db_port=5433,
        db_name="test_db"
    )


@pytest.fixture
def redis_settings():
    return RedisSettings(
        redis_host="localhost",
        redis_port=6380
    )

@pytest.fixture
def llm_settings():
    return LLMSettings(api_key="api_key", model_name="gemini-2.5-flash")

@pytest.fixture
def db_test(pg_settings):
    return AsyncPostgreManager(pg_settings)


@pytest.fixture
def redis_test(redis_settings):
    return RedisClient(redis_settings)


@pytest.fixture
def operator_test(pg_settings, redis_settings):
    def _create(index: int):
        return OperatorWorker(
            pg_settings,
            redis_settings,
            WorkerIdentity(
                worker_name="operator",
                index=index
            )
        )

    return _create

@pytest.fixture
def scheduler_test(pg_settings, redis_settings):
    def _create(index: int, scheduler_settings: SchedulerSettings):
        return SessionWorker(
            pg_settings,
            redis_settings,
            WorkerIdentity(
                worker_name="operator",
                index=index
            ),
            scheduler_settings
        )
    return  _create

@pytest.fixture
def brain_test(pg_settings, redis_settings, llm_settings):
    def _create(index: int):
        return BrainWorker(
            pg_settings,
            redis_settings,
            WorkerIdentity(
                worker_name="brain_worker",
                index=index
            ),
            PersonalityManifest(
                role=(
                    "Сократ, древнегреческий философ из Афин, примерно 470–399 гг. до н.э. "
                    "Известен своей любовью к диалогу, поиску истины и сократическому методу. "
                    "Характеризуется мудростью, проницательностью, терпением и ироничным чувством юмора."
                ),
                goal=(
                    "Постоянно направлять собеседника к самопознанию и истине через вопросы. "
                    "Помогать понимать сложные концепции, стимулировать критическое мышление и моральное рассуждение."
                ),
                response_style=(
                    "Вежливый, логичный и терпеливый. Использует метафоры из повседневной жизни и философии. "
                    "Сообщения средней длины, без сленга и эмодзи. Иногда проявляет лёгкую иронию, но никогда оскорбительно."
                ),
                pulse_behavior=(
                    "На первом импульсе — приветствовать собеседника, уточнить тему диалога. "
                    "На каждом импульсе — задавать наводящие вопросы, разъяснять философские идеи через диалог. "
                    "На последнем импульсе — подводить выводы, оставлять собеседника с мыслью для размышлений, прощаться в стиле мудреца."
                ),
                relationship_rules=(
                    "Отношение к участнику определяется по доверию и взаимному уважению. "
                    "0 = игнорировать, 50 = нейтрально, 100 = максимальное доверие и внимание к ответам."
                ),
                memories_behavior=(
                    "Фиксирует ключевые идеи и ответы участников, которые помогают понять их мышление. "
                    "Хранит максимум 10 воспоминаний на пользователя, старые стираются автоматически. "
                    "Избирательно запоминает только полезную информацию для дальнейших диалогов."
                ),
                restrictions=[
                    "Не даёт прямых инструкций по действиям в реальном мире.",
                    "Не использует современные термины или технологии.",
                    "Не говорит о себе как о боте."
                ]
            ),
            llm_settings
        )
    return _create



@pytest_asyncio.fixture(autouse=True)
async def clean_all_db(db_test, redis_test):
    """Очищает Redis и PostgreSQL ПЕРЕД каждым тестом"""
    async with redis_test.lifecycle(), db_test.lifecycle():
        await redis_test._client.flushdb()
        async with db_test._pool_acquire() as conn:
            await conn.execute("TRUNCATE bot_configs, participants CASCADE;")
            await conn.execute("ALTER SEQUENCE bot_configs_id_seq RESTART WITH 1;")

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
            user_name="Артем",
            text="Привет сократ!"
        )

        async with redis_test.lifecycle():
            for _ in range(count):
                await redis_test.stream_add(test_messages, stream_context)

    return _create
