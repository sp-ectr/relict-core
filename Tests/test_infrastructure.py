async def test_databases_are_alive_and_clean(db_test, redis_test):
    async with redis_test.lifecycle(), db_test.lifecycle():
        keys = await redis_test._client.keys("*")
        assert len(keys) == 0, "Redis is not empty!"
        async with db_test._pool_acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM bot_configs;")
            assert count == 0, "Postgres is not empty!"