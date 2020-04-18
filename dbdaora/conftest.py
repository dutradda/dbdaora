import pytest
from aioredis import create_redis_pool

from dbdaora import (
    AioRedisDataSource,
    DictFallbackDataSource,
    DictMemoryDataSource,
)


@pytest.mark.asyncio
@pytest.fixture
async def dict_repository(fake_repository_cls, mocker):
    return fake_repository_cls(
        memory_data_source=DictMemoryDataSource(),
        fallback_data_source=DictFallbackDataSource(),
        expire_time=1,
    )


@pytest.mark.asyncio
@pytest.fixture
async def aioredis_repository(fake_repository_cls, mocker):
    memory_data_source = await create_redis_pool(
        commands_factory=AioRedisDataSource, address='redis://',
    )
    yield fake_repository_cls(
        memory_data_source=memory_data_source,
        fallback_data_source=DictFallbackDataSource(),
        expire_time=1,
    )
    memory_data_source.close()
    await memory_data_source.wait_closed()
