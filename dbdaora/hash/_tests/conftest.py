from functools import partial

import pytest
from aioredis import RedisError

from dbdaora import (
    CacheType,
    DictFallbackDataSource,
    HashService,
    build_service,
    make_aioredis_data_source,
)


@pytest.mark.asyncio
@pytest.fixture
async def fake_service(
    mocker,
    fallback_data_source,
    fake_hash_repository_cls,
    has_add_cb,
    has_delete_cb,
):
    memory_data_source_factory = partial(
        make_aioredis_data_source,
        'redis://',
        'redis://localhost/1',
        'redis://localhost/2',
    )

    async def fallback_data_source_factory():
        return fallback_data_source

    service = await build_service(
        HashService,
        fake_hash_repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time=1,
        cache_type=CacheType.TTL,
        cache_ttl=1,
        cache_max_size=2,
        cb_failure_threshold=0,
        cb_recovery_timeout=10,
        cb_expected_exception=RedisError,
        cb_expected_fallback_exception=KeyError,
        logger=mocker.MagicMock(),
        has_add_circuit_breaker=has_add_cb,
        has_delete_circuit_breaker=has_delete_cb,
    )

    yield service

    await service.shutdown()


@pytest.fixture
def fallback_data_source():
    return DictFallbackDataSource()
