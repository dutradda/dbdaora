from functools import partial

import pytest
from aioredis import RedisError

from dbdaora import make_aioredis_data_source, make_geospatial_service


@pytest.mark.asyncio
@pytest.fixture
async def fake_service(mocker, fallback_data_source, fake_repository_cls):
    memory_data_source_factory = partial(
        make_aioredis_data_source,
        'redis://',
        'redis://localhost/1',
        'redis://localhost/2',
    )

    async def fallback_data_source_factory():
        return fallback_data_source

    service = await make_geospatial_service(
        fake_repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time=1,
        cb_failure_threshold=0,
        cb_recovery_timeout=10,
        cb_expected_exception=RedisError,
        logger=mocker.MagicMock(),
    )

    yield service

    await service.shutdown()
