import itertools

import asynctest
import pytest
from aioredis import RedisError


@pytest.mark.asyncio
async def test_should_exists(
    fake_service, serialized_fake_entity, fake_entity, repository
):
    await repository.memory_data_source.geoadd(
        'fake:fake2:fake', *itertools.chain(*serialized_fake_entity)
    )
    assert fake_service.exists(
        fake_id=fake_entity.fake_id, fake2_id=fake_entity.fake2_id,
    )


@pytest.mark.asyncio
async def test_should_exists_from_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, mocker, fake_fallback_data_entity
):
    fake_service.repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake2:fake'
    ] = fake_fallback_data_entity

    assert await fake_service.exists(
        fake_id=fake_entity.fake_id, fake2_id=fake_entity.fake2_id,
    )
    assert fake_service.logger.warning.call_count == 1
