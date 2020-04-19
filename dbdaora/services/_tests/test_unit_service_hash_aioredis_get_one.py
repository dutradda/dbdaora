import itertools

import asynctest
import pytest
from aioredis import RedisError


@pytest.mark.asyncio
async def test_should_get_one(
    fake_service, serialized_fake_entity, fake_entity
):
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    entity = await fake_service.get_one('fake')

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_from_cache(
    fake_service, serialized_fake_entity, fake_entity
):
    fake_service.repository.memory_data_source.hgetall = (
        asynctest.CoroutineMock()
    )
    fake_service.cache['fake'] = fake_entity
    entity = await fake_service.get_one('fake')

    assert entity == fake_entity
    assert not fake_service.repository.memory_data_source.hgetall.called


@pytest.mark.asyncio
async def test_should_get_one_from_fallback_after_open_circuit_breaker(
    fake_service, serialized_fake_entity, fake_entity, mocker
):
    fake_service.repository.memory_data_source.hgetall = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake'
    ] = serialized_fake_entity

    entity = await fake_service.get_one('fake')

    assert entity == fake_entity
    assert fake_service.logger.warning.call_args_list == [
        mocker.call('circuit-breaker=fake')
    ]
