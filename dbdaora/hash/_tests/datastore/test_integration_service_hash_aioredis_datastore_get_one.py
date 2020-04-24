import itertools

import asynctest
import pytest
from aioredis import RedisError
from jsondaora import dataclasses


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
async def test_should_get_one_with_fields(
    fake_service, serialized_fake_entity, fake_entity
):
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    fake_entity.number = None
    fake_entity.boolean = False

    entity = await fake_service.get_one(
        'fake', fields=['id', 'integer', 'inner_entities']
    )

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
    fake_service, fake_entity, mocker
):
    fake_service.repository.memory_data_source.hgetall = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    key = fake_service.repository.fallback_data_source.make_key('fake', 'fake')
    await fake_service.repository.fallback_data_source.put(
        key, dataclasses.asdict(fake_entity)
    )

    entity = await fake_service.get_one('fake')

    assert entity == fake_entity
    assert fake_service.logger.warning.call_count == 1
