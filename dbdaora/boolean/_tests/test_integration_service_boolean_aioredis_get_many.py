import asynctest
import pytest
from aioredis import RedisError


@pytest.mark.asyncio
async def test_should_get_many(
    fake_service,
    serialized_fake_entity,
    fake_entity,
    serialized_fake_entity2,
    fake_entity2,
):
    await fake_service.repository.memory_data_source.set(
        'fake:fake', serialized_fake_entity
    )
    await fake_service.repository.memory_data_source.set(
        'fake:fake2', serialized_fake_entity2
    )
    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity.id, fake_entity2.id]


@pytest.mark.asyncio
async def test_should_get_many_without_cache(
    fake_service,
    serialized_fake_entity,
    fake_entity,
    serialized_fake_entity2,
    fake_entity2,
):
    fake_service.cache = None
    await fake_service.repository.memory_data_source.set(
        'fake:fake', serialized_fake_entity
    )
    await fake_service.repository.memory_data_source.set(
        'fake:fake2', serialized_fake_entity2
    )
    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity.id, fake_entity2.id]


@pytest.mark.asyncio
async def test_should_get_many_from_cache(
    fake_service, serialized_fake_entity, fake_entity, fake_entity2
):
    fake_service.repository.memory_data_source.get = asynctest.CoroutineMock()
    fake_service.cache['fake'] = fake_entity.id
    fake_service.cache['fake2'] = fake_entity2.id
    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity.id, fake_entity2.id]
    assert not fake_service.repository.memory_data_source.get.called


@pytest.mark.asyncio
async def test_should_get_many_from_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, fake_entity2, mocker
):
    fake_service.repository.memory_data_source.get = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db['fake:fake'] = {
        'value': True
    }
    fake_service.repository.fallback_data_source.db['fake:fake2'] = {
        'value': True
    }

    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity.id, fake_entity2.id]
    assert fake_service.logger.warning.call_count == 2


@pytest.mark.asyncio
async def test_should_get_many_from_fallback_after_open_circuit_breaker_without_cache(
    fake_service, fake_entity, fake_entity2, mocker
):
    fake_service.cache = None
    fake_service.repository.memory_data_source.get = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db['fake:fake'] = {
        'value': True
    }
    fake_service.repository.fallback_data_source.db['fake:fake2'] = {
        'value': True
    }

    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity.id, fake_entity2.id]
    assert fake_service.logger.warning.call_count == 1
