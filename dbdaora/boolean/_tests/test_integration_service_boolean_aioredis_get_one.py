import asynctest
import pytest
from aioredis import RedisError


@pytest.mark.asyncio
async def test_should_get_one(
    fake_service, serialized_fake_entity, fake_entity
):
    await fake_service.repository.memory_data_source.set(
        'fake:fake', serialized_fake_entity
    )
    entity = await fake_service.get_one('fake')

    assert entity == fake_entity.id


@pytest.mark.asyncio
async def test_should_get_one_without_cache(
    fake_service, serialized_fake_entity, fake_entity
):
    fake_service.cache = None
    await fake_service.repository.memory_data_source.set(
        'fake:fake', serialized_fake_entity
    )
    entity = await fake_service.get_one('fake')

    assert entity == fake_entity.id


@pytest.mark.asyncio
async def test_should_get_one_from_cache(
    fake_service, serialized_fake_entity, fake_entity
):
    fake_service.repository.memory_data_source.exists = (
        asynctest.CoroutineMock()
    )
    fake_service.cache['fake'] = fake_entity.id
    entity = await fake_service.get_one('fake')

    assert entity == fake_entity.id
    assert not fake_service.repository.memory_data_source.exists.called


@pytest.mark.asyncio
async def test_should_get_one_from_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, mocker
):
    fake_service.repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db['fake:fake'] = {
        'value': True
    }

    entity = await fake_service.get_one('fake')

    assert entity == fake_entity.id
    assert fake_service.logger.warning.call_count == 1


@pytest.mark.asyncio
async def test_should_get_one_from_fallback_after_open_circuit_breaker_without_cache(
    fake_service, fake_entity, mocker
):
    fake_service.cache = None
    fake_service.repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db['fake:fake'] = {
        'value': True
    }

    entity = await fake_service.get_one('fake')

    assert entity == fake_entity.id
    assert fake_service.logger.warning.call_count == 1
