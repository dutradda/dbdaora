import asynctest
import pytest
from aioredis import RedisError


@pytest.mark.asyncio
async def test_should_add(fake_service, fake_entity):
    await fake_service.repository.add(fake_entity)

    entity = await fake_service.get_one('fake')

    assert entity == fake_entity.id


@pytest.mark.asyncio
async def test_should_add_to_fallback_after_open_circuit_breaker(
    fake_service, fake_entity
):
    fake_get = asynctest.CoroutineMock(side_effect=RedisError)
    fake_service.repository.memory_data_source.get = fake_get
    await fake_service.add(fake_entity)

    entity = await fake_service.get_one('fake')

    assert entity == fake_entity.id
    assert fake_service.logger.warning.call_count == 1
