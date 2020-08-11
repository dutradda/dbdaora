import asynctest
import pytest
from aioredis import RedisError
from circuitbreaker import CircuitBreakerError
from pymongo.errors import PyMongoError


@pytest.mark.asyncio
async def test_should_add(
    fake_service, fake_entity, fake_entity_add, fake_entity_add2
):
    await fake_service.add(fake_entity_add)
    await fake_service.add(fake_entity_add2)

    entity = await fake_service.get_one(
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=5,
        longitude=6,
        max_distance=1,
    )

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_add_to_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, fake_entity_add, fake_entity_add2
):
    fake_geoadd = asynctest.CoroutineMock(side_effect=RedisError)
    fake_service.repository.memory_data_source.geoadd = fake_geoadd
    await fake_service.add(fake_entity_add)
    await fake_service.add(fake_entity_add2)

    entity = await fake_service.get_one(
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=5,
        longitude=6,
        max_distance=1,
    )

    assert entity == fake_entity
    assert fake_service.logger.warning.call_count == 2


@pytest.mark.asyncio
async def test_should_add_to_fallback_after_open_fallback_circuit_breaker(
    fake_service, fake_entity, fake_entity_add, fake_entity_add2
):
    fake_put = asynctest.CoroutineMock(side_effect=PyMongoError)
    fake_service.repository.fallback_data_source.put = fake_put

    with pytest.raises(CircuitBreakerError):
        await fake_service.add(fake_entity_add, memory=False)

    assert fake_service.logger.warning.call_count == 1
