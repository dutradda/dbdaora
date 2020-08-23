import asynctest
import pytest
from aioredis import RedisError
from circuitbreaker import CircuitBreakerError


@pytest.mark.asyncio
async def test_should_add(fake_service, fake_entity, fake_entity_withscores):
    print(fake_service.repository.fallback_data_source)
    await fake_service.add(fake_entity_withscores)

    entity = await fake_service.get_one(fake_id=fake_entity_withscores.fake_id)

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_add_to_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, fake_entity_withscores
):
    fake_exists = asynctest.CoroutineMock(side_effect=RedisError)
    fake_service.repository.memory_data_source.exists = fake_exists
    fake_zadd = asynctest.CoroutineMock(side_effect=RedisError)
    fake_service.repository.memory_data_source.zadd = fake_zadd
    await fake_service.add(fake_entity_withscores)

    assert fake_service.logger.warning.call_count == 1
    fake_service.logger.warning.reset_mock()

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, memory=False
    )

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_add_to_fallback_after_open_fallback_circuit_breaker(
    fake_service, fake_entity_withscores, fallback_cb_error
):
    fake_put = asynctest.CoroutineMock(side_effect=fallback_cb_error)
    fake_service.repository.fallback_data_source.put = fake_put

    with pytest.raises(CircuitBreakerError):
        await fake_service.add(fake_entity_withscores, memory=False)

    assert fake_service.logger.warning.call_count == 1
