import asynctest
import pytest
from aioredis import RedisError


@pytest.mark.asyncio
async def test_should_add(fake_service, fake_entity):
    await fake_service.repository.add(fake_entity)

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
    fake_service, fake_entity
):
    fake_geoadd = asynctest.CoroutineMock(side_effect=RedisError)
    fake_service.repository.memory_data_source.geoadd = fake_geoadd
    await fake_service.add(fake_entity)

    entity = await fake_service.get_one(
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=5,
        longitude=6,
        max_distance=1,
    )

    assert entity == fake_entity
    assert fake_service.logger.warning.call_count == 1
