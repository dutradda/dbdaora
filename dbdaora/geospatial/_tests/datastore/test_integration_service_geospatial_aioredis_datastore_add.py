import asynctest
import pytest
from aioredis import RedisError

from dbdaora import EntityNotFoundError


@pytest.fixture
def has_add_cb():
    return True


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
    fake_exists = asynctest.CoroutineMock(side_effect=[True, True])
    fake_geoadd = asynctest.CoroutineMock(side_effect=RedisError)
    fake_service.repository.memory_data_source.exists = fake_exists
    fake_service.repository.memory_data_source.geoadd = fake_geoadd
    await fake_service.add(fake_entity_add)
    await fake_service.add(fake_entity_add2)

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity.fake_id,
            fake2_id=fake_entity.fake2_id,
            latitude=5,
            longitude=6,
            max_distance=1,
        )

    assert fake_service.logger.warning.call_count == 3
