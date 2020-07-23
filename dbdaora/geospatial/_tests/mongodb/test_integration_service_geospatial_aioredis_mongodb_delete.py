import asynctest
import pytest
from aioredis import RedisError

from dbdaora.exceptions import EntityNotFoundError


@pytest.mark.asyncio
async def test_should_delete(
    fake_service, serialized_fake_entity, fake_entity
):
    await fake_service.repository.memory_data_source.delete('fake:fake2:fake')
    await fake_service.add(fake_entity)

    assert await fake_service.get_one(
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=5,
        longitude=6,
        max_distance=1,
    )

    await fake_service.delete(
        fake_id=fake_entity.fake_id, fake2_id=fake_entity.fake2_id,
    )

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity.fake_id,
            fake2_id=fake_entity.fake2_id,
            latitude=5,
            longitude=6,
            max_distance=1,
        )


@pytest.mark.asyncio
async def test_should_delete_from_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, fake_fallback_data_entity, repository
):
    await fake_service.repository.memory_data_source.delete('fake:fake2:fake')
    key = fake_service.repository.fallback_data_source.make_key(
        'fake', 'fake2', 'fake'
    )
    await fake_service.repository.fallback_data_source.put(
        key, fake_fallback_data_entity
    )

    assert await fake_service.get_one(
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=5,
        longitude=6,
        max_distance=1,
    )

    fake_service.repository.memory_data_source.delete = asynctest.CoroutineMock(
        side_effect=RedisError
    )

    await fake_service.delete(
        fake_id=fake_entity.fake_id, fake2_id=fake_entity.fake2_id,
    )

    assert fake_service.logger.warning.call_count == 1
