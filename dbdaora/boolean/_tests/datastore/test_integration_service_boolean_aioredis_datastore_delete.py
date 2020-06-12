import asynctest
import pytest
from aioredis import RedisError

from dbdaora.exceptions import EntityNotFoundError


@pytest.mark.asyncio
async def test_should_delete(
    fake_service, serialized_fake_entity, fake_entity
):
    await fake_service.add(fake_entity)

    assert await fake_service.get_one('fake', other_id='other_fake')

    await fake_service.delete(fake_entity.id, other_id='other_fake')
    fake_service.cache.clear()

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one('fake', other_id='other_fake')


@pytest.mark.asyncio
async def test_should_delete_from_fallback_after_open_circuit_breaker(
    fake_service, serialized_fake_entity, fake_entity, mocker
):
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:not-found:other_fake:fake'
    )
    key = fake_service.repository.fallback_data_source.make_key(
        'fake', 'other_fake', 'fake'
    )
    await fake_service.repository.fallback_data_source.put(
        key, {'value': True}
    )

    assert await fake_service.get_one('fake', other_id='other_fake')

    fake_service.repository.memory_data_source.set = asynctest.CoroutineMock(
        side_effect=RedisError
    )

    await fake_service.delete(fake_entity.id, other_id='other_fake')
    fake_service.cache.clear()

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one('fake', other_id='other_fake')

    assert fake_service.logger.warning.call_count == 2
