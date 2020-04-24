import asynctest
import pytest
from aioredis import RedisError
from jsondaora import dataclasses

from dbdaora.exceptions import EntityNotFoundError


@pytest.mark.asyncio
async def test_should_delete(
    fake_service, serialized_fake_entity, fake_entity
):
    await fake_service.add(fake_entity)

    assert await fake_service.get_one('fake')

    await fake_service.delete(fake_entity.id)

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one('fake')


@pytest.mark.asyncio
async def test_should_delete_from_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, mocker
):
    await fake_service.repository.memory_data_source.delete('fake:fake')
    await fake_service.repository.memory_data_source.delete(
        'fake:not-found:fake'
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake'
    ] = dataclasses.asdict(fake_entity, dumps_value=True)

    assert await fake_service.get_one('fake')

    fake_service.repository.memory_data_source.delete = asynctest.CoroutineMock(
        side_effect=RedisError
    )

    await fake_service.delete(fake_entity.id)

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one('fake')

    assert fake_service.logger.warning.call_count == 2
