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
    fake_service, serialized_fake_entity, fake_entity, mocker
):
    await fake_service.repository.memory_data_source.delete('fake:fake')
    await fake_service.repository.memory_data_source.delete(
        'fake:not-found:fake'
    )
    key = fake_service.repository.fallback_data_source.make_key('fake', 'fake')
    await fake_service.repository.fallback_data_source.put(
        key, dataclasses.asdict(fake_entity)
    )

    assert await fake_service.get_one('fake')

    fake_service.repository.memory_data_source.delete = asynctest.CoroutineMock(
        side_effect=RedisError
    )

    await fake_service.delete(fake_entity.id)

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one('fake')

    assert fake_service.logger.warning.call_args_list == [
        mocker.call('circuit-breaker=fake; method=delete'),
        mocker.call('circuit-breaker=fake; method=get_one'),
    ]
