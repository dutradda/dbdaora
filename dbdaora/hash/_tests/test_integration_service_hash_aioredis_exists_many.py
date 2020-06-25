import itertools

import asynctest
import pytest
from aioredis import RedisError
from jsondaora import dataclasses


@pytest.mark.asyncio
async def test_should_exists(
    fake_service, serialized_fake_entity, serialized_fake_entity2,
):
    await fake_service.repository.memory_data_source.delete('fake:fake')
    await fake_service.repository.memory_data_source.delete('fake:fake2')
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake2', *itertools.chain(*serialized_fake_entity2.items())
    )
    entities = [e async for e in fake_service.exists_many('fake', 'fake2')]

    assert entities == [True, True]


@pytest.mark.asyncio
async def test_should_exists_without_cache(
    fake_service, serialized_fake_entity, serialized_fake_entity2,
):
    fake_service.cache = None
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake2', *itertools.chain(*serialized_fake_entity2.items())
    )
    entities = [e async for e in fake_service.exists_many('fake', 'fake2')]

    assert entities == [True, True]


@pytest.mark.asyncio
async def test_should_exists_from_cache(
    fake_service, serialized_fake_entity, fake_entity, fake_entity2
):
    fake_service.repository.memory_data_source.exists = (
        asynctest.CoroutineMock()
    )
    fake_service.exists_cache['fake'] = True
    fake_service.exists_cache['fake2'] = True
    entities = [e async for e in fake_service.exists_many('fake', 'fake2')]

    assert entities == [True, True]
    assert not fake_service.repository.memory_data_source.exists.called


@pytest.mark.asyncio
async def test_should_exists_from_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, fake_entity2, mocker
):
    fake_service.repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake'
    ] = dataclasses.asdict(fake_entity)
    fake_service.repository.fallback_data_source.db[
        'fake:fake2'
    ] = dataclasses.asdict(fake_entity2)

    entities = [e async for e in fake_service.exists_many('fake', 'fake2')]

    assert entities == [True, True]
    assert fake_service.logger.warning.call_count == 2


@pytest.mark.asyncio
async def test_should_exists_from_fallback_after_open_circuit_breaker_without_cache(
    fake_service, fake_entity, fake_entity2, mocker
):
    fake_service.cache = None
    fake_service.repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake'
    ] = dataclasses.asdict(fake_entity)
    fake_service.repository.fallback_data_source.db[
        'fake:fake2'
    ] = dataclasses.asdict(fake_entity2)

    entities = [e async for e in fake_service.exists_many('fake', 'fake2')]

    assert entities == [True, True]
    assert fake_service.logger.warning.call_count == 2
