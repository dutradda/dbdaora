import itertools

import asynctest
import pytest
from aioredis import RedisError
from jsondaora import dataclasses


@pytest.mark.asyncio
async def test_should_get_many(
    fake_service,
    serialized_fake_entity,
    fake_entity,
    serialized_fake_entity2,
    fake_entity2,
):
    await fake_service.repository.memory_data_source.delete('fake:fake')
    await fake_service.repository.memory_data_source.delete('fake:fake2')
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake2', *itertools.chain(*serialized_fake_entity2.items())
    )
    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity, fake_entity2]


@pytest.mark.asyncio
async def test_should_get_many_without_cache(
    fake_service,
    serialized_fake_entity,
    fake_entity,
    serialized_fake_entity2,
    fake_entity2,
):
    fake_service.cache = None
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake2', *itertools.chain(*serialized_fake_entity2.items())
    )
    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity, fake_entity2]


@pytest.mark.asyncio
async def test_should_get_many_from_cache(
    fake_service, serialized_fake_entity, fake_entity, fake_entity2
):
    fake_service.repository.memory_data_source.hgetall = (
        asynctest.CoroutineMock()
    )
    fake_service.cache['fake'] = fake_entity
    fake_service.cache['fake2'] = fake_entity2
    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity, fake_entity2]
    assert not fake_service.repository.memory_data_source.hgetall.called


@pytest.mark.asyncio
async def test_should_get_many_from_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, fake_entity2, mocker
):
    fake_service.repository.memory_data_source.hgetall = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake'
    ] = dataclasses.asdict(fake_entity)
    fake_service.repository.fallback_data_source.db[
        'fake:fake2'
    ] = dataclasses.asdict(fake_entity2)

    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity, fake_entity2]
    assert fake_service.logger.warning.call_count == 2


@pytest.mark.asyncio
async def test_should_get_many_from_fallback_after_open_circuit_breaker_without_cache(
    fake_service, fake_entity, fake_entity2, mocker
):
    fake_service.cache = None
    fake_service.repository.memory_data_source.hgetall = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake'
    ] = dataclasses.asdict(fake_entity)
    fake_service.repository.fallback_data_source.db[
        'fake:fake2'
    ] = dataclasses.asdict(fake_entity2)

    entities = [e async for e in fake_service.get_many('fake', 'fake2')]

    assert entities == [fake_entity, fake_entity2]
    assert fake_service.logger.warning.call_count == 2


@pytest.mark.asyncio
async def test_should_get_many_with_fields(
    fake_service,
    serialized_fake_entity,
    fake_entity,
    serialized_fake_entity2,
    fake_entity2,
):
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake2', *itertools.chain(*serialized_fake_entity2.items())
    )
    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', fields=['id', 'integer', 'inner_entities']
        )
    ]
    fake_entity.number = None
    fake_entity.boolean = False
    fake_entity2.number = None
    fake_entity2.boolean = False

    assert entities == [fake_entity, fake_entity2]


@pytest.mark.asyncio
async def test_should_get_many_from_cache_with_fields(
    fake_service, serialized_fake_entity, fake_entity, fake_entity2
):
    fake_service.repository.memory_data_source.hgetall = (
        asynctest.CoroutineMock()
    )
    fake_service.cache['fakeidintegerinner_entities'] = fake_entity
    fake_service.cache['fake2idintegerinner_entities'] = fake_entity2
    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', fields=['id', 'integer', 'inner_entities']
        )
    ]
    fake_entity.number = None
    fake_entity.boolean = False
    fake_entity2.number = None
    fake_entity2.boolean = False

    assert entities == [fake_entity, fake_entity2]
    assert not fake_service.repository.memory_data_source.hgetall.called


@pytest.mark.asyncio
async def test_should_get_many_from_fallback_after_open_circuit_breaker_with_fields(
    fake_service, fake_entity, fake_entity2, mocker
):
    fake_service.repository.memory_data_source.hmget = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake'
    ] = dataclasses.asdict(fake_entity)
    fake_service.repository.fallback_data_source.db[
        'fake:fake2'
    ] = dataclasses.asdict(fake_entity2)
    fake_entity.number = None
    fake_entity.boolean = False
    fake_entity2.number = None
    fake_entity2.boolean = False

    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', fields=['id', 'integer', 'inner_entities']
        )
    ]

    assert entities == [fake_entity, fake_entity2]
    assert fake_service.logger.warning.call_count == 2
