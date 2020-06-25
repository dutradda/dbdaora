import itertools

import asynctest
import pytest
from aioredis import RedisError
from jsondaora import dataclasses

from dbdaora.data_sources.fallback.mongodb import Key


@pytest.mark.asyncio
async def test_should_get_many(
    fake_service,
    serialized_fake_entity,
    fake_entity,
    serialized_fake_entity2,
    fake_entity2,
):
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake2'
    )
    await fake_service.repository.memory_data_source.hmset(
        'fake:other_fake:fake',
        *itertools.chain(*serialized_fake_entity.items()),
    )
    await fake_service.repository.memory_data_source.hmset(
        'fake:other_fake:fake2',
        *itertools.chain(*serialized_fake_entity2.items()),
    )
    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', other_id='other_fake'
        )
    ]

    assert entities == [fake_entity, fake_entity2]


@pytest.mark.asyncio
async def test_should_get_many_from_cache(
    fake_service, serialized_fake_entity, fake_entity, fake_entity2
):
    fake_service.repository.memory_data_source.hgetall = (
        asynctest.CoroutineMock()
    )
    fake_service.cache['fakeother_idother_fake'] = fake_entity
    fake_service.cache['fake2other_idother_fake'] = fake_entity2
    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', other_id='other_fake'
        )
    ]

    assert entities == [fake_entity, fake_entity2]
    assert not fake_service.repository.memory_data_source.hgetall.called


@pytest.mark.asyncio
async def test_should_get_many_from_fallback_when_not_found_on_memory(
    fake_service, serialized_fake_entity, fake_entity, fake_entity2
):
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake2'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:not-found:other_fake:fake'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:not-found:other_fake:fake2'
    )
    await fake_service.repository.fallback_data_source.put(
        Key('fake', 'other_fake:fake'), dataclasses.asdict(fake_entity)
    )
    await fake_service.repository.fallback_data_source.put(
        Key('fake', 'other_fake:fake2'), dataclasses.asdict(fake_entity2),
    )

    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', other_id='other_fake'
        )
    ]

    assert entities == [fake_entity, fake_entity2]
    assert fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake'
    )
    assert fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake2'
    )


@pytest.mark.asyncio
async def test_should_get_many_from_fallback_when_not_found_on_memory_with_fields(
    fake_service, serialized_fake_entity, fake_entity, fake_entity2
):
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake2'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:not-found:other_fake:fake'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:not-found:other_fake:fake2'
    )
    await fake_service.repository.fallback_data_source.put(
        Key('fake', 'other_fake:fake'), dataclasses.asdict(fake_entity)
    )
    await fake_service.repository.fallback_data_source.put(
        Key('fake', 'other_fake:fake2'), dataclasses.asdict(fake_entity2),
    )
    fake_entity.number = None
    fake_entity.boolean = False
    fake_entity2.number = None
    fake_entity2.boolean = False

    entities = [
        e
        async for e in fake_service.get_many(
            'fake',
            'fake2',
            other_id='other_fake',
            fields=['id', 'other_id', 'integer', 'inner_entities'],
        )
    ]

    assert entities == [fake_entity, fake_entity2]
    assert fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake'
    )
    assert fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake2'
    )


@pytest.mark.asyncio
async def test_should_get_many_from_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, fake_entity2, mocker
):
    fake_service.repository.memory_data_source.hgetall = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    key = fake_service.repository.fallback_data_source.make_key(
        'fake', 'other_fake', 'fake'
    )
    await fake_service.repository.fallback_data_source.put(
        key, dataclasses.asdict(fake_entity)
    )
    key = fake_service.repository.fallback_data_source.make_key(
        'fake', 'other_fake', 'fake2'
    )
    await fake_service.repository.fallback_data_source.put(
        key, dataclasses.asdict(fake_entity2)
    )

    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', other_id='other_fake'
        )
    ]

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
        'fake:other_fake:fake',
        *itertools.chain(*serialized_fake_entity.items()),
    )
    await fake_service.repository.memory_data_source.hmset(
        'fake:other_fake:fake2',
        *itertools.chain(*serialized_fake_entity2.items()),
    )
    entities = [
        e
        async for e in fake_service.get_many(
            'fake',
            'fake2',
            fields=['id', 'other_id', 'integer', 'inner_entities'],
            other_id='other_fake',
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
    fake_service.cache[
        'fakeidother_idintegerinner_entitiesother_idother_fake'
    ] = fake_entity
    fake_service.cache[
        'fake2idother_idintegerinner_entitiesother_idother_fake'
    ] = fake_entity2
    entities = [
        e
        async for e in fake_service.get_many(
            'fake',
            'fake2',
            fields=['id', 'other_id', 'integer', 'inner_entities'],
            other_id='other_fake',
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
    key = fake_service.repository.fallback_data_source.make_key('fake', 'fake')
    await fake_service.repository.fallback_data_source.put(
        key, dataclasses.asdict(fake_entity)
    )
    key = fake_service.repository.fallback_data_source.make_key(
        'fake', 'fake2'
    )
    await fake_service.repository.fallback_data_source.put(
        key, dataclasses.asdict(fake_entity2)
    )
    fake_entity.number = None
    fake_entity.boolean = False
    fake_entity2.number = None
    fake_entity2.boolean = False

    entities = [
        e
        async for e in fake_service.get_many(
            'fake',
            'fake2',
            fields=['id', 'other_id', 'integer', 'inner_entities'],
            other_id='other_fake',
        )
    ]

    assert entities == [fake_entity, fake_entity2]
    assert fake_service.logger.warning.call_count == 2


@pytest.mark.asyncio
async def test_should_get_many_from_cache_memory_and_fallback(
    fake_service, fake_entity, fake_entity2, fake_entity3
):
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake2'
    )
    await fake_service.repository.memory_data_source.delete(
        'fake:other_fake:fake3'
    )
    await fake_service.add(fake_entity)
    await fake_service.add(fake_entity2)
    await fake_service.add(fake_entity3)

    assert not await fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake'
    )
    assert not await fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake2'
    )
    assert not await fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake3'
    )

    entities = [
        e async for e in fake_service.get_many('fake', other_id='other_fake')
    ]
    assert entities == [fake_entity]

    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', other_id='other_fake'
        )
    ]
    assert entities == [fake_entity, fake_entity2]

    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', 'fake3', other_id='other_fake'
        )
    ]

    assert entities == [fake_entity, fake_entity2, fake_entity3]

    assert await fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake'
    )
    assert await fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake2'
    )
    assert await fake_service.repository.memory_data_source.exists(
        'fake:other_fake:fake3'
    )


@pytest.mark.asyncio
async def test_should_get_many_from_cache_when_not_found_some_entities(
    fake_service, fake_entity
):
    await fake_service.delete('fake', other_id='other_fake')
    await fake_service.delete('fake2', other_id='other_fake')
    await fake_service.delete('fake3', other_id='other_fake')
    await fake_service.add(fake_entity)

    entities = [
        e async for e in fake_service.get_many('fake', other_id='other_fake')
    ]
    assert entities == [fake_entity]

    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', 'fake3', other_id='other_fake'
        )
    ]
    assert entities == [fake_entity]


@pytest.mark.asyncio
async def test_should_raise_entity_not_found_error_when_get_many(fake_service):
    await fake_service.delete('fake', other_id='other_fake')
    await fake_service.delete('fake2', other_id='other_fake')
    await fake_service.delete('fake3', other_id='other_fake')

    entities = [
        e
        async for e in fake_service.get_many(
            'fake', 'fake2', 'fake3', other_id='other_fake'
        )
    ]

    assert not entities
