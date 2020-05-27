import asynctest
import pytest
from aioredis import RedisError

from dbdaora import EntityNotFoundError


@pytest.mark.asyncio
async def test_should_get_many(
    fake_service,
    serialized_fake_entity,
    fake_entity,
    serialized_fake_entity2,
    fake_entity2,
):
    await fake_service.repository.memory_data_source.set(
        'fake:other_fake:fake', serialized_fake_entity,
    )
    await fake_service.repository.memory_data_source.set(
        'fake:other_fake:fake2', serialized_fake_entity2,
    )
    entities = await fake_service.get_many(
        'fake', 'fake2', other_id='other_fake'
    )

    assert entities == [fake_entity.id, fake_entity2.id]


@pytest.mark.asyncio
async def test_should_get_many_from_cache(
    fake_service, serialized_fake_entity, fake_entity, fake_entity2
):
    fake_service.repository.memory_data_source.exists = (
        asynctest.CoroutineMock()
    )
    fake_service.cache['fakeother_idother_fake'] = fake_entity.id
    fake_service.cache['fake2other_idother_fake'] = fake_entity2.id
    entities = await fake_service.get_many(
        'fake', 'fake2', other_id='other_fake'
    )

    assert entities == [fake_entity.id, fake_entity2.id]
    assert not fake_service.repository.memory_data_source.exists.called


@pytest.mark.asyncio
async def test_should_get_many_from_fallback_when_not_found_on_memory(
    fake_service, serialized_fake_entity, fake_entity, fake_entity2
):
    client = fake_service.repository.fallback_data_source.client
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
        client.key('fake', 'other_fake:fake'), {'value': True}
    )
    await fake_service.repository.fallback_data_source.put(
        client.key('fake', 'other_fake:fake2'), {'value': True},
    )

    entities = await fake_service.get_many(
        'fake', 'fake2', other_id='other_fake'
    )

    assert entities == [fake_entity.id, fake_entity2.id]
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
    fake_service.repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    key = fake_service.repository.fallback_data_source.make_key(
        'fake', 'other_fake', 'fake'
    )
    await fake_service.repository.fallback_data_source.put(
        key, {'value': True}
    )
    key = fake_service.repository.fallback_data_source.make_key(
        'fake', 'other_fake', 'fake2'
    )
    await fake_service.repository.fallback_data_source.put(
        key, {'value': True}
    )

    entities = await fake_service.get_many(
        'fake', 'fake2', other_id='other_fake'
    )

    assert entities == [fake_entity.id, fake_entity2.id]
    assert fake_service.logger.warning.call_count == 1


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

    entities = await fake_service.get_many('fake', other_id='other_fake')
    assert entities == [fake_entity.id]

    entities = await fake_service.get_many(
        'fake', 'fake2', other_id='other_fake'
    )
    assert entities == [fake_entity.id, fake_entity2.id]

    entities = await fake_service.get_many(
        'fake', 'fake2', 'fake3', other_id='other_fake'
    )

    assert entities == [fake_entity.id, fake_entity2.id, fake_entity3.id]

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

    entities = await fake_service.get_many('fake', other_id='other_fake')
    assert entities == [fake_entity.id]

    entities = await fake_service.get_many(
        'fake', 'fake2', 'fake3', other_id='other_fake'
    )
    assert entities == [fake_entity.id]


@pytest.mark.asyncio
async def test_should_raise_entity_not_found_error_when_get_many(fake_service):
    await fake_service.delete('fake', other_id='other_fake')
    await fake_service.delete('fake2', other_id='other_fake')
    await fake_service.delete('fake3', other_id='other_fake')

    with pytest.raises(EntityNotFoundError) as exc_info:
        await fake_service.get_many(
            'fake', 'fake2', 'fake3', other_id='other_fake'
        )

    assert exc_info.value.args == (
        ('fake', 'fake2', 'fake3'),
        None,
        {'other_id': 'other_fake'},
    )
