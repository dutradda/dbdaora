import itertools

import asynctest
import pytest

from dbdaora.exceptions import EntityNotFoundError
from dbdaora.repositories.hash.query import HashQuery


@pytest.fixture
def repository(aioredis_repository):
    return aioredis_repository


@pytest.mark.asyncio
async def test_should_get_from_memory(
    repository, serialized_fake_entity, fake_entity
):
    await repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    entity = await repository.query('fake').get()

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository, fake_entity, mocker):
    await repository.memory_data_source.delete('fake:fake')
    fake_query = HashQuery(repository, fake_entity.id)

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity.id).get()

    assert exc_info.value.args == (fake_query,)


@pytest.mark.asyncio
async def test_should_raise_not_found_error_when_already_raised_before(
    repository, mocker
):
    fake_entity = 'fake'
    expected_query = HashQuery(repository, entity_id=fake_entity)
    repository.memory_data_source.hgetall = asynctest.CoroutineMock(
        side_effect=[None]
    )
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[True]
    )
    repository.memory_data_source.hmset = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity).get()

    assert exc_info.value.args == (expected_query,)
    assert repository.memory_data_source.hgetall.call_args_list == [
        mocker.call('fake:fake'),
    ]
    assert repository.memory_data_source.exists.call_args_list == [
        mocker.call('fake:not-found:fake')
    ]
    assert not repository.memory_data_source.hmset.called


@pytest.mark.asyncio
async def test_should_get_from_fallback(
    repository, serialized_fake_entity, fake_entity
):
    await repository.memory_data_source.delete('fake:fake')
    await repository.memory_data_source.delete('fake:not-found:fake')
    repository.fallback_data_source.db['fake:fake'] = serialized_fake_entity
    entity = await repository.query(fake_entity.id).get()

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_set_memory_after_got_fallback(
    repository, serialized_fake_entity, fake_entity, mocker
):
    repository.memory_data_source.hgetall = asynctest.CoroutineMock(
        side_effect=[None]
    )
    repository.memory_data_source.hmset = asynctest.CoroutineMock()
    repository.fallback_data_source.db['fake:fake'] = serialized_fake_entity
    entity = await repository.query(fake_entity.id).get()

    assert repository.memory_data_source.hgetall.called
    assert repository.memory_data_source.hmset.call_args_list == [
        mocker.call(
            'fake:fake',
            b'id',
            b'fake',
            b'integer',
            b'1',
            b'number',
            b'0.1',
            b'boolean',
            b'1',
        )
    ]
    assert entity == fake_entity