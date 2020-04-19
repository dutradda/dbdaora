import itertools

import asynctest
import pytest

from dbdaora.exceptions import EntityNotFoundError
from dbdaora.repositories.hash.query import HashQuery


@pytest.fixture
def repository(dict_repository):
    return dict_repository


@pytest.mark.asyncio
async def test_should_get_from_memory(
    repository, serialized_fake_entity, fake_entity
):
    await repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    fake_entity.number = None
    fake_entity.boolean = False

    entity = await repository.query('fake', fields=['id', 'integer']).entity

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository, fake_entity, mocker):
    fake_query = HashQuery(repository, fake_entity.id)

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity.id).entity

    assert exc_info.value.args == (fake_query,)


@pytest.mark.asyncio
async def test_should_raise_not_found_error_when_already_raised_before(
    repository, mocker
):
    fake_entity = 'fake'
    fields = ['id', 'integer']
    expected_query = HashQuery(
        repository, entity_id=fake_entity, fields=fields
    )
    repository.memory_data_source.hmget = asynctest.CoroutineMock(
        side_effect=[[None]]
    )
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[True]
    )
    repository.memory_data_source.hmset = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query('fake', fields=fields).entity

    assert exc_info.value.args == (expected_query,)
    assert repository.memory_data_source.hmget.call_args_list == [
        mocker.call('fake:fake', *fields),
    ]
    assert repository.memory_data_source.exists.call_args_list == [
        mocker.call('fake:not-found:fake')
    ]
    assert not repository.memory_data_source.hmset.called


@pytest.mark.asyncio
async def test_should_get_from_fallback(
    repository, serialized_fake_entity, fake_entity
):
    repository.memory_data_source.hmget = asynctest.CoroutineMock(
        side_effect=[[None]]
    )
    fields = ['id', 'integer']
    repository.fallback_data_source.db['fake:fake'] = serialized_fake_entity
    fake_entity.number = None
    fake_entity.boolean = False

    entity = await repository.query('fake', fields=fields).entity

    assert repository.memory_data_source.hmget.called
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_set_memory_after_got_fallback(
    repository, serialized_fake_entity, fake_entity, mocker
):
    repository.memory_data_source.hmget = asynctest.CoroutineMock(
        side_effect=[[None]]
    )
    repository.memory_data_source.hmset = asynctest.CoroutineMock()
    repository.fallback_data_source.db['fake:fake'] = serialized_fake_entity
    fake_entity.number = None
    fake_entity.boolean = False

    entity = await repository.query('fake', fields=['id', 'integer']).entity

    assert repository.memory_data_source.hmget.called
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
