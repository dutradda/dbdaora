import itertools

import asynctest
import pytest
from jsondaora import dataclasses

from dbdaora import HashQuery
from dbdaora.exceptions import EntityNotFoundError


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
    fake_entity.number = None
    fake_entity.boolean = False

    entity = await repository.query('fake', fields=['id', 'integer']).entity

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository, fake_entity, mocker):
    await repository.memory_data_source.delete('fake:fake')
    fake_query = HashQuery(repository, memory=True, entity_id=fake_entity.id)

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
        repository, memory=True, entity_id=fake_entity, fields=fields
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
async def test_should_get_from_fallback(repository, fake_entity):
    await repository.memory_data_source.delete('fake:fake')
    await repository.memory_data_source.delete('fake:not-found:fake')
    repository.fallback_data_source.db['fake:fake'] = dataclasses.asdict(
        fake_entity
    )
    fake_entity.number = None
    fake_entity.boolean = False
    fields = ['id', 'integer']

    entity = await repository.query('fake', fields=fields).entity

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_set_memory_after_got_fallback(
    repository, fake_entity, mocker
):
    repository.memory_data_source.hmget = asynctest.CoroutineMock(
        side_effect=[[None]]
    )
    repository.memory_data_source.hmset = asynctest.CoroutineMock()
    repository.fallback_data_source.db['fake:fake'] = dataclasses.asdict(
        fake_entity
    )
    fake_entity.number = None
    fake_entity.boolean = False

    entity = await repository.query('fake', fields=['id', 'integer']).entity

    assert repository.memory_data_source.hmget.called
    assert repository.memory_data_source.hmset.call_args_list == [
        mocker.call(
            'fake:fake',
            'id',
            'fake',
            'integer',
            1,
            'number',
            0.1,
            'boolean',
            1,
        )
    ]
    assert entity == fake_entity
