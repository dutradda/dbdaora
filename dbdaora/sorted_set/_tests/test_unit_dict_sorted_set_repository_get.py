import asynctest
import pytest

from dbdaora import SortedSetQuery
from dbdaora.exceptions import EntityNotFoundError


@pytest.fixture
def repository(dict_repository):
    return dict_repository


@pytest.mark.asyncio
async def test_should_get_from_memory(
    repository, fake_entity, fake_entity_withscores
):
    await repository.memory_data_source.zadd('fake:fake', 0, '1', 2, '2')
    entity = await repository.query(fake_entity.id).entity

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository, fake_entity, mocker):
    fake_query = SortedSetQuery(repository, memory=True, id=fake_entity.id)

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity.id).entity

    assert exc_info.value.args == (fake_query,)


@pytest.mark.asyncio
async def test_should_raise_not_found_error_when_already_raised_before(
    repository, mocker
):
    fake_entity = 'fake'
    expected_query = SortedSetQuery(repository, memory=True, id=fake_entity)
    repository.memory_data_source.zrange = asynctest.CoroutineMock(
        side_effect=[None]
    )
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[True]
    )
    repository.memory_data_source.zadd = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity).entity

    assert exc_info.value.args == (expected_query,)
    assert repository.memory_data_source.zrange.call_args_list == [
        mocker.call('fake:fake'),
    ]
    assert repository.memory_data_source.exists.call_args_list == [
        mocker.call('fake:not-found:fake')
    ]
    assert not repository.memory_data_source.zadd.called


@pytest.mark.asyncio
async def test_should_get_from_fallback(
    repository, fake_entity_withscores, fake_entity
):
    repository.memory_data_source.zrange = asynctest.CoroutineMock(
        side_effect=[None, fake_entity.values]
    )
    repository.fallback_data_source.db['fake:fake'] = {
        'values': fake_entity_withscores.values
    }
    entity = await repository.query(fake_entity.id).entity

    assert repository.memory_data_source.zrange.called
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_set_memory_after_got_fallback(
    repository, fake_entity_withscores, fake_entity, mocker
):
    repository.memory_data_source.zrange = asynctest.CoroutineMock(
        side_effect=[None, fake_entity.values]
    )
    repository.memory_data_source.zadd = asynctest.CoroutineMock()
    repository.fallback_data_source.db['fake:fake'] = {
        'values': fake_entity_withscores.values
    }
    entity = await repository.query(fake_entity.id).entity

    assert repository.memory_data_source.zrange.called
    assert repository.memory_data_source.zadd.call_args_list == [
        mocker.call('fake:fake', 1, '2', 0, '1')
    ]
    assert entity == fake_entity
