import asynctest
import pytest

from dbdaora import Query
from dbdaora.exceptions import EntityNotFoundError


@pytest.mark.asyncio
async def test_should_get_from_memory(
    repository, serialized_fake_entity, fake_entity
):
    await repository.memory_data_source.set(
        'fake:fake', serialized_fake_entity
    )
    entity = await repository.query('fake').entity

    assert entity == fake_entity.id


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository, fake_entity, mocker):
    await repository.memory_data_source.delete('fake:fake')
    fake_query = Query(repository, memory=True, id=fake_entity.id)

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity.id).entity

    assert exc_info.value.args == (fake_query,)


@pytest.mark.asyncio
async def test_should_raise_not_found_error_when_already_raised_before(
    repository, mocker
):
    fake_entity = 'fake'
    expected_query = Query(repository, memory=True, id=fake_entity)
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[False, True]
    )
    repository.memory_data_source.set = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity).entity

    assert exc_info.value.args == (expected_query,)
    assert repository.memory_data_source.exists.call_args_list == [
        mocker.call('fake:fake'),
        mocker.call('fake:not-found:fake'),
    ]
    assert not repository.memory_data_source.set.called


@pytest.mark.asyncio
async def test_should_set_already_not_found_error(repository, mocker):
    fake_entity = 'fake'
    expected_query = Query(repository, memory=True, id=fake_entity)
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[False, False]
    )
    repository.fallback_data_source.get = asynctest.CoroutineMock(
        return_value=None
    )
    repository.memory_data_source.set = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity).entity

    assert exc_info.value.args == (expected_query,)
    assert repository.memory_data_source.exists.call_args_list == [
        mocker.call('fake:fake'),
        mocker.call('fake:not-found:fake'),
    ]
    assert repository.fallback_data_source.get.call_args_list == [
        mocker.call('fake:fake')
    ]
    assert repository.memory_data_source.set.call_args_list == [
        mocker.call('fake:not-found:fake', '1')
    ]


@pytest.mark.asyncio
async def test_should_get_from_fallback(repository, fake_entity):
    await repository.memory_data_source.delete('fake:fake')
    await repository.memory_data_source.delete('fake:not-found:fake')
    repository.fallback_data_source.db['fake:fake'] = {'value': True}
    entity = await repository.query(fake_entity.id).entity

    assert entity == fake_entity.id
    assert repository.memory_data_source.exists('fake:fake')


@pytest.mark.asyncio
async def test_should_set_memory_after_got_fallback(
    repository, fake_entity, mocker
):
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[False, False]
    )
    repository.memory_data_source.set = asynctest.CoroutineMock()
    repository.fallback_data_source.db['fake:fake'] = {'value': True}
    entity = await repository.query(fake_entity.id).entity

    assert repository.memory_data_source.exists.called
    assert repository.memory_data_source.set.call_args_list == [
        mocker.call('fake:fake', '1')
    ]
    assert entity == fake_entity.id
