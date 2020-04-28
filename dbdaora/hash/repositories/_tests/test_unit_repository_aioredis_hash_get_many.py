import asynctest
import pytest
from jsondaora import dataclasses

from dbdaora import HashQueryMany
from dbdaora.exceptions import EntityNotFoundError


@pytest.mark.asyncio
async def test_should_set_already_not_found_error_when_get_many(
    repository, mocker
):
    fake_entity = 'fake'
    expected_query = HashQueryMany(repository, memory=True, many=[fake_entity])
    pipeline = repository.memory_data_source.pipeline = mocker.MagicMock()
    pipeline.return_value.execute = asynctest.CoroutineMock(
        return_value=[None]
    )
    pipeline.exists = asynctest.CoroutineMock(side_effect=[False])
    repository.fallback_data_source.get_many = asynctest.CoroutineMock(
        return_value=[None]
    )
    repository.memory_data_source.set = asynctest.CoroutineMock()
    repository.memory_data_source.hmset = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(many=[fake_entity]).entities

    assert exc_info.value.args == (expected_query,)
    assert pipeline.return_value.hgetall.call_args_list == [
        mocker.call('fake:fake'),
    ]
    assert pipeline.return_value.exists.call_args_list == [
        mocker.call('fake:not-found:fake')
    ]
    assert repository.fallback_data_source.get_many.call_args_list == [
        mocker.call(['fake:fake'])
    ]
    assert repository.memory_data_source.set.call_args_list == [
        mocker.call('fake:not-found:fake', '1')
    ]
    assert not repository.memory_data_source.hmset.called


@pytest.mark.asyncio
async def test_should_get_many_from_fallback(repository, fake_entity):
    await repository.memory_data_source.delete('fake:fake')
    await repository.memory_data_source.delete('fake:not-found:fake')
    repository.fallback_data_source.db['fake:fake'] = dataclasses.asdict(
        fake_entity, dumps_value=True
    )

    entities = await repository.query(many=[fake_entity.id]).entities

    assert entities == [fake_entity]
    assert repository.memory_data_source.exists('fake:fake')


@pytest.mark.asyncio
async def test_should_get_many_with_one_item_already_not_found_from_fallback(
    repository, fake_entity
):
    await repository.memory_data_source.delete('fake:fake')
    await repository.memory_data_source.delete('fake:fake2')
    await repository.memory_data_source.delete('fake:not-found:fake')
    await repository.memory_data_source.set('fake:not-found:fake2', '1')
    repository.fallback_data_source.db['fake:fake'] = dataclasses.asdict(
        fake_entity, dumps_value=True
    )

    entities = await repository.query(many=[fake_entity.id, 'fake2']).entities

    assert entities == [fake_entity]
    assert repository.memory_data_source.exists('fake:fake')


@pytest.mark.asyncio
async def test_should_get_many_with_one_item_already_not_found_and_another_not_found_from_fallback(
    repository, fake_entity
):
    await repository.memory_data_source.delete('fake:fake')
    await repository.memory_data_source.delete('fake:fake2')
    await repository.memory_data_source.delete('fake:fake3')
    await repository.memory_data_source.delete('fake:not-found:fake')
    await repository.memory_data_source.delete('fake:not-found:fake3')
    await repository.memory_data_source.set('fake:not-found:fake2', '1')
    repository.fallback_data_source.db['fake:fake'] = dataclasses.asdict(
        fake_entity, dumps_value=True
    )

    entities = await repository.query(
        many=[fake_entity.id, 'fake2', 'fake3']
    ).entities

    assert entities == [fake_entity]
    assert repository.memory_data_source.exists('fake:fake')
    assert repository.memory_data_source.exists('fake:not-found:fake3')
