import asynctest
import pytest
from jsondaora import dataclasses

from dbdaora import HashQueryMany
from dbdaora.exceptions import EntityNotFoundError


@pytest.mark.asyncio
async def test_should_set_already_not_found_error_when_get_many_with_fields(
    repository, mocker
):
    fake_entity = 'fake'
    expected_query = HashQueryMany(
        repository,
        memory=True,
        many=[fake_entity],
        fields=['id', 'integer', 'inner_entities'],
    )
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
        await repository.query(
            many=[fake_entity], fields=['id', 'integer', 'inner_entities']
        ).entities

    assert exc_info.value.args == (expected_query,)
    assert pipeline.return_value.hmget.call_args_list == [
        mocker.call('fake:fake', 'id', 'integer', 'inner_entities'),
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
async def test_should_get_many_from_fallback_with_fields(
    repository, fake_entity
):
    await repository.memory_data_source.delete('fake:fake')
    await repository.memory_data_source.delete('fake:not-found:fake')
    repository.fallback_data_source.db['fake:fake'] = dataclasses.asdict(
        fake_entity, dumps_value=True
    )
    fake_entity.number = None
    fake_entity.boolean = False

    entities = await repository.query(
        many=[fake_entity.id], fields=['id', 'integer', 'inner_entities']
    ).entities

    assert entities == [fake_entity]
    assert repository.memory_data_source.exists('fake:fake')
