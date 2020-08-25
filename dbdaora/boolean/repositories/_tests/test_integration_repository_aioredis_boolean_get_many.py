import pytest


@pytest.mark.asyncio
async def test_should_set_already_not_found_error_when_get_many(
    repository, mocker
):
    fake_entity = 'fake'
    repository.memory_data_source.get = mocker.AsyncMock(side_effect=[None])
    repository.fallback_data_source.get = mocker.AsyncMock(side_effect=[None])
    repository.memory_data_source.set = mocker.AsyncMock()

    assert [
        e async for e in repository.query(many=[fake_entity]).entities
    ] == []

    assert repository.memory_data_source.get.call_args_list == [
        mocker.call('fake:fake'),
    ]
    assert repository.fallback_data_source.get.call_args_list == [
        mocker.call('fake:fake')
    ]
    assert repository.memory_data_source.set.call_args_list == [
        mocker.call('fake:fake', '0')
    ]


@pytest.mark.asyncio
async def test_should_get_many_from_fallback(repository, fake_entity):
    await repository.memory_data_source.delete('fake:fake')
    repository.fallback_data_source.db['fake:fake'] = {'value': True}

    entities = [
        e async for e in repository.query(many=[fake_entity.id]).entities
    ]

    assert entities == [fake_entity.id]
    assert await repository.memory_data_source.get('fake:fake') == b'1'


@pytest.mark.asyncio
async def test_should_get_many_with_one_item_already_not_found_from_fallback(
    repository, fake_entity
):
    await repository.memory_data_source.delete('fake:fake')
    await repository.memory_data_source.delete('fake:fake2')
    repository.fallback_data_source.db['fake:fake'] = {'value': True}

    entities = [
        e
        async for e in repository.query(
            many=[fake_entity.id, 'fake2']
        ).entities
    ]

    assert entities == [fake_entity.id]
    assert await repository.memory_data_source.get('fake:fake') == b'1'
    assert await repository.memory_data_source.get('fake:fake2') == b'0'


@pytest.mark.asyncio
async def test_should_get_many_with_one_item_already_not_found_and_another_not_found_from_fallback(
    repository, fake_entity
):
    await repository.memory_data_source.delete('fake:fake')
    await repository.memory_data_source.set('fake:fake2', '0')
    await repository.memory_data_source.delete('fake:fake3')
    repository.fallback_data_source.db['fake:fake'] = {'value': True}

    entities = [
        e
        async for e in repository.query(
            many=[fake_entity.id, 'fake2', 'fake3']
        ).entities
    ]

    assert entities == [fake_entity.id]
    assert await repository.memory_data_source.get('fake:fake') == b'1'
    assert await repository.memory_data_source.get('fake:fake2') == b'0'
    assert await repository.memory_data_source.get('fake:fake3') == b'0'
