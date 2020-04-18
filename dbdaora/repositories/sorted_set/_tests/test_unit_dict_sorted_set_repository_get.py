import dataclasses

import asynctest
import pytest

from dbdaora.exceptions import EntityNotFoundError
from dbdaora.repositories import SortedSetEntity, SortedSetQueryBase
from dbdaora.repositories.sorted_set.dict import DictSortedSetRepository


@dataclasses.dataclass
class FakeRepository(DictSortedSetRepository[str, str, str]):
    query_cls = SortedSetQueryBase
    entity_name = 'fake'
    expire_time: int = 1

    @classmethod
    def entity_key(cls, query):
        return query.entity_id


@pytest.fixture
def repository(mocker):
    return FakeRepository()


@pytest.fixture
def fake_entity():
    return SortedSetEntity(id='fake', data=['1', '2'])


@pytest.fixture
def fake_entity_withscores():
    return SortedSetEntity(id='fake', data=[('1', 0), ('2', 1)])


@pytest.mark.asyncio
async def test_should_get_from_memory(
    repository, fake_entity, fake_entity_withscores
):
    repository.memory_data_source.db['fake:fake'] = fake_entity_withscores.data
    entity = await repository.query(fake_entity.id).get()

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository, fake_entity, mocker):
    fake_query = SortedSetQueryBase(repository, fake_entity.id)

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity.id).get()

    assert exc_info.value.args == (fake_query,)


@pytest.mark.asyncio
async def test_should_raise_not_found_error_when_already_raised_before(
    repository, mocker
):
    fake_entity = 'fake'
    expected_query = SortedSetQueryBase(repository, entity_id=fake_entity)
    repository.memory_data_source.zrange = asynctest.CoroutineMock(
        side_effect=[None]
    )
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[True]
    )
    repository.memory_data_source.zadd = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity).get()

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
        side_effect=[None, fake_entity.data]
    )
    repository.fallback_data_source.db[
        'fake:fake'
    ] = fake_entity_withscores.data
    entity = await repository.query(fake_entity.id).get()

    assert repository.memory_data_source.zrange.called
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_set_memory_after_got_fallback(
    repository, fake_entity_withscores, fake_entity, mocker
):
    repository.memory_data_source.zrange = asynctest.CoroutineMock(
        side_effect=[None, fake_entity.data]
    )
    repository.memory_data_source.zadd = asynctest.CoroutineMock()
    repository.fallback_data_source.db[
        'fake:fake'
    ] = fake_entity_withscores.data
    entity = await repository.query(fake_entity.id).get()

    assert repository.memory_data_source.zrange.called
    assert repository.memory_data_source.zadd.call_args_list == [
        mocker.call('fake:fake', 1, '2', 0, '1')
    ]
    assert entity == fake_entity
