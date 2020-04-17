import dataclasses

import asynctest
import pytest

from dbdaora.exceptions import EntityNotFoundError
from dbdaora.query import Query
from dbdaora.repositories.memory import DictMemoryRepository


@dataclasses.dataclass
class FakeQuery(Query):
    entity_id: str


@dataclasses.dataclass
class FakeRepository(DictMemoryRepository):
    query_cls = FakeQuery
    entity_name = 'fake'
    expire_time: int = 1

    def memory_key(self, query):
        return f'{self.entity_name}:{query.entity_id}'

    def fallback_key(self, query):
        return f'{self.entity_name}:{query.entity_id}'

    def make_fallback_not_found_key(self, query) -> str:
        return self.memory_data_source.make_key(
            self.entity_name, 'not-found', query.entity_id
        )


@pytest.fixture
def repository():
    return FakeRepository()


@pytest.mark.asyncio
async def test_should_get_from_memory(repository):
    expected_entity = 'fake'
    repository.memory_data_source.db['fake:fake'] = expected_entity
    assert await repository.query(expected_entity).get() == expected_entity


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository):
    fake_entity = 'fake'
    expected_query = FakeQuery(repository, entity_id=fake_entity)

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity).get()

    assert exc_info.value.args == (expected_query,)


@pytest.mark.asyncio
async def test_should_raise_not_found_error_when_already_raised_before(
    repository, mocker
):
    fake_entity = 'fake'
    expected_query = FakeQuery(repository, entity_id=fake_entity)
    repository.memory_data_source.get_obj = mocker.MagicMock(
        side_effect=[None]
    )
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[True]
    )
    repository.memory_data_source.set = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity).get()

    assert exc_info.value.args == (expected_query,)
    assert repository.memory_data_source.get_obj.call_args_list == [
        mocker.call('fake:fake'),
    ]
    assert repository.memory_data_source.exists.call_args_list == [
        mocker.call('fake:not-found:fake')
    ]
    assert not repository.memory_data_source.set.called


@pytest.mark.asyncio
async def test_should_get_from_fallback(repository, mocker):
    expected_entity = 'fake'
    repository.memory_data_source.get_obj = mocker.MagicMock(
        side_effect=[None, expected_entity]
    )
    repository.fallback_data_source.db['fake:fake'] = expected_entity
    entity = await repository.query(expected_entity).get()

    assert repository.memory_data_source.get_obj.called
    assert entity == expected_entity


@pytest.mark.asyncio
async def test_should_set_memory_after_got_fallback(repository, mocker):
    expected_entity = 'fake'
    repository.memory_data_source.get_obj = mocker.MagicMock(
        side_effect=[None, expected_entity]
    )
    repository.memory_data_source.set_obj = mocker.MagicMock()
    repository.fallback_data_source.db['fake:fake'] = expected_entity
    entity = await repository.query(expected_entity).get()

    assert repository.memory_data_source.get_obj.called
    assert repository.memory_data_source.set_obj.call_args_list == [
        mocker.call('fake:fake', expected_entity)
    ]
    assert entity == expected_entity
