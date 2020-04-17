import dataclasses

import asynctest
import pytest

from dbdaora.exceptions import EntityNotFoundError
from dbdaora.repositories.memory.hash import serializer
from dbdaora.repositories.memory.hash.dict import DictHashRepository
from dbdaora.repositories.memory.hash.query import HashQuery


@dataclasses.dataclass
class FakeEntity:
    id: str
    integer: int
    number: float
    boolean: bool


@dataclasses.dataclass
class FakeRepository(DictHashRepository[FakeEntity, str]):
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
    return FakeEntity(id='fake', integer=1, number=0.1, boolean=True)


@pytest.fixture
def serialized_fake_entity():
    return {
        b'id': b'fake',
        b'i:integer': b'1',
        b'n:number': b'0.1',
        b'b:boolean': b'1',
    }


@pytest.mark.asyncio
async def test_should_get_from_memory(
    repository, serialized_fake_entity, fake_entity
):
    repository.memory_data_source.db['fake:fake'] = serialized_fake_entity
    entity = await repository.query('fake').get()

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository, fake_entity, mocker):
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
    repository.memory_data_source.hgetall = asynctest.CoroutineMock(
        side_effect=[None]
    )
    repository.fallback_data_source.db['fake:fake'] = serialized_fake_entity
    entity = await repository.query(fake_entity.id).get()

    assert repository.memory_data_source.hgetall.called
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
            b'i:integer',
            b'1',
            b'n:number',
            b'0.1',
            b'b:boolean',
            b'1',
        )
    ]
    assert entity == fake_entity
