import pytest
from google.cloud import datastore
from jsondaora import jsondaora

from dbdaora import DatastoreSortedSetRepository, SortedSetDictEntity
from dbdaora.data_sources.fallback.datastore import DatastoreDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource


@jsondaora
class FakeEntity(SortedSetDictEntity):
    id: str


class FakeRepository(DatastoreSortedSetRepository, entity_cls=FakeEntity):
    key_attrs = ('id',)


@pytest.fixture
async def repository(mocker, dict_repository_cls):
    return FakeRepository(
        memory_data_source=DictMemoryDataSource(),
        fallback_data_source=DatastoreDataSource(),
        expire_time=1,
    )


@pytest.mark.asyncio
async def test_should_exclude_all_attributes_from_indexes(repository):
    client = repository.fallback_data_source.client
    values = ['v1', 1, 'v2', 2]
    key = client.key('fake', 'fake')
    client.delete(key)
    entity = datastore.Entity(key=key)
    entity.update({'values': values})
    client.put(entity)
    query = client.query(kind='fake')
    query.add_filter('values', '=', 'v1')

    entities = query.fetch()
    assert tuple(entities) == (entity,)

    values = [('v1', 1), ('v2', 2)]
    await repository.add(FakeEntity(id='fake', values=values))

    query = client.query(kind='fake')
    query.add_filter('values', '=', 'v1')
    entities = query.fetch()
    assert tuple(entities) == tuple()
