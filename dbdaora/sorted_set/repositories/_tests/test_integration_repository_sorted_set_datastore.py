import time
from typing import TypedDict

import pytest
from google.cloud import datastore
from jsondaora import jsondaora

from dbdaora import DatastoreSortedSetRepository, SortedSetData
from dbdaora.data_sources.fallback.datastore import DatastoreDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource


@jsondaora
class FakeEntity(TypedDict):
    id: str
    values: SortedSetData


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
    time.sleep(1)
    query = client.query(kind='fake')
    query.add_filter('values', '=', 'v1')

    entities = query.fetch()
    assert tuple(entities) == (entity,)

    values = [('v1', 1), ('v2', 2)]
    client.delete(key)
    await repository.add(FakeEntity(id='fake', values=values))

    query = client.query(kind='fake')
    query.add_filter('values', '=', 'v1')
    entities = query.fetch()
    assert tuple(entities) == tuple()
