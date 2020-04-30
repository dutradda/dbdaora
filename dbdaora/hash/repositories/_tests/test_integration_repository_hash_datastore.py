import time
from dataclasses import dataclass

import pytest
from google.cloud import datastore

from dbdaora import DatastoreHashRepository
from dbdaora.data_sources.fallback.datastore import DatastoreDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource


@dataclass
class FakeEntity:
    id: str
    fake_int: int


class FakeRepository(DatastoreHashRepository, entity_cls=FakeEntity):
    key_attrs = ('id',)
    exclue_all_from_indexes = True


@pytest.fixture
async def repository():
    return FakeRepository(
        memory_data_source=DictMemoryDataSource(),
        fallback_data_source=DatastoreDataSource(),
        expire_time=1,
    )


@pytest.mark.asyncio
async def test_should_exclude_all_attributes_from_indexes(repository):
    client = repository.fallback_data_source.client
    key = client.key('fake', 'fake')
    client.delete(key)
    entity = datastore.Entity(key=key)
    entity.update({'id': 'fake', 'fake_int': 1})
    client.put(entity)
    time.sleep(0.5)
    query = client.query(kind='fake')
    query.add_filter('fake_int', '=', 1)

    entities = query.fetch()
    assert tuple(entities) == (entity,)

    await repository.add(FakeEntity(id='fake', fake_int=1))

    time.sleep(0.5)
    query = client.query(kind='fake')
    query.add_filter('values', '=', 1)
    entities = query.fetch()
    assert tuple(entities) == tuple()
