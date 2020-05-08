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
    exclude_all_from_indexes = True


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
    entity = datastore.Entity(key=key)
    entity.update({'id': 'fake', 'fake_int': 1})
    client.put(entity)

    assert not client.get(key).exclude_from_indexes

    await repository.add(FakeEntity(id='fake', fake_int=1))

    assert client.get(key).exclude_from_indexes == set(['id', 'fake_int'])
