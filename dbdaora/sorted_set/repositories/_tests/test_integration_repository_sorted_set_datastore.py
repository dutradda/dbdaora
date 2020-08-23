import pytest
from google.cloud import datastore

from dbdaora import DatastoreDataSource, DatastoreSortedSetRepository


@pytest.fixture
def fallback_data_source():
    return DatastoreDataSource()


@pytest.fixture
def fake_repository_cls(fake_entity_cls):
    class FakeRepository(DatastoreSortedSetRepository):
        entity_cls = fake_entity_cls

    return FakeRepository


@pytest.mark.asyncio
async def test_should_exclude_values_attributes_from_indexes(
    repository, fake_entity_cls
):
    client = repository.fallback_data_source.client
    values = ['v1', 1, 'v2', 2]
    key = client.key('fake', 'fake')
    entity = datastore.Entity(key=key)
    entity.update({'values': values})
    client.put(entity)

    assert not client.get(key).exclude_from_indexes

    values = [('v1', 1), ('v2', 2)]
    await repository.add(fake_entity_cls(id='fake', values=values))

    assert client.get(key).exclude_from_indexes == set(['values'])
