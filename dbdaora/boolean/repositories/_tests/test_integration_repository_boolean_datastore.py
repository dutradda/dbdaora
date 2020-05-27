import pytest
from google.cloud import datastore

from dbdaora import DatastoreBooleanRepository
from dbdaora.data_sources.fallback.datastore import DatastoreDataSource


@pytest.fixture
def fallback_data_source():
    return DatastoreDataSource()


@pytest.fixture
def fake_boolean_repository_cls(fake_entity_cls):
    class FakeRepository(
        DatastoreBooleanRepository, entity_cls=fake_entity_cls
    ):
        id_name = 'id'

    return FakeRepository


@pytest.mark.asyncio
async def test_should_exclude_attributes_from_indexes(
    repository, fake_entity_cls
):
    client = repository.fallback_data_source.client
    key = client.key('fake', 'fake')
    entity = datastore.Entity(key=key)
    entity.update({'value': True})
    client.put(entity)

    assert not client.get(key).exclude_from_indexes

    await repository.add(fake_entity_cls(id='fake'))

    assert client.get(key).exclude_from_indexes == set(['value'])
