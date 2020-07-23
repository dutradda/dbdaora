import pytest
from aioredis import GeoMember, GeoPoint
from google.cloud import datastore

from dbdaora import (
    DatastoreDataSource,
    DatastoreGeoSpatialRepository,
    GeoSpatialEntity,
)


class FakeGeoSpatialRepository(DatastoreGeoSpatialRepository):
    name = 'fake'


@pytest.fixture
def fake_repository_cls():
    return FakeGeoSpatialRepository


@pytest.fixture
def fallback_data_source():
    return DatastoreDataSource()


@pytest.mark.asyncio
async def test_should_exclude_all_attributes_from_indexes(repository):
    await repository.memory_data_source.delete('fake:fake')
    client = repository.fallback_data_source.client
    key = client.key('fake', 'fake')
    entity = datastore.Entity(key=key)
    entity.update({'data': [{'latitude': 1, 'longitude': 1}]})
    client.put(entity)

    assert not client.get(key).exclude_from_indexes

    await repository.add(
        GeoSpatialEntity(
            id='fake',
            data=[
                GeoMember(
                    member='fake', dist=None, hash=None, coord=GeoPoint(1, 1)
                )
            ],
        )
    )

    assert client.get(key).exclude_from_indexes == set(['data'])
