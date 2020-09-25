import pytest
from aioredis import GeoMember, GeoPoint
from google.cloud import datastore

from dbdaora import (
    DatastoreGeoSpatialRepository,
    GeoSpatialEntity,
    KindKeyDatastoreDataSource,
)


class FakeGeoSpatialEntity(GeoSpatialEntity):
    id: str


class FakeGeoSpatialRepository(
    DatastoreGeoSpatialRepository, entity_cls=FakeGeoSpatialEntity
):
    name = 'fakegeo'


@pytest.fixture
def fake_repository_cls():
    return FakeGeoSpatialRepository


@pytest.fixture
def fallback_data_source():
    return KindKeyDatastoreDataSource()


@pytest.mark.asyncio
async def test_should_exclude_all_attributes_from_indexes(repository):
    await repository.memory_data_source.delete('fakegeo:fake')
    client = repository.fallback_data_source.client
    key = client.key('fakegeo:fake', 'fake')
    entity = datastore.Entity(key=key)
    entity.update({'latitude': 1, 'longitude': 1, 'member': 'fake'})
    client.put(entity)

    assert not client.get(key).exclude_from_indexes
    entity = FakeGeoSpatialEntity(
        id='fake',
        data=GeoMember(
            member='fake', dist=None, hash=None, coord=GeoPoint(1, 1)
        ),
    )

    await repository.add(entity)

    assert client.get(key).exclude_from_indexes == set(
        FakeGeoSpatialRepository.exclude_from_indexes
    )
