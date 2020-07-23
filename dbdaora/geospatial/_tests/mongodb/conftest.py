import os

import pytest
from motor.motor_asyncio import AsyncIOMotorClient

from dbdaora import CollectionKeyMongoDataSource, MongodbGeoSpatialRepository


@pytest.fixture
def fallback_data_source(event_loop):
    auth = os.environ.get('MONGO_AUTH', 'mongo:mongo')
    client = AsyncIOMotorClient(
        f'mongodb://{auth}@localhost:27017', io_loop=event_loop
    )
    return CollectionKeyMongoDataSource(database_name='dbdaora', client=client)


@pytest.fixture
def fake_repository_cls(fake_entity_cls):
    class FakeGeoSpatialRepository(MongodbGeoSpatialRepository):
        name = 'fake'
        key_attrs = ('fake2_id', 'fake_id')
        entity_cls = fake_entity_cls

    return FakeGeoSpatialRepository
