import os

import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

from dbdaora import MongoDataSource, MongodbSortedSetRepository


@pytest.fixture
def fallback_data_source(event_loop):
    auth = os.environ.get('MONGO_AUTH', 'mongo:mongo')
    client = AsyncIOMotorClient(
        f'mongodb://{auth}@localhost:27017', io_loop=event_loop
    )
    return MongoDataSource(database_name='dbdaora', client=client)


@pytest.fixture
def fake_repository_cls(fake_entity_cls):
    class FakeRepository(MongodbSortedSetRepository):
        key_attrs = ('id',)
        entity_cls = fake_entity_cls

    return FakeRepository


@pytest.fixture
def fallback_cb_error():
    return PyMongoError
