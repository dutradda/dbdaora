import os

import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure

from dbdaora import MongoDataSource, MongodbSortedSetRepository


@pytest.fixture
def fallback_data_source(event_loop):
    auth = os.environ.get('MONGO_AUTH', 'mongo:mongo')
    client = AsyncIOMotorClient(
        f'mongodb://{auth}@localhost:27017', io_loop=event_loop
    )
    MongoDataSource.collections_has_ttl_index.clear()
    return MongoDataSource(database_name='dbdaora', client=client)


@pytest.fixture
def fake_repository_cls(fake_entity_cls):
    class FakeRepository(MongodbSortedSetRepository):
        entity_cls = fake_entity_cls

    return FakeRepository


@pytest.fixture(autouse=True)
async def drop_ttl_index(repository):
    collection = repository.fallback_data_source.client.dbdaora.fake

    try:
        await collection.drop_index('last_modified_1')
    except OperationFailure:
        ...


@pytest.fixture
async def create_ttl_index(repository):
    collection = repository.fallback_data_source.client.dbdaora.fake

    try:
        await collection.create_index(
            'last_modified', expireAfterSeconds=60,
        )
    except OperationFailure:
        ...

    yield

    try:
        await collection.drop_index('last_modified_1')
    except OperationFailure:
        ...


@pytest.mark.asyncio
async def test_should_create_collection_ttl_index(repository, fake_entity_cls):
    collection = repository.fallback_data_source.client.dbdaora.fake

    assert not [
        i
        async for i in collection.list_indexes()
        if 'last_modified' in i['key']
    ]

    await repository.add(
        fake_entity_cls(id='fake', values=[('v1', 1), ('v2', 2)]),
        fallback_ttl=60,
    )

    assert [
        i
        async for i in collection.list_indexes()
        if 'last_modified' in i['key'] and i['expireAfterSeconds'] == 60
    ]


@pytest.mark.asyncio
async def test_should_drop_and_create_collection_ttl_index(
    create_ttl_index, repository, fake_entity_cls
):
    collection = repository.fallback_data_source.client.dbdaora.fake

    assert [
        i
        async for i in collection.list_indexes()
        if 'last_modified' in i['key'] and i['expireAfterSeconds'] == 60
    ]

    await repository.add(
        fake_entity_cls(id='fake', values=[('v1', 1), ('v2', 2)]),
        fallback_ttl=61,
    )

    assert [
        i
        async for i in collection.list_indexes()
        if 'last_modified' in i['key'] and i['expireAfterSeconds'] == 61
    ]


@pytest.mark.asyncio
async def test_should_not_drop_collection_ttl_index(
    create_ttl_index, repository, fake_entity_cls
):
    collection = repository.fallback_data_source.client.dbdaora.fake

    assert [
        i
        async for i in collection.list_indexes()
        if 'last_modified' in i['key'] and i['expireAfterSeconds'] == 60
    ]

    await repository.add(
        fake_entity_cls(id='fake', values=[('v1', 1), ('v2', 2)]),
        fallback_ttl=60,
    )

    assert [
        i
        async for i in collection.list_indexes()
        if 'last_modified' in i['key'] and i['expireAfterSeconds'] == 60
    ]


@pytest.mark.asyncio
async def test_should_get_entity_with_ttl_index(
    create_ttl_index, repository, fake_entity_cls
):
    await repository.add(
        fake_entity_cls(id='fake', values=[('v1', 1), ('v2', 2)]),
        fallback_ttl=60,
    )

    fake_entity = await repository.query(id='fake').entity

    assert fake_entity.id == 'fake'
    assert fake_entity.values == [b'v1', b'v2']
