import dataclasses
import os
from typing import List, Optional

import pytest
from motor.motor_asyncio import AsyncIOMotorClient

from dbdaora import HashRepository, MongoDataSource


@pytest.fixture
def fallback_data_source(event_loop):
    auth = os.environ.get('MONGO_AUTH', 'mongo:mongo')
    client = AsyncIOMotorClient(
        f'mongodb://{auth}@localhost:27017', io_loop=event_loop
    )
    return MongoDataSource(database_name='dbdaora', client=client)


@dataclasses.dataclass
class FakeInnerEntity:
    id: str


@dataclasses.dataclass
class FakeEntity:
    id: str
    other_id: str
    integer: int
    inner_entities: List[FakeInnerEntity]
    number: Optional[float] = None
    boolean: Optional[bool] = None


@pytest.fixture
def fake_entity_cls():
    return FakeEntity


@pytest.fixture
def fake_entity():
    return FakeEntity(
        id='fake',
        other_id='other_fake',
        inner_entities=[FakeInnerEntity('inner1'), FakeInnerEntity('inner2')],
        integer=1,
        number=0.1,
        boolean=True,
    )


@pytest.fixture
def fake_entity2():
    return FakeEntity(
        id='fake2',
        other_id='other_fake',
        inner_entities=[FakeInnerEntity('inner3'), FakeInnerEntity('inner4')],
        integer=2,
        number=0.2,
        boolean=False,
    )


@pytest.fixture
def fake_entity3():
    return FakeEntity(
        id='fake3',
        other_id='other_fake',
        inner_entities=[FakeInnerEntity('inner3'), FakeInnerEntity('inner4')],
        integer=2,
        number=0.2,
        boolean=False,
    )


@pytest.fixture
def serialized_fake_entity():
    return {
        b'id': b'fake',
        b'other_id': b'other_fake',
        b'integer': b'1',
        b'number': b'0.1',
        b'boolean': b'1',
        b'inner_entities': b'[{"id":"inner1"},{"id":"inner2"}]',
    }


@pytest.fixture
def serialized_fake_entity2():
    return {
        b'id': b'fake2',
        b'other_id': b'other_fake',
        b'integer': b'2',
        b'number': b'0.2',
        b'boolean': b'0',
        b'inner_entities': b'[{"id":"inner3"},{"id":"inner4"}]',
    }


@pytest.fixture
def serialized_fake_entity3():
    return {
        b'id': b'fake3',
        b'other_id': b'other_fake',
        b'integer': b'2',
        b'number': b'0.2',
        b'boolean': b'0',
        b'inner_entities': b'[{"id":"inner3"},{"id":"inner4"}]',
    }


class FakeHashRepository(HashRepository):
    name = 'fake'
    key_attrs = ('other_id', 'id')
    many_key_attrs = ('id',)
    entity_cls = FakeEntity


@pytest.fixture
def fake_hash_repository_cls():
    return FakeHashRepository
