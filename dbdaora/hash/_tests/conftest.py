import dataclasses
from typing import List, Optional

import pytest
from aioredis import RedisError
from cachetools import TTLCache

from dbdaora import (
    AsyncCircuitBreaker,
    DatastoreDataSource,
    DatastoreHashRepository,
    DictFallbackDataSource,
    HashRepository,
    HashService,
    make_aioredis_data_source,
)


@pytest.fixture
def fake_service(aioredis_repository, mocker):
    circuit_breaker = AsyncCircuitBreaker(
        failure_threshold=0,
        recovery_timeout=10,
        expected_exception=RedisError,
        name='fake',
    )
    cache = TTLCache(maxsize=1, ttl=1)
    return HashService(
        aioredis_repository, circuit_breaker, cache, logger=mocker.MagicMock(),
    )


@dataclasses.dataclass
class FakeInnerEntity:
    id: str


@dataclasses.dataclass
class FakeEntity:
    id: str
    integer: int
    inner_entities: List[FakeInnerEntity]
    number: Optional[float] = None
    boolean: Optional[bool] = None


class FakeHashRepository(HashRepository[str]):
    entity_name = 'fake'
    entity_cls = FakeEntity


class FakeDatastoreHashRepository(DatastoreHashRepository):
    entity_name = 'fake'
    entity_cls = FakeEntity


@pytest.fixture
def fake_datastore_repository_cls():
    return FakeDatastoreHashRepository


@pytest.fixture
def dict_repository_cls():
    return FakeHashRepository


@pytest.fixture
def fake_entity():
    return FakeEntity(
        id='fake',
        inner_entities=[FakeInnerEntity('inner1'), FakeInnerEntity('inner2')],
        integer=1,
        number=0.1,
        boolean=True,
    )


@pytest.fixture
def fake_entity2():
    return FakeEntity(
        id='fake2',
        inner_entities=[FakeInnerEntity('inner3'), FakeInnerEntity('inner4')],
        integer=2,
        number=0.2,
        boolean=False,
    )


@pytest.fixture
def serialized_fake_entity():
    return {
        b'id': b'fake',
        b'integer': b'1',
        b'number': b'0.1',
        b'boolean': b'1',
        b'inner_entities': b'[{"id":"inner1"},{"id":"inner2"}]',
    }


@pytest.fixture
def serialized_fake_entity2():
    return {
        b'id': b'fake2',
        b'integer': b'2',
        b'number': b'0.2',
        b'boolean': b'0',
        b'inner_entities': b'[{"id":"inner3"},{"id":"inner4"}]',
    }


@pytest.mark.asyncio
@pytest.fixture
async def aioredis_repository(mocker):
    memory_data_source = await make_aioredis_data_source(
        'redis://', 'redis://localhost/1', 'redis://localhost/2'
    )
    yield FakeHashRepository(
        memory_data_source=memory_data_source,
        fallback_data_source=DictFallbackDataSource(),
        expire_time=1,
    )
    memory_data_source.close()
    await memory_data_source.wait_closed()


@pytest.mark.asyncio
@pytest.fixture
async def aioredis_datastore_repository(mocker):
    memory_data_source = await make_aioredis_data_source(
        'redis://', 'redis://localhost/1', 'redis://localhost/2'
    )
    yield FakeDatastoreHashRepository(
        memory_data_source=memory_data_source,
        fallback_data_source=DatastoreDataSource(),
        expire_time=1,
    )
    memory_data_source.close()
    await memory_data_source.wait_closed()
