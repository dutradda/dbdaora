import dataclasses
from functools import partial
from typing import List, Optional

import pytest
from aioredis import RedisError

from dbdaora import (
    CacheType,
    DictFallbackDataSource,
    HashRepository,
    HashService,
    build_service,
    make_aioredis_data_source,
)


@pytest.mark.asyncio
@pytest.fixture
async def fake_service(mocker, fallback_data_source, fake_hash_repository_cls):
    memory_data_source_factory = partial(
        make_aioredis_data_source,
        'redis://',
        'redis://localhost/1',
        'redis://localhost/2',
    )

    async def fallback_data_source_factory():
        return fallback_data_source

    service = await build_service(
        HashService,
        fake_hash_repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time=1,
        cache_type=CacheType.TTL,
        cache_ttl=1,
        cache_max_size=1,
        cb_failure_threshold=0,
        cb_recovery_timeout=10,
        cb_expected_exception=RedisError,
        logger=mocker.MagicMock(),
    )

    yield service

    service.repository.memory_data_source.close()
    await service.repository.memory_data_source.wait_closed()


@pytest.fixture
def fallback_data_source():
    return DictFallbackDataSource()


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
    name = 'fake'
    entity_cls = FakeEntity


@pytest.fixture
def fake_hash_repository_cls():
    return FakeHashRepository


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
async def repository(mocker):
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
