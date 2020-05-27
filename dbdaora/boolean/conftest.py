import dataclasses
from functools import partial

import pytest
from aioredis import RedisError

from dbdaora import (
    BooleanRepository,
    BooleanService,
    CacheType,
    DictFallbackDataSource,
    build_service,
    make_aioredis_data_source,
)


@pytest.fixture
def memory_data_source_factory():
    return partial(
        make_aioredis_data_source,
        'redis://',
        'redis://localhost/1',
        'redis://localhost/2',
    )


@pytest.mark.asyncio
@pytest.fixture
async def fake_service(
    memory_data_source_factory,
    mocker,
    fallback_data_source,
    fake_boolean_repository_cls,
):
    async def fallback_data_source_factory():
        return fallback_data_source

    service = await build_service(
        BooleanService,
        fake_boolean_repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time=1,
        cache_type=CacheType.TTL,
        cache_ttl=1,
        cache_max_size=2,
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
class FakeEntity:
    id: str


class FakeBooleanRepository(BooleanRepository[str]):
    name = 'fake'
    id_name = 'id'
    entity_cls = FakeEntity


@pytest.fixture
def fake_entity_cls():
    return FakeEntity


@pytest.fixture
def fake_boolean_repository_cls():
    return FakeBooleanRepository


@pytest.fixture
def dict_repository_cls():
    return FakeBooleanRepository


@pytest.fixture
def fake_entity():
    return FakeEntity(id='fake',)


@pytest.fixture
def fake_entity2():
    return FakeEntity(id='fake2',)


@pytest.fixture
def serialized_fake_entity():
    return '1'


@pytest.fixture
def serialized_fake_entity2():
    return '1'


@pytest.mark.asyncio
@pytest.fixture
async def repository(
    mocker,
    memory_data_source_factory,
    fallback_data_source,
    fake_boolean_repository_cls,
):
    memory_data_source = await memory_data_source_factory()
    yield fake_boolean_repository_cls(
        memory_data_source=memory_data_source,
        fallback_data_source=fallback_data_source,
        expire_time=1,
    )
    memory_data_source.close()
    await memory_data_source.wait_closed()
