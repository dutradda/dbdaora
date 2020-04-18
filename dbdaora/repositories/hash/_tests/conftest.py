import dataclasses
from typing import Optional

import mockaioredis
import pytest

from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory import MemoryDataSource
from dbdaora.repositories.hash import HashData, HashRepository
from dbdaora.repositories.hash.dict import DictHashRepository


@dataclasses.dataclass
class FakeEntity:
    id: str
    integer: int
    number: Optional[float] = None
    boolean: Optional[bool] = None


class FakeRepository(HashRepository[FakeEntity, HashData, str]):
    entity_name = 'fake'


class FakeAioRedisDataSource(mockaioredis.MockRedis, MemoryDataSource):
    ...


@pytest.mark.asyncio
@pytest.fixture
async def repository(mocker):
    repo = FakeRepository(
        memory_data_source=await mockaioredis.create_redis_pool(
            'redis://', commands_factory=FakeAioRedisDataSource
        ),
        fallback_data_source=DictFallbackDataSource(),
        expire_time=1,
    )
    yield repo
    repo.memory_data_source.close()


@pytest.fixture
def fake_entity_cls():
    return FakeEntity


@pytest.fixture
def fake_entity():
    return FakeEntity(id='fake', integer=1, number=0.1, boolean=True)


@pytest.fixture
def serialized_fake_entity():
    return {
        b'id': b'fake',
        b'integer': b'1',
        b'number': b'0.1',
        b'boolean': b'1',
    }
