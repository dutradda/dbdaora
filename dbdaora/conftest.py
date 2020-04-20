import dataclasses
from typing import Optional

import pytest

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    HashRepository,
    make_aioredis_data_source,
)


@dataclasses.dataclass
class FakeEntity:
    id: str
    integer: int
    number: Optional[float] = None
    boolean: Optional[bool] = None


class FakeHashRepository(HashRepository[FakeEntity, str]):
    entity_name = 'fake'


@pytest.fixture
def fake_repository_cls():
    return FakeHashRepository


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


@pytest.mark.asyncio
@pytest.fixture
async def dict_repository(fake_repository_cls, mocker):
    return fake_repository_cls(
        memory_data_source=DictMemoryDataSource(),
        fallback_data_source=DictFallbackDataSource(),
        expire_time=1,
    )


@pytest.mark.asyncio
@pytest.fixture
async def aioredis_repository(fake_repository_cls, mocker):
    memory_data_source = await make_aioredis_data_source(
        'redis://', 'redis://localhost/1', 'redis://localhost/2'
    )
    yield fake_repository_cls(
        memory_data_source=memory_data_source,
        fallback_data_source=DictFallbackDataSource(),
        expire_time=1,
    )
    memory_data_source.close()
    await memory_data_source.wait_closed()
