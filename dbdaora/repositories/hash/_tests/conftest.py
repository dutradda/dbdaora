import dataclasses
from typing import Optional

import pytest

from dbdaora import HashRepository
from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource


@dataclasses.dataclass
class FakeEntity:
    id: str
    integer: int
    number: Optional[float] = None
    boolean: Optional[bool] = None


class FakeRepository(HashRepository[FakeEntity, str]):
    entity_name = 'fake'


@pytest.fixture
def fake_repository_cls():
    return FakeRepository


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
