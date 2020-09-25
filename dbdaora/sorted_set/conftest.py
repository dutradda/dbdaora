from dataclasses import dataclass
from typing import Optional

import pytest

from dbdaora import DictFallbackDataSource, SortedSetData, SortedSetRepository


@dataclass
class FakeEntity:
    id: str
    data: SortedSetData
    max_size: Optional[int] = None


@pytest.fixture
def fake_entity_cls():
    return FakeEntity


@pytest.fixture
def fake_entity(fake_entity_cls):
    return fake_entity_cls(id='fake', data=[b'1', b'2'])


@pytest.fixture
def fake_entity_withscores(fake_entity_cls):
    return fake_entity_cls(id='fake', data=[(b'1', 0), (b'2', 1)])


@pytest.fixture
def fake_repository_cls(fake_entity_cls):
    class FakeRepository(SortedSetRepository[fake_entity_cls, str]):
        ...

    return FakeRepository


@pytest.fixture
def fallback_data_source():
    return DictFallbackDataSource()
