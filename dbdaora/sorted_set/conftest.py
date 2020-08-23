from dataclasses import dataclass
from typing import Optional

import pytest

from dbdaora import SortedSetData, SortedSetRepository


@dataclass
class FakeEntity:
    id: str
    values: SortedSetData
    max_size: Optional[int] = None


class FakeRepository(SortedSetRepository[str], entity_cls=FakeEntity):
    key_attrs = ('id',)


@pytest.fixture
def fake_entity():
    return FakeEntity(id='fake', values=[b'1', b'2'])


@pytest.fixture
def fake_entity_withscores():
    return FakeEntity(id='fake', values=[(b'1', 0), (b'2', 1)])


@pytest.fixture
def dict_repository_cls():
    return FakeRepository


@pytest.fixture
def fake_repository_cls():
    return FakeRepository


@pytest.fixture
def fake_entity_cls():
    return FakeEntity
