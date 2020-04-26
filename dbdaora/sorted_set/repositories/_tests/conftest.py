from dataclasses import dataclass

import pytest

from dbdaora import SortedSetData, SortedSetRepository


@dataclass
class FakeEntity:
    id: str
    values: SortedSetData


class FakeRepository(SortedSetRepository[str], entity_cls=FakeEntity):
    key_attrs = ('id',)


@pytest.fixture
def fake_entity():
    return FakeEntity(id='fake', values=['1', '2'])


@pytest.fixture
def fake_entity_withscores():
    return FakeEntity(id='fake', values=[('1', 0), ('2', 1)])


@pytest.fixture
def dict_repository_cls():
    return FakeRepository
