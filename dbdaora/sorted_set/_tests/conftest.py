from dataclasses import dataclass

import pytest

from dbdaora import SortedSetEntity, SortedSetRepository


@dataclass
class FakeEntity(SortedSetEntity):
    id: str


class FakeRepository(SortedSetRepository[str]):
    entity_name = 'fake'
    entity_id_name = 'id'
    key_attrs = ('id',)
    entity_cls = FakeEntity


@pytest.fixture
def fake_entity():
    return FakeEntity(id='fake', values=['1', '2'])


@pytest.fixture
def fake_entity_withscores():
    return FakeEntity(id='fake', values=[('1', 0), ('2', 1)])


@pytest.fixture
def dict_repository_cls():
    return FakeRepository
