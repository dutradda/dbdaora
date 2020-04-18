import dataclasses

import asynctest
import pytest

from dbdaora import SortedSetEntity, SortedSetQueryBase, SortedSetRepository
from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource
from dbdaora.exceptions import EntityNotFoundError


class FakeRepository(SortedSetRepository[str, str, str]):
    query_cls = SortedSetQueryBase
    entity_name = 'fake'


@pytest.fixture
def fake_repository_cls():
    return FakeRepository


@pytest.fixture
def fake_entity():
    return SortedSetEntity(id='fake', data=['1', '2'])


@pytest.fixture
def fake_entity_withscores():
    return SortedSetEntity(id='fake', data=[('1', 0), ('2', 1)])
