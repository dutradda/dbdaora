import pytest

from dbdaora import (
    DictFallbackDataSource,
    SortedSetEntity,
    SortedSetRepository,
)


@pytest.fixture
def fake_entity_cls():
    return SortedSetEntity


@pytest.fixture
def fake_entity(fake_entity_cls):
    return fake_entity_cls(id='fake', values=[b'1', b'2'])


@pytest.fixture
def fake_entity_withscores(fake_entity_cls):
    return fake_entity_cls(id='fake', values=[(b'1', 0), (b'2', 1)])


@pytest.fixture
def fake_repository_cls(fake_entity_cls):
    class FakeRepository(SortedSetRepository[str]):
        entity_cls = fake_entity_cls

    return FakeRepository


@pytest.fixture
def fallback_data_source():
    return DictFallbackDataSource()
