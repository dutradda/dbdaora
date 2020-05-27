import dataclasses

import pytest

from dbdaora import DatastoreBooleanRepository, DatastoreDataSource


@pytest.fixture
def fallback_data_source():
    return DatastoreDataSource()


@dataclasses.dataclass
class FakeEntity:
    id: str
    other_id: str


@pytest.fixture
def fake_entity_cls():
    return FakeEntity


@pytest.fixture
def fake_entity():
    return FakeEntity(id='fake', other_id='other_fake')


@pytest.fixture
def fake_entity2():
    return FakeEntity(id='fake2', other_id='other_fake')


@pytest.fixture
def fake_entity3():
    return FakeEntity(id='fake3', other_id='other_fake')


@pytest.fixture
def fake_boolean_repository_cls():
    class FakeRepository(DatastoreBooleanRepository, entity_cls=FakeEntity):
        key_attrs = ('other_id', 'id')

    return FakeRepository
