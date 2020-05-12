import dataclasses

import pytest

from dbdaora import (
    DatastoreDataSource,
    DatastoreSortedSetRepository,
    SortedSetData,
)


@pytest.fixture
def fallback_data_source():
    return DatastoreDataSource()


@dataclasses.dataclass
class FakeDatastoreEntity:
    id: str
    other_id: str
    values: SortedSetData


@pytest.fixture
def fake_entity():
    return FakeDatastoreEntity(
        id='fake', other_id='other_fake', values=[('1', 0), ('2', 1)],
    )


@pytest.fixture
def fake_entity2():
    return FakeDatastoreEntity(
        id='fake2', other_id='other_fake', values=[('3', 0), ('4', 1)],
    )


@pytest.fixture
def fake_entity3():
    return FakeDatastoreEntity(
        id='fake3', other_id='other_fake', values=[('5', 0), ('6', 1)],
    )


@pytest.fixture
def serialized_fake_entity():
    return ['1', 0, '2', 1]


@pytest.fixture
def serialized_fake_entity2():
    return ['3', 0, '4', 1]


@pytest.fixture
def serialized_fake_entity3():
    return ['5', 0, '6', 1]


class FakeDatastoreSortedSetRepository(DatastoreSortedSetRepository):
    name = 'fake'
    key_attrs = ('other_id', 'id')
    many_key_attrs = ('id',)
    entity_cls = FakeDatastoreEntity


@pytest.fixture
def fake_sorted_set_repository_cls():
    return FakeDatastoreSortedSetRepository
