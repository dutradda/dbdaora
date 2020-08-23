import pytest
from google.api_core.exceptions import GoogleAPIError

from dbdaora import DatastoreDataSource, DatastoreSortedSetRepository


@pytest.fixture
def fallback_data_source():
    return DatastoreDataSource()


@pytest.fixture
def fake_repository_cls(fake_entity_cls):
    class FakeRepository(DatastoreSortedSetRepository):
        key_attrs = ('fake_id',)
        entity_cls = fake_entity_cls

    return FakeRepository


@pytest.fixture
def fallback_cb_error():
    return GoogleAPIError
