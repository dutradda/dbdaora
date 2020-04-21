import pytest

from dbdaora import DatastoreDataSource


@pytest.fixture
def fallback_data_source():
    return DatastoreDataSource()
