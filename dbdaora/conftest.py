import pytest

from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource


@pytest.fixture
async def dict_repository(mocker, dict_repository_cls):
    return dict_repository_cls(
        memory_data_source=DictMemoryDataSource(),
        fallback_data_source=DictFallbackDataSource(),
        expire_time=1,
    )
