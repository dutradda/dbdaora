import pytest

from dbdaora.data_sources.memory.dict import DictMemoryDataSource


@pytest.fixture
async def repository(fake_repository_cls, fallback_data_source):
    return fake_repository_cls(
        memory_data_source=DictMemoryDataSource(),
        fallback_data_source=fallback_data_source,
        expire_time=1,
    )
