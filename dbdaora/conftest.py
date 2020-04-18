import pytest

from dbdaora import DictFallbackDataSource, DictMemoryDataSource


@pytest.mark.asyncio
@pytest.fixture
async def dict_repository(fake_repository_cls, mocker):
    return fake_repository_cls(
        memory_data_source=DictMemoryDataSource(),
        fallback_data_source=DictFallbackDataSource(),
        expire_time=1,
    )
