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


@pytest.fixture
def async_iterator():
    return AsyncIterator


class AsyncIterator:
    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration
