import pytest

from dbdaora.repositories.memory.base import MemoryRepository
from dbdaora.repositories.memory.query.base import Query


class FakeQuery(Query):
    ...


class FakeMemoryRepository(MemoryRepository[str, str]):
    ...


@pytest.fixture
def repository(mocker):
    return FakeMemoryRepository(
        memory_data_source=mocker.MagicMock(),
        fallback_data_source=mocker.MagicMock(),
        expire_time=1
    )


@pytest.mark.asyncio
async def test_should_get_from_memory(repository):
    expected_entity = 'fake'
    assert await repository.get(FakeQuery(repository)) == expected_entity
