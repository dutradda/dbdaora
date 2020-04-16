import pytest
import dataclasses
from typing import Optional, Dict, Any, TypeVar

from dbdaora.repositories.memory.base import MemoryRepository
from dbdaora.repositories.memory.base import MemoryRepository
from dbdaora.repositories.memory.query.base import Query
from dbdaora.entity import EntityData, Entity
from dbdaora.exceptions import EntityNotFoundError
from dbdaora.data_sources.memory import MemoryDataSource
from dbdaora.data_sources.fallback import FallbackDataSource


DBData = TypeVar('DBData')


@dataclasses.dataclass
class DictFallbackDataSource(FallbackDataSource):
    db: Dict[str, Any] = dataclasses.field(default_factory=dict)

    async def get(self, key: str) -> Optional[DBData]:
        return self.db.get(key)

    async def put(self, key: str, data: DBData) -> None:
        self.db[key] = data

    async def delete(self, key: str) -> None:
        self.db.pop(key, None)

    async def expire(self, key: str, time: int) -> None:
        ...
