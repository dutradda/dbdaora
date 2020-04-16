import pytest
import dataclasses
from typing import Optional, Dict, Any, TypeVar

from .base import MemoryRepository
from .base import MemoryRepository
from .query.base import Query
from dbdaora.entity import EntityData, Entity
from dbdaora.exceptions import EntityNotFoundError
from dbdaora.data_sources.memory import MemoryDataSource
from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource
from .query.dict import DictQuery


@dataclasses.dataclass
class DictMemoryRepository(MemoryRepository[str, str]):
    expire_time: int = 1
    memory_data_source: DictMemoryDataSource = dataclasses.field(default_factory=DictMemoryDataSource)
    fallback_data_source: DictFallbackDataSource = dataclasses.field(default_factory=DictFallbackDataSource)
    query_cls = DictQuery

    async def get_memory_data(self, query: Query[str, str]) -> Optional[str]:
        return await self.memory_data_source.get(query.key)

    async def get_fallback_data(self, query: Query[str, str]) -> Optional[str]:
        return await self.fallback_data_source.get(query.key)

    def make_data(self, entity: str) -> str:
        return entity

    async def add_memory_data(self, key: str, data: str) -> None:
        await self.memory_data_source.set(key, data)

    async def add_fallback_data(self, key: str, data: str) -> None:
        await self.fallback_data_source.put(key, data)

    def make_entity(self, data: str) -> str:
        return data
