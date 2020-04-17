import dataclasses
from typing import Optional, Union

from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource
from dbdaora.entity import Entity
from dbdaora.query import Query

from .base import MemoryRepository


@dataclasses.dataclass
class DictMemoryRepository(MemoryRepository[Entity, Entity, str, Entity]):
    memory_data_source: DictMemoryDataSource[Entity] = dataclasses.field(
        default_factory=DictMemoryDataSource
    )
    fallback_data_source: DictFallbackDataSource[Entity] = dataclasses.field(
        default_factory=DictFallbackDataSource
    )

    def make_memory_data_from_fallback(
        self,
        query: Union[Query[Entity, Entity, str, Entity], Entity],
        data: Entity,
        for_memory: bool = False,
    ) -> Entity:
        return data

    async def get_memory_data(
        self,
        key: str,
        query: Union[Query[Entity, Entity, str, Entity], Entity],
    ) -> Optional[Entity]:
        return self.memory_data_source.get_obj(key)

    async def get_fallback_data(
        self,
        query: Union[Query[Entity, Entity, str, Entity], Entity],
        for_memory: bool = False,
    ) -> Optional[Entity]:
        return await self.fallback_data_source.get(self.fallback_key(query))

    async def add_memory_data(self, key: str, data: Entity) -> None:
        self.memory_data_source.set_obj(key, data)

    def response(
        self,
        query: Union[Query[Entity, Entity, str, Entity], Entity],
        data: Entity,
    ) -> Entity:
        return data
