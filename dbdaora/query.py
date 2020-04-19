import dataclasses
from typing import Any, Generic, List, Union

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey


@dataclasses.dataclass
class Query(Generic[Entity, EntityData, FallbackKey]):
    repository: 'MemoryRepository[Entity, EntityData, FallbackKey]'
    memory: bool

    @property
    async def entities(self) -> List[Entity]:
        return await self.repository.entities(self, memory=self.memory)

    @property
    async def entity(self) -> Entity:
        return await self.repository.entity(self, memory=self.memory)


from .repositories.base import MemoryRepository  # noqa isort:skip
