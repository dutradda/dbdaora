import dataclasses
from typing import Any, Generic, List, Union

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey


@dataclasses.dataclass
class Query(Generic[Entity, EntityData, FallbackKey]):
    repository: 'MemoryRepository[Entity, EntityData, FallbackKey]'
    memory: bool

    @property
    async def entity(self) -> Entity:
        return await self.repository.entity(self)

    @property
    async def delete(self) -> None:
        await self.repository.delete(self)

    @property
    async def add(self) -> None:
        await self.repository.add(query=self)


from .repositories.base import MemoryRepository  # noqa isort:skip
