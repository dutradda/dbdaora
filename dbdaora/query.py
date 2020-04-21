import dataclasses
from typing import Any, Generic

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey


@dataclasses.dataclass(init=False)
class Query(Generic[Entity, EntityData, FallbackKey]):
    repository: 'MemoryRepository[Entity, EntityData, FallbackKey]'
    memory: bool

    def __init__(
        self,
        repository: 'MemoryRepository[Entity, EntityData, FallbackKey]',
        memory: bool = True,
        *args: Any,
        **kwargs: Any,
    ):
        self.repository = repository
        self.memory = memory

    @property
    async def entity(self) -> Entity:
        return await self.repository.entity(self)

    @property
    async def delete(self) -> None:
        await self.repository.delete(self)

    @property
    async def add(self) -> None:
        await self.repository.add(query=self)


from .repository import MemoryRepository  # noqa isort:skip
