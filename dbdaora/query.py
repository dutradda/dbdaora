import dataclasses
from typing import Any, Generic, List, Union

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey


@dataclasses.dataclass(init=False)
class Query(Generic[Entity, EntityData, FallbackKey]):
    repository: 'MemoryRepository[Entity, EntityData, FallbackKey]'

    def __init__(
        self,
        repository: 'MemoryRepository[Entity, EntityData, FallbackKey]',
        *args: Any,
        **kwargs: Any,
    ):
        self.repository = repository

    @property
    async def entities(self) -> List[Entity]:
        return await self.repository.entities(self)

    @property
    async def entity(self) -> Entity:
        return await self.repository.entity(self)


from .repositories.base import MemoryRepository  # noqa isort:skip
