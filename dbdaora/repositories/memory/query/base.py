from dataclasses import dataclass
from typing import Any, Generic

from dbdaora.entity import Entity, EntityData


@dataclass(init=False)
class Query(Generic[Entity, EntityData]):
    repository: 'MemoryRepository[Entity, EntityData]'

    def __init__(
        self, repository: 'MemoryRepository[Entity, EntityData]', *args: Any, **kwargs: Any
    ):
        self.repository = repository

    @property
    def key(self) -> str:
        raise NotImplementedError()

    @classmethod
    def key_from_entity(cls, entity: Entity) -> str:
        raise NotImplementedError()

    async def get(self) -> Entity:
        return await self.repository.get(self)


from ..base import MemoryRepository  # noqa isort: skip
