import dataclasses
from typing import Any, Generic, Iterable, Union

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

    async def get(self) -> Union[Iterable[Entity], Entity]:
        return await self.repository.get(self)


from .repositories.memory.base import MemoryRepository  # noqa isort:skip
