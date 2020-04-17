import dataclasses
from typing import Generic, Any, Union, Iterable

from dbdaora.entity import Entity
from dbdaora.keys import MemoryKey, FallbackKey
from dbdaora.data import MemoryData, FallbackData


@dataclasses.dataclass(init=False)
class Query(Generic[Entity, MemoryData, FallbackKey, FallbackData]):
    repository: 'MemoryRepository[Entity, MemoryData, FallbackKey, FallbackData]'

    def __init__(
        self,
        repository: 'MemoryRepository[Entity, MemoryData, FallbackKey, FallbackData]',
        *args: Any,
        **kwargs: Any,
    ):
        self.repository = repository

    async def get(self) -> Union[Iterable[Entity], Entity]:
        return await self.repository.get(self)


from .repositories.memory.base import MemoryRepository  # noqa isort:skip
