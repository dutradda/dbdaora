from logging import Logger, getLogger
from typing import Any, Generic, Optional, Protocol, Sequence

from cachetools import Cache

from ..circuitbreaker import AsyncCircuitBreaker
from ..entity import Entity, EntityData
from ..keys import FallbackKey
from ..repository import MemoryRepository


class Service(Protocol, Generic[Entity, EntityData, FallbackKey]):
    repository: MemoryRepository[Entity, EntityData, FallbackKey]
    circuit_breaker: AsyncCircuitBreaker
    cache: Optional[Cache]
    logger: Logger

    def __init__(
        self,
        repository: MemoryRepository[Entity, EntityData, FallbackKey],
        circuit_breaker: AsyncCircuitBreaker,
        cache: Optional[Cache] = None,
        logger: Logger = getLogger(__name__),
    ):
        ...

    async def add(self, entity: Entity, *entities: Entity) -> None:
        ...

    async def shutdown(self) -> None:
        ...

    async def get_all(
        self, fields: Optional[Sequence[str]] = None
    ) -> Sequence[Entity]:
        ...

    async def get_many(
        self,
        *ids: str,
        fields: Optional[Sequence[str]] = None,
        **filters: Any,
    ) -> Sequence[Entity]:
        ...

    async def get_one(
        self, id: str, fields: Optional[Sequence[str]] = None, **filters: Any
    ) -> Entity:
        ...
