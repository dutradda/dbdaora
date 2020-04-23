from logging import Logger, getLogger
from typing import Generic, Optional, Protocol

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
