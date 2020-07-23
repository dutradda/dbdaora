from logging import Logger, getLogger
from typing import Optional

from cachetools import Cache

from ...circuitbreaker import AsyncCircuitBreaker
from ...entity import Entity
from ...keys import FallbackKey
from ...repository import MemoryRepository
from ...service import Service
from ..repositories import GeoSpatialData


class GeoSpatialService(Service[Entity, GeoSpatialData, FallbackKey]):
    def __init__(
        self,
        repository: MemoryRepository[Entity, GeoSpatialData, FallbackKey],
        circuit_breaker: AsyncCircuitBreaker,
        cache: Optional[Cache] = None,
        exists_cache: Optional[Cache] = None,
        logger: Logger = getLogger(__name__),
    ):
        super().__init__(
            repository=repository,
            circuit_breaker=circuit_breaker,
            cache=None,
            exists_cache=None,
            logger=logger,
        )
        self.entity_circuit = self.repository.entity
        self.entities_circuit = self.repository.entities
