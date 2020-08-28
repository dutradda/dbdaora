from logging import Logger, getLogger
from typing import Any, AsyncGenerator, Optional, Sequence, Tuple, Union

from cachetools import Cache

from ...circuitbreaker import AsyncCircuitBreaker
from ...entity import Entity
from ...exceptions import CacheNotAvailableError
from ...keys import FallbackKey
from ...repository import MemoryRepository
from ...service import CacheAlreadyNotFound, Service
from ..entity import GeoSpatialData


class GeoSpatialService(Service[Entity, GeoSpatialData, FallbackKey]):
    def __init__(
        self,
        repository: MemoryRepository[Entity, GeoSpatialData, FallbackKey],
        circuit_breaker: AsyncCircuitBreaker,
        fallback_circuit_breaker: AsyncCircuitBreaker,
        cache: Optional[Cache] = None,
        exists_cache: Optional[Cache] = None,
        logger: Logger = getLogger(__name__),
    ):
        if cache is not None:
            raise CacheNotAvailableError(
                'GeoSpatialService class do not support cache'
            )

        super().__init__(
            repository=repository,
            circuit_breaker=circuit_breaker,
            fallback_circuit_breaker=fallback_circuit_breaker,
            cache=None,
            exists_cache=None,
            logger=logger,
        )
        self.entity_circuit = self.repository.entity
        self.entities_circuit = self.repository.entities

    def get_many(
        self, *ids: str, **filters: Any,
    ) -> AsyncGenerator[Entity, None]:
        raise NotImplementedError()  # pragma: no cover

    def get_many_cached(
        self,
        ids: Sequence[str],
        cache: Cache,
        memory: bool = True,
        **filters: Any,
    ) -> AsyncGenerator[Entity, None]:
        raise NotImplementedError()  # pragma: no cover

    def get_cached_entity(
        self, id: Union[str, Tuple[str, ...]], key_suffix: str, **filters: Any,
    ) -> Any:
        raise NotImplementedError()  # pragma: no cover

    def cache_key(self, id: Union[str, Tuple[str, ...]], suffix: str) -> str:
        raise NotImplementedError()  # pragma: no cover

    def set_cached_entity(
        self,
        id: Union[str, Tuple[str, ...]],
        key_suffix: str,
        entity: Union[Entity, CacheAlreadyNotFound],
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    def cache_key_suffix(self, **filters: Any) -> str:
        raise NotImplementedError()  # pragma: no cover

    async def get_one_cached(
        self, cache: Cache, memory: bool = True, **filters: Any,
    ) -> Any:
        raise NotImplementedError()  # pragma: no cover

    async def delete(
        self, entity_id: Optional[str] = None, **filters: Any
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def exists(self, id: Optional[str] = None, **filters: Any) -> bool:
        raise NotImplementedError()  # pragma: no cover

    async def exists_cached(
        self, cache: Cache, memory: bool = True, **filters: Any,
    ) -> bool:
        raise NotImplementedError()  # pragma: no cover
