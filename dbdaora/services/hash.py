from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Generic, Iterable, List, Optional

from cachetools import Cache
from circuitbreaker import CircuitBreaker, CircuitBreakerError

from ..entity import Entity
from ..keys import FallbackKey
from ..repositories.hash import HashRepository


@dataclass(init=False)
class HashService(Generic[Entity, FallbackKey]):
    repository: HashRepository[Entity, FallbackKey]
    circuit_breaker: CircuitBreaker
    cache: Optional[Cache]
    logger: Logger

    def __init__(
        self,
        repository: HashRepository[Entity, FallbackKey],
        circuit_breaker: CircuitBreaker,
        cache: Optional[Cache] = None,
        logger: Logger = getLogger(__name__),
    ):
        self.repository = repository
        self.circuit_breaker = circuit_breaker
        self.cache = cache
        self.entity_circuit = self.circuit_breaker(self.repository.entity)
        self.entities_circuit = self.circuit_breaker(self.repository.entities)
        self.logger = logger

    async def get_all(
        self, fields: Optional[Iterable[str]] = None
    ) -> Iterable[Entity]:
        try:
            return await self.entities_circuit(
                self.repository.query(all=True, fields=fields)
            )
        except CircuitBreakerError:
            self.logger.exception(
                f'circuit-breaker={self.circuit_breaker.name}'
            )
            return await self.repository.query(
                all=True, fields=fields, memory=False
            ).entities

    async def get_many(
        self, ids: Iterable[str], fields: Optional[Iterable[str]] = None
    ) -> Iterable[Entity]:
        try:
            if self.cache is None:
                return await self.entities_circuit(
                    self.repository.query(entities_ids=ids, fields=fields)
                )

            return await self.get_many_cached(ids, self.cache, fields=fields)

        except CircuitBreakerError:
            self.logger.exception(
                f'circuit-breaker={self.circuit_breaker.name}'
            )
            if self.cache:
                return await self.get_many_cached(
                    ids, self.cache, fields=fields, memory=False
                )

            return await self.repository.query(
                entities_ids=ids, fields=fields, memory=False
            ).entities

    async def get_many_cached(
        self,
        ids: Iterable[str],
        cache: Cache,
        fields: Optional[Iterable[str]] = None,
        memory: bool = True,
    ) -> Iterable[Entity]:
        missed_ids = []
        entities = [
            missed_ids.append(id_)  # type: ignore
            if (entity := cache.get((id_, fields) if fields else id_)) is None
            else entity
            for id_ in ids
        ]

        if missed_ids:
            if memory:
                missed_entities = await self.entities_circuit(
                    self.repository.query(
                        entities_ids=missed_ids, fields=fields, memory=memory
                    )
                )
            else:
                missed_entities = await self.repository.query(
                    entities_ids=missed_ids, fields=fields, memory=False
                ).entities
            missed_index = 0

            for i, entity in enumerate(entities):
                if entity is None:
                    entities[i] = missed_entities[missed_index]
                    missed_index += 1

        return entities

    async def get_one(
        self, id: str, fields: Optional[Iterable[str]] = None
    ) -> Entity:
        try:
            if self.cache is None:
                return await self.entity_circuit(
                    self.repository.query(entity_id=id, fields=fields)
                )

            return await self.get_one_cached(id, self.cache, fields=fields)

        except CircuitBreakerError:
            self.logger.warning(f'circuit-breaker={self.circuit_breaker.name}')
            if self.cache:
                return await self.get_one_cached(
                    id, self.cache, fields=fields, memory=False
                )

            return await self.repository.query(
                entity_id=id, fields=fields, memory=False
            ).entity

    async def get_one_cached(
        self,
        id: str,
        cache: Cache,
        fields: Optional[Iterable[str]] = None,
        memory: bool = True,
    ) -> Entity:
        entity = cache.get((id, fields) if fields else id)

        if entity is None:
            if memory:
                entity = await self.entity_circuit(
                    self.repository.query(entity_id=id, fields=fields)
                )
            else:
                entity = await self.repository.query(
                    entity_id=id, fields=fields, memory=False
                ).entity

            cache[(id, fields)] = entity

        return entity
