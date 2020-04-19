from dataclasses import dataclass
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

    def __init__(
        self,
        repository: HashRepository[Entity, FallbackKey],
        circuit_breaker: CircuitBreaker,
        cache: Optional[Cache] = None,
    ):
        self.repository = repository
        self.circuit_breaker = circuit_breaker
        self.cache = cache
        self.entity = self.circuit_breaker(self.repository.entity)
        self.entities = self.circuit_breaker(self.repository.entities)

    async def get_all(
        self, fields: Optional[Iterable[str]] = None
    ) -> Iterable[Entity]:
        try:
            return await self.entities(
                self.repository.query(all=True, fields=fields)
            )
        except CircuitBreakerError:
            return await self.entities(
                self.repository.query(all=True, fields=fields, memory=False)
            )

    async def get_many(
        self, ids: Iterable[str], fields: Optional[Iterable[str]] = None
    ) -> Iterable[Entity]:
        try:
            if self.cache:
                return await self.get_many_cached(
                    ids, self.cache, fields=fields
                )

            return await self.entities(
                self.repository.query(entities_ids=ids, fields=fields)
            )

        except CircuitBreakerError:
            if self.cache:
                return await self.get_many_cached(
                    ids, self.cache, fields=fields, memory=False
                )

            return await self.entities(
                self.repository.query(
                    entities_ids=ids, fields=fields, memory=False
                )
            )

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
            missed_entities = await self.entities(
                self.repository.query(
                    entities_ids=missed_ids, fields=fields, memory=memory
                )
            )
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
            if self.cache:
                return await self.get_one_cached(id, self.cache, fields=fields)

            return await self.entity(
                self.repository.query(entity_id=id, fields=fields)
            )

        except CircuitBreakerError:
            if self.cache:
                return await self.get_one_cached(
                    id, self.cache, fields=fields, memory=False
                )

            return await self.entity(
                self.repository.query(
                    entity_id=id, fields=fields, memory=False
                )
            )

    async def get_one_cached(
        self,
        id: str,
        cache: Cache,
        fields: Optional[Iterable[str]] = None,
        memory: bool = True,
    ) -> Entity:
        entity = cache.get((id, fields) if fields else id)

        if entity is None:
            entity = await self.entity(
                self.repository.query(
                    entity_id=id, fields=fields, memory=memory
                )
            )
            cache[(id, fields)] = entity

        return entity
