from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Any, Generic, Iterable, Optional

from cachetools import Cache
from circuitbreaker import CircuitBreakerError

from ..circuitbreaker import AsyncCircuitBreaker
from ..keys import FallbackKey
from .entity import HashEntity
from .repositories import HashRepository


@dataclass(init=False)
class HashService(Generic[FallbackKey]):
    repository: HashRepository[FallbackKey]
    circuit_breaker: AsyncCircuitBreaker
    cache: Optional[Cache]
    logger: Logger

    def __init__(
        self,
        repository: HashRepository[FallbackKey],
        circuit_breaker: AsyncCircuitBreaker,
        cache: Optional[Cache] = None,
        logger: Logger = getLogger(__name__),
    ):
        self.repository = repository
        self.circuit_breaker = circuit_breaker
        self.cache = cache
        self.entity_circuit = self.circuit_breaker(self.repository.entity)
        self.entities_circuit = self.circuit_breaker(self.repository.entities)
        self.add_circuit = self.circuit_breaker(self.repository.add)
        self.delete_circuit = self.circuit_breaker(self.repository.delete)
        self.logger = logger

    async def get_all(
        self, fields: Optional[Iterable[str]] = None
    ) -> Iterable[HashEntity]:
        try:
            return await self.entities_circuit(
                self.repository.query(all=True, fields=fields)
            )
        except CircuitBreakerError as error:
            self.logger.warning(error)
            return await self.repository.query(
                all=True, fields=fields, memory=False
            ).entities

    async def get_many(
        self,
        *ids: str,
        fields: Optional[Iterable[str]] = None,
        **filters: Any,
    ) -> Iterable[HashEntity]:
        try:
            if self.cache is None:
                return [
                    entity
                    for entity in await self.entities_circuit(
                        self.repository.query(
                            many=ids, fields=fields, **filters
                        )
                    )
                    if entity is not None
                ]

            return await self.get_many_cached(
                ids, self.cache, fields=fields, **filters
            )

        except CircuitBreakerError as error:
            self.logger.warning(error)
            if self.cache:
                return await self.get_many_cached(
                    ids, self.cache, fields=fields, memory=False, **filters
                )

            return [
                entity
                for entity in await self.repository.query(
                    many=ids, fields=fields, memory=False, **filters
                ).entities
                if entity is not None
            ]

    async def get_many_cached(
        self,
        ids: Iterable[str],
        cache: Cache,
        fields: Optional[Iterable[str]] = None,
        memory: bool = True,
        **filters: Any,
    ) -> Iterable[HashEntity]:
        missed_ids = []
        entities = {
            id_: missed_ids.append(id_)  # type: ignore
            if (
                entity := cache.get(  # noqa
                    (id_, ''.join(fields)) if fields else id_
                )
            )
            is None
            else entity
            for id_ in ids
        }

        if missed_ids:
            if memory:
                missed_entities = await self.entities_circuit(
                    self.repository.query(
                        many=missed_ids, fields=fields, **filters
                    )
                )
            else:
                missed_entities = await self.repository.query(
                    many=missed_ids, fields=fields, memory=False, **filters
                ).entities

            missed_entities_map = {
                entity.id: entity for entity in missed_entities
            }

            for id_, entity in entities.items():
                if entity is None:
                    entities[id_] = missed_entities_map.get(id_)

        return [entity for entity in entities.values() if entity is not None]

    async def get_one(
        self, id: str, fields: Optional[Iterable[str]] = None, **filters: Any
    ) -> HashEntity:
        try:
            if self.cache is None:
                return await self.entity_circuit(
                    self.repository.query(id, fields=fields, **filters)
                )

            return await self.get_one_cached(
                id, self.cache, fields=fields, **filters
            )

        except CircuitBreakerError as error:
            self.logger.warning(error)
            if self.cache:
                return await self.get_one_cached(
                    id, self.cache, fields=fields, memory=False, **filters
                )

            return await self.repository.query(
                id, fields=fields, memory=False, **filters
            ).entity

    async def get_one_cached(
        self,
        id: str,
        cache: Cache,
        fields: Optional[Iterable[str]] = None,
        memory: bool = True,
        **filters: Any,
    ) -> HashEntity:
        entity = cache.get((id, fields) if fields else id)

        if entity is None:
            if memory:
                entity = await self.entity_circuit(
                    self.repository.query(id, fields=fields, **filters)
                )
            else:
                entity = await self.repository.query(
                    id, fields=fields, memory=False, **filters
                ).entity

            cache[(id, fields)] = entity

        return entity

    async def add(self, entity: HashEntity, *entities: HashEntity) -> None:
        try:
            await self.add_circuit(entity, *entities)

        except CircuitBreakerError as error:
            self.logger.warning(error)
            await self.repository.add(entity, *entities, memory=False)

    async def delete(self, entity_id: str, **filters: Any) -> None:
        try:
            await self.delete_circuit(
                self.repository.query(entity_id, **filters)
            )

        except CircuitBreakerError as error:
            self.logger.warning(error)
            await self.repository.query(
                entity_id, memory=False, **filters
            ).delete

    async def shutdown(self) -> None:
        self.repository.memory_data_source.close()
        await self.repository.memory_data_source.wait_closed()
