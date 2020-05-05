from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Any, Optional, Sequence

from cachetools import Cache
from circuitbreaker import CircuitBreakerError

from dbdaora.exceptions import EntityNotFoundError
from dbdaora.service import Service

from ..circuitbreaker import AsyncCircuitBreaker
from ..keys import FallbackKey
from .repositories import HashData, HashRepository


@dataclass(init=False)
class HashService(Service[Any, HashData, FallbackKey]):
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
        self, fields: Optional[Sequence[str]] = None
    ) -> Sequence[Any]:
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
        fields: Optional[Sequence[str]] = None,
        **filters: Any,
    ) -> Sequence[Any]:
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
        ids: Sequence[str],
        cache: Cache,
        fields: Optional[Sequence[str]] = None,
        memory: bool = True,
        **filters: Any,
    ) -> Sequence[Any]:
        fields_key = ''.join(fields) if fields else None
        missed_ids = []
        entities = {
            id_: missed_ids.append(id_)  # type: ignore
            if (
                entity := cache.get(  # noqa
                    (id_, fields_key) if fields_key else id_
                )
            )
            is None
            else entity
            for id_ in ids
        }

        if missed_ids:
            try:
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
            except EntityNotFoundError:
                missed_entities = []

            missed_entities_map = {
                entity[self.repository.id_name]
                if isinstance(entity, dict)
                else getattr(entity, self.repository.id_name): entity
                for entity in missed_entities
            }

            for id_, entity in entities.items():
                if entity is None:
                    entities[id_] = missed_entities_map.get(id_)

        final_entities = []

        for entity in entities.values():
            if entity is not None:
                entity_id = (
                    entity[self.repository.id_name]
                    if isinstance(entity, dict)
                    else getattr(entity, self.repository.id_name)
                )
                final_entities.append(entity)
                cache[
                    (entity_id, fields_key) if fields_key else entity_id
                ] = entity

        if not final_entities:
            raise EntityNotFoundError(ids)

        return final_entities

    async def get_one(
        self, id: str, fields: Optional[Sequence[str]] = None, **filters: Any
    ) -> Any:
        try:
            if self.cache is None:
                return await self.entity_circuit(
                    self.repository.query(id=id, fields=fields, **filters)
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
                id=id, fields=fields, memory=False, **filters
            ).entity

    async def get_one_cached(
        self,
        id: str,
        cache: Cache,
        fields: Optional[Sequence[str]] = None,
        memory: bool = True,
        **filters: Any,
    ) -> Any:
        cache_key = (id, ''.join(fields)) if fields else id
        entity = cache.get(cache_key)

        if entity is None:
            if memory:
                entity = await self.entity_circuit(
                    self.repository.query(id=id, fields=fields, **filters)
                )
            else:
                entity = await self.repository.query(
                    id=id, fields=fields, memory=False, **filters
                ).entity

            cache[cache_key] = entity

        return entity

    async def add(self, entity: Any, *entities: Any) -> None:
        try:
            await self.add_circuit(entity, *entities)

        except CircuitBreakerError as error:
            self.logger.warning(error)
            await self.repository.add(entity, *entities, memory=False)

    async def delete(self, entity_id: str, **filters: Any) -> None:
        if self.cache:
            self.cache.pop(entity_id)

            for id_, fields_key in tuple(self.cache.keys()):
                if id_ == entity_id:
                    self.cache.pop((id_, fields_key))

        try:
            await self.delete_circuit(
                self.repository.query(id=entity_id, **filters)
            )

        except CircuitBreakerError as error:
            self.logger.warning(error)
            await self.repository.query(
                id=entity_id, memory=False, **filters
            ).delete

    async def shutdown(self) -> None:
        self.repository.memory_data_source.close()
        await self.repository.memory_data_source.wait_closed()
