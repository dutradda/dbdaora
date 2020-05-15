from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Any, Dict, Optional, Sequence

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
            if self.cache is not None:
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
        key_suffix = cache_key_suffix(fields, filters)
        missed_ids = []
        entities = {}

        for id_ in ids:
            key = f'{id_}{key_suffix}'
            entity = cache.get(key)
            if entity is None:
                missed_ids.append(id_)

            entities[key] = entity

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

            for entity in missed_entities:
                id_ = (
                    entity[self.repository.id_name]
                    if isinstance(entity, dict)
                    else getattr(entity, self.repository.id_name)
                )
                entities[f'{id_}{key_suffix}'] = entity

        final_entities = []

        for key, entity in entities.items():
            if entity is not None and entity is not CACHE_ALREADY_NOT_FOUND:
                final_entities.append(entity)
                cache[key] = entity

            elif entity is None:
                cache[key] = CACHE_ALREADY_NOT_FOUND

        if not final_entities:
            raise EntityNotFoundError(ids, fields, filters)

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
            if self.cache is not None:
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
        cache_key = f'{id}{cache_key_suffix(fields, filters)}'
        entity = cache.get(cache_key)

        if entity is None:
            try:
                if memory:
                    entity = await self.entity_circuit(
                        self.repository.query(id=id, fields=fields, **filters)
                    )
                else:
                    entity = await self.repository.query(
                        id=id, fields=fields, memory=False, **filters
                    ).entity

                cache[cache_key] = entity

            except EntityNotFoundError as error:
                cache[cache_key] = error
                raise

        elif isinstance(entity, EntityNotFoundError):
            raise entity

        return entity

    async def add(
        self, entity: Any, *entities: Any, memory: bool = True
    ) -> None:
        if not memory:
            await self.repository.add(entity, *entities, memory=False)
            return

        try:
            await self.add_circuit(entity, *entities)

        except CircuitBreakerError as error:
            self.logger.warning(error)
            await self.repository.add(entity, *entities, memory=False)

    async def delete(self, entity_id: str, **filters: Any) -> None:
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


def cache_key_suffix(
    fields: Optional[Sequence[str]], filters: Dict[str, Any],
) -> str:
    fields_key = ''.join(fields) if fields else ''
    filters_key = (
        ''.join(f'{f}{v}' for f, v in filters.items()) if filters else ''
    )
    return f'{fields_key}{filters_key}'


class CacheAlreadyNotFound:
    ...


CACHE_ALREADY_NOT_FOUND = CacheAlreadyNotFound()
