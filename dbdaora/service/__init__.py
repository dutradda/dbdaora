from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Any, Generic, List, Optional, Sequence, Union

from cachetools import Cache
from circuitbreaker import CircuitBreakerError

from dbdaora.exceptions import EntityNotFoundError

from ..circuitbreaker import AsyncCircuitBreaker
from ..entity import Entity, EntityData
from ..keys import FallbackKey
from ..repository import MemoryRepository


@dataclass(init=False)
class Service(Generic[Entity, EntityData, FallbackKey]):
    repository: MemoryRepository[Entity, EntityData, FallbackKey]
    circuit_breaker: AsyncCircuitBreaker
    cache: Optional[Cache]
    logger: Logger

    def __init__(
        self,
        repository: MemoryRepository[Entity, EntityData, FallbackKey],
        circuit_breaker: AsyncCircuitBreaker,
        cache: Optional[Cache] = None,
        exists_cache: Optional[Cache] = None,
        logger: Logger = getLogger(__name__),
    ):
        self.repository = repository
        self.circuit_breaker = circuit_breaker
        self.cache = cache
        self.exists_cache = exists_cache
        self.entity_circuit = self.circuit_breaker(self.repository.entity)
        self.entities_circuit = self.circuit_breaker(self.repository.entities)
        self.add_circuit = self.circuit_breaker(self.repository.add)
        self.delete_circuit = self.circuit_breaker(self.repository.delete)
        self.exists_circuit = self.circuit_breaker(self.repository.exists)
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
        missed_ids: List[str] = []
        entities = {}
        cache_key_suffix = self.cache_key_suffix(**filters)

        for id_ in ids:
            entity = self.get_cached_entity(id_, cache_key_suffix, fields)

            if entity is None:
                missed_ids.append(id_)

            entities[id_] = entity

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
                    else entity
                    if isinstance(entity, str)
                    else getattr(entity, self.repository.id_name)
                )
                entities[id_] = entity

        final_entities = []

        for id_, entity in entities.items():
            if entity is not None and entity is not CACHE_ALREADY_NOT_FOUND:
                final_entities.append(entity)
                self.set_cached_entity(id_, cache_key_suffix, entity)

            elif entity is None:
                self.set_cached_entity(
                    id_, cache_key_suffix, CACHE_ALREADY_NOT_FOUND
                )

        if not final_entities:
            raise EntityNotFoundError(ids, fields, filters)

        return final_entities

    def get_cached_entity(
        self, id: str, key_suffix: str, fields: Optional[Sequence[str]] = None,
    ) -> Any:
        if self.cache is None:
            return None

        entity = self.cache.get(self.cache_key(id, key_suffix))

        if fields is None:
            return entity

        if isinstance(entity, dict):
            for field in fields:
                if field not in entity:
                    return None

        else:
            for field in fields:
                if hasattr(entity, field):
                    return None

        return entity

    def cache_key(self, id: str, suffix: str) -> str:
        return f'{id}{suffix}'

    def set_cached_entity(
        self,
        id: str,
        key_suffix: str,
        entity: Union[Entity, 'CacheAlreadyNotFound'],
    ) -> None:
        if self.cache is not None:
            self.cache[self.cache_key(id, key_suffix)] = entity

    def cache_key_suffix(self, **filters: Any) -> str:
        return (
            ''.join(f'{f}{v}' for f, v in filters.items()) if filters else ''
        )

    async def get_one(
        self, id: str, fields: Optional[Sequence[str]] = None, **filters: Any
    ) -> Any:
        filters[self.repository.id_name] = id

        try:
            if self.cache is None:
                return await self.entity_circuit(
                    self.repository.query(fields=fields, **filters)
                )

            return await self.get_one_cached(
                cache=self.cache, fields=fields, **filters
            )

        except CircuitBreakerError as error:
            self.logger.warning(error)
            if self.cache is not None:
                return await self.get_one_cached(
                    cache=self.cache, fields=fields, memory=False, **filters
                )

            return await self.repository.query(
                fields=fields, memory=False, **filters
            ).entity

    async def get_one_cached(
        self,
        id: str,
        cache: Cache,
        fields: Optional[Sequence[str]] = None,
        memory: bool = True,
        **filters: Any,
    ) -> Any:
        cache_key_suffix = self.cache_key_suffix(**filters)
        entity = self.get_cached_entity(id, cache_key_suffix, fields)
        filters[self.repository.id_name] = id

        if entity is None:
            try:
                if memory:
                    entity = await self.entity_circuit(
                        self.repository.query(fields=fields, **filters)
                    )
                else:
                    entity = await self.repository.query(
                        fields=fields, memory=False, **filters
                    ).entity

                self.set_cached_entity(id, cache_key_suffix, entity)

            except EntityNotFoundError:
                self.set_cached_entity(
                    id, cache_key_suffix, CACHE_ALREADY_NOT_FOUND
                )
                raise

        elif entity == CACHE_ALREADY_NOT_FOUND:
            raise EntityNotFoundError(id)

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

    async def exists(self, id: str, **filters: Any) -> bool:
        try:
            if self.exists_cache is None:
                return await self.exists_circuit(
                    self.repository.query(id=id, **filters)
                )

            return await self.exists_cached(id, self.exists_cache, **filters)

        except CircuitBreakerError as error:
            self.logger.warning(error)
            if self.exists_cache is not None:
                return await self.exists_cached(
                    id, self.exists_cache, memory=False, **filters,
                )

            return await self.repository.query(
                id=id, memory=False, **filters
            ).exists

    async def exists_cached(
        self, id: str, cache: Cache, memory: bool = True, **filters: Any,
    ) -> bool:
        cache_key = self.cache_key(id, self.cache_key_suffix(**filters))
        entity_exists = cache.get(cache_key)

        if entity_exists is None:
            if memory:
                entity_exists = await self.exists_circuit(
                    self.repository.query(id=id, **filters)
                )
            else:
                entity_exists = await self.repository.query(
                    id=id, memory=False, **filters
                ).exists

            if not entity_exists:
                cache[cache_key] = CACHE_ALREADY_NOT_FOUND
                return False
            else:
                cache[cache_key] = True

        elif entity_exists == CACHE_ALREADY_NOT_FOUND:
            return False

        return True

    async def shutdown(self) -> None:
        self.repository.memory_data_source.close()
        await self.repository.memory_data_source.wait_closed()


class CacheAlreadyNotFound:
    ...


CACHE_ALREADY_NOT_FOUND = CacheAlreadyNotFound()
