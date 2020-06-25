import asyncio
from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Any, AsyncGenerator, Generic, Optional, Sequence, Union

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
    get_entity_timeout: float
    cache: Optional[Cache]
    logger: Logger

    def __init__(
        self,
        repository: MemoryRepository[Entity, EntityData, FallbackKey],
        circuit_breaker: AsyncCircuitBreaker,
        get_entity_timeout: float,
        cache: Optional[Cache] = None,
        exists_cache: Optional[Cache] = None,
        logger: Logger = getLogger(__name__),
    ):
        self.repository = repository
        self.circuit_breaker = circuit_breaker
        self.get_entity_timeout = get_entity_timeout
        self.cache = cache
        self.exists_cache = exists_cache
        self.entity_circuit = self.circuit_breaker(self.repository.entity)
        self.entities_circuit = self.circuit_breaker(self.repository.entities)
        self.add_circuit = self.circuit_breaker(self.repository.add)
        self.delete_circuit = self.circuit_breaker(self.repository.delete)
        self.exists_circuit = self.circuit_breaker(self.repository.exists)
        self.logger = logger

    async def get_one(
        self, id: str, fields: Optional[Sequence[str]] = None, **filters: Any
    ) -> Any:
        try:
            return await asyncio.wait_for(
                self._get_one(id, fields=fields, **filters),
                self.get_entity_timeout,
            )
        except asyncio.TimeoutError:
            self.logger.warning(
                f'Timeout get entity for {self.repository.name} with id={id}'
            )
            raise EntityNotFoundError(id)

    async def _get_one(
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
        cache_key_suffix = self.cache_key_suffix(**filters)
        entity = self.get_cached_entity(id, cache_key_suffix, fields)

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

                self.set_cached_entity(id, cache_key_suffix, entity)

            except EntityNotFoundError:
                self.set_cached_entity(
                    id, cache_key_suffix, CACHE_ALREADY_NOT_FOUND
                )
                raise

        elif entity == CACHE_ALREADY_NOT_FOUND:
            raise EntityNotFoundError(id)

        return entity

    async def get_many(
        self,
        *ids: str,
        fields: Optional[Sequence[str]] = None,
        **filters: Any,
    ) -> AsyncGenerator[Any, None]:
        tasks = []

        for id_ in ids:
            task = asyncio.create_task(
                self.get_one(id_, fields=fields, **filters)
            )
            task.add_done_callback(task_callback)
            tasks.append(task)

        for task in tasks:
            try:
                yield await task
            except EntityNotFoundError:
                continue

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
            return await asyncio.wait_for(
                self._exists(id, **filters), self.get_entity_timeout,
            )
        except asyncio.TimeoutError:
            self.logger.warning(
                f'Timeout exists for {self.repository.name} with id={id}'
            )
            return False

    async def _exists(self, id: str, **filters: Any) -> bool:
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

    async def exists_many(
        self, *ids: str, **filters: Any,
    ) -> AsyncGenerator[Any, None]:
        tasks = []

        for id_ in ids:
            task = asyncio.create_task(self.exists(id_, **filters))
            task.add_done_callback(task_callback)
            tasks.append(task)

        for task in tasks:
            yield await task

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


def task_callback(future: Any) -> None:
    try:
        future.result()
    except EntityNotFoundError:
        ...
