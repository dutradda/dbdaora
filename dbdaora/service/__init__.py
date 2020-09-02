import asyncio
from dataclasses import dataclass
from logging import Logger, getLogger
from typing import (
    Any,
    AsyncGenerator,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from cachetools import Cache

from dbdaora.exceptions import EntityNotFoundError, RequiredKeyAttributeError

from ..circuitbreaker import AsyncCircuitBreaker, DBDaoraCircuitBreakerError
from ..entity import Entity, EntityData
from ..keys import FallbackKey
from ..repository import MemoryRepository


@dataclass(init=False)
class Service(Generic[Entity, EntityData, FallbackKey]):
    repository: MemoryRepository[Entity, EntityData, FallbackKey]
    circuit_breaker: AsyncCircuitBreaker
    fallback_circuit_breaker: AsyncCircuitBreaker
    cache: Optional[Cache]
    logger: Logger

    def __init__(
        self,
        repository: MemoryRepository[Entity, EntityData, FallbackKey],
        circuit_breaker: AsyncCircuitBreaker,
        fallback_circuit_breaker: AsyncCircuitBreaker,
        cache: Optional[Cache] = None,
        exists_cache: Optional[Cache] = None,
        logger: Logger = getLogger(__name__),
    ):
        self.repository = repository
        self.circuit_breaker = circuit_breaker
        self.fallback_circuit_breaker = fallback_circuit_breaker
        self.cache = cache
        self.exists_cache = exists_cache
        self.entity_circuit = self.circuit_breaker(self.repository.entity)
        self.add_circuit = self.circuit_breaker(self.repository.add)
        self.delete_circuit = self.circuit_breaker(self.repository.delete)
        self.exists_circuit = self.circuit_breaker(self.repository.exists)
        self.logger = logger

        self.entity_fallback_circuit = self.fallback_circuit_breaker(
            self.repository.entity
        )
        self.add_fallback_circuit = self.fallback_circuit_breaker(
            self.repository.add
        )
        self.delete_fallback_circuit = self.fallback_circuit_breaker(
            self.repository.delete
        )
        self.exists_fallback_circuit = self.fallback_circuit_breaker(
            self.repository.exists
        )

    def get_many(
        self, *ids: str, memory: bool = True, **filters: Any
    ) -> AsyncGenerator[Entity, None]:
        if self.cache is None:
            if memory:
                return self.get_many_no_cache_memory(*ids, **filters)

            return self.get_many_no_cache_fallback(*ids, **filters)

        return self.get_many_cached(ids, self.cache, **filters)

    def should_raise_not_found_error_for_fallback_circuit_breaker(
        self, error: DBDaoraCircuitBreakerError
    ) -> bool:
        if self.fallback_circuit_breaker:
            exceptions = self.fallback_circuit_breaker.expected_exception

            if not isinstance(exceptions, tuple):
                exceptions = (exceptions,)

            for fallback_exception in exceptions:
                if isinstance(error, fallback_exception):
                    return True

        return False

    async def get_many_no_cache_memory(
        self, *ids: str, **filters: Any
    ) -> AsyncGenerator[Entity, None]:
        try:
            try:
                async for entity in self.repository.query(
                    many=ids, **filters
                ).entities:
                    yield entity

                self.circuit_breaker.set_success()

            except self.circuit_breaker.expected_exception as error:
                self.circuit_breaker.set_failure('get-many-memory', error)
                raise

        except DBDaoraCircuitBreakerError as error:
            self.logger.warning(error)
            if self.should_raise_not_found_error_for_fallback_circuit_breaker(
                error
            ):
                raise EntityNotFoundError(ids, filters)

            try:
                try:
                    async for entity in self.repository.query(
                        many=ids, memory=False, **filters
                    ).entities:
                        yield entity

                    self.fallback_circuit_breaker.set_success()

                except self.fallback_circuit_breaker.expected_exception as error:
                    self.fallback_circuit_breaker.set_failure(
                        'get-many-fallback', error
                    )
                    raise

            except DBDaoraCircuitBreakerError as fallback_error:
                self.logger.warning(fallback_error)
                raise EntityNotFoundError(ids, filters)

    async def get_many_no_cache_fallback(
        self, *ids: str, **filters: Any
    ) -> AsyncGenerator[Entity, None]:
        try:
            try:
                async for entity in self.repository.query(
                    many=ids, memory=False, **filters
                ).entities:
                    yield entity

                self.fallback_circuit_breaker.set_success()

            except self.fallback_circuit_breaker.expected_exception as error:
                self.fallback_circuit_breaker.set_failure(
                    'get-many-fallback', error
                )
                raise

        except DBDaoraCircuitBreakerError as fallback_error:
            self.logger.warning(fallback_error)
            raise EntityNotFoundError(ids, filters)

    async def get_many_cached(
        self,
        ids: Sequence[str],
        cache: Cache,
        memory: bool = True,
        **filters: Any,
    ) -> AsyncGenerator[Entity, None]:
        cache_key_suffix = self.cache_key_suffix(**filters)
        futures: List[Union[Entity, asyncio.Task[Optional[Entity]]]] = []

        for id_ in ids:
            entity = self.get_cached_entity(id_, cache_key_suffix, **filters)

            if entity is None:
                futures.append(
                    asyncio.create_task(
                        self.repository_entity(
                            id_, cache_key_suffix, memory, **filters
                        )
                    )
                )
            else:
                futures.append(entity)

        for future in futures:
            if isinstance(future, asyncio.Task):
                entity = await future
                if entity is not None:
                    yield entity

            elif future is not CACHE_ALREADY_NOT_FOUND:
                yield future

    async def repository_entity(
        self,
        id_: Union[str, Tuple[str, ...]],
        cache_key_suffix: str,
        memory: bool,
        **filters: Any,
    ) -> Optional[Entity]:
        circuit = (
            self.entity_circuit if memory else self.entity_fallback_circuit
        )

        try:
            if isinstance(id_, tuple):
                entity = await circuit(
                    self.repository.query(*id_, memory=memory, **filters)
                )
            else:
                entity = await circuit(
                    self.repository.query(id_, memory=memory, **filters)
                )
        except EntityNotFoundError:
            self.set_cached_entity(
                id_, cache_key_suffix, CACHE_ALREADY_NOT_FOUND,
            )
            return None
        except DBDaoraCircuitBreakerError as error:
            self.logger.warning(error)
            if self.should_raise_not_found_error_for_fallback_circuit_breaker(
                error
            ):
                self.set_cached_entity(
                    id_, cache_key_suffix, CACHE_ALREADY_NOT_FOUND,
                )
                return None

            return await self.repository_entity(
                id_, cache_key_suffix, memory=False, **filters
            )

        self.set_cached_entity(id_, cache_key_suffix, entity)
        return entity

    def entity_id(
        self, entity: Entity, is_composed_key: bool
    ) -> Union[str, Tuple[str, ...]]:
        if is_composed_key:
            return tuple(
                entity[id_name]
                if isinstance(entity, dict)
                else entity
                if isinstance(entity, str)
                else getattr(entity, id_name)
                for id_name in self.repository.many_key_attrs
            )

        return (
            entity[self.repository.id_name]
            if isinstance(entity, dict)
            else entity
            if isinstance(entity, str)
            else getattr(entity, self.repository.id_name)
        )

    def get_cached_entity(
        self, id: Union[str, Tuple[str, ...]], key_suffix: str, **filters: Any,
    ) -> Any:
        if self.cache is None:
            return None

        return self.cache.get(self.cache_key(id, key_suffix))

    def cache_key(self, id: Union[str, Tuple[str, ...]], suffix: str) -> str:
        return f'{id}{suffix}'

    def set_cached_entity(
        self,
        id: Union[str, Tuple[str, ...]],
        key_suffix: str,
        entity: Union[Entity, 'CacheAlreadyNotFound'],
    ) -> None:
        if self.cache is not None:
            self.cache[self.cache_key(id, key_suffix)] = entity

    def cache_key_suffix(self, **filters: Any) -> str:
        return (
            ''.join(f'{f}{v}' for f, v in filters.items()) if filters else ''
        )

    async def get_one(self, id: Optional[str] = None, **filters: Any) -> Any:
        if id is not None:
            filters[self.repository.id_name] = id

        try:
            if self.cache is None:
                return await self.entity_circuit(
                    self.repository.query(**filters)
                )

            return await self.get_one_cached(cache=self.cache, **filters)

        except DBDaoraCircuitBreakerError as error:
            self.logger.warning(error)
            if self.should_raise_not_found_error_for_fallback_circuit_breaker(
                error
            ):
                raise EntityNotFoundError(id, filters)

            try:
                filters.pop('memory', None)

                if self.cache is not None:
                    return await self.get_one_cached(
                        cache=self.cache, memory=False, **filters
                    )

                return await self.entity_fallback_circuit(
                    self.repository.query(memory=False, **filters)
                )

            except DBDaoraCircuitBreakerError as fallback_error:
                self.logger.warning(fallback_error)
                raise EntityNotFoundError(id, filters)

    async def get_one_cached(
        self, cache: Cache, memory: bool = True, **filters: Any,
    ) -> Any:
        id = filters.pop(self.repository.id_name, None)

        if id is None:
            raise RequiredKeyAttributeError(
                type(self.repository).__name__,
                self.repository.id_name,
                self.repository.key_attrs,
            )

        cache_key_suffix = self.cache_key_suffix(**filters)
        entity = self.get_cached_entity(id, cache_key_suffix, **filters)

        if entity is None:
            filters[self.repository.id_name] = id

            try:
                if memory:
                    entity = await self.entity_circuit(
                        self.repository.query(**filters)
                    )
                else:
                    entity = await self.entity_fallback_circuit(
                        self.repository.query(memory=False, **filters)
                    )

                self.set_cached_entity(id, cache_key_suffix, entity)

            except EntityNotFoundError:
                self.set_cached_entity(
                    id, cache_key_suffix, CACHE_ALREADY_NOT_FOUND
                )
                raise

        elif entity is CACHE_ALREADY_NOT_FOUND:
            raise EntityNotFoundError(id)

        return entity

    async def add(
        self,
        entity: Any,
        *entities: Any,
        memory: bool = True,
        fallback_ttl: Optional[int] = None,
    ) -> None:
        if not memory:
            try:
                await self.add_fallback_circuit(
                    entity, memory=False, *entities
                )

            except DBDaoraCircuitBreakerError as fallback_error:
                self.logger.warning(fallback_error)
                raise

            return

        try:
            await self.add_circuit(entity, *entities)

        except DBDaoraCircuitBreakerError as error:
            self.logger.warning(error)
            if self.should_raise_not_found_error_for_fallback_circuit_breaker(
                error
            ):
                raise

            try:
                await self.add_fallback_circuit(
                    entity, memory=False, *entities
                )
            except DBDaoraCircuitBreakerError as fallback_error:
                self.logger.warning(fallback_error)
                raise

    async def delete(
        self, entity_id: Optional[str] = None, **filters: Any
    ) -> None:
        if entity_id is not None:
            filters[self.repository.id_name] = entity_id

        try:
            await self.delete_circuit(self.repository.query(**filters))

        except DBDaoraCircuitBreakerError as error:
            self.logger.warning(error)
            if self.should_raise_not_found_error_for_fallback_circuit_breaker(
                error
            ):
                raise

            try:
                await self.delete_fallback_circuit(
                    self.repository.query(memory=False, **filters)
                )

            except DBDaoraCircuitBreakerError as fallback_error:
                self.logger.warning(fallback_error)
                raise

    async def exists(self, id: Optional[str] = None, **filters: Any) -> bool:
        if id is not None:
            filters['id'] = id

        try:
            if self.exists_cache is None:
                return await self.exists_circuit(
                    self.repository.query(**filters)
                )

            return await self.exists_cached(self.exists_cache, **filters)

        except DBDaoraCircuitBreakerError as error:
            self.logger.warning(error)
            if self.should_raise_not_found_error_for_fallback_circuit_breaker(
                error
            ):
                raise

            if self.exists_cache is not None:
                return await self.exists_cached(
                    self.exists_cache, memory=False, **filters,
                )

            return await self.exists_fallback_circuit(
                self.repository.query(memory=False, **filters)
            )

    async def exists_cached(
        self, cache: Cache, memory: bool = True, **filters: Any,
    ) -> bool:
        id = filters.pop(self.repository.id_name, None)

        if id is None:
            raise RequiredKeyAttributeError(
                type(self.repository).__name__,
                self.repository.id_name,
                self.repository.key_attrs,
            )

        cache_key = self.cache_key(id, self.cache_key_suffix(**filters))
        entity_exists = cache.get(cache_key)

        if entity_exists is None:
            filters[self.repository.id_name] = id

            if memory:
                entity_exists = await self.exists_circuit(
                    self.repository.query(**filters)
                )
            else:
                entity_exists = await self.exists_fallback_circuit(
                    self.repository.query(memory=False, **filters)
                )

            if not entity_exists:
                cache[cache_key] = CACHE_ALREADY_NOT_FOUND
                return False
            else:
                cache[cache_key] = True

        elif entity_exists is CACHE_ALREADY_NOT_FOUND:
            return False

        return True

    async def shutdown(self) -> None:
        self.repository.memory_data_source.close()
        await self.repository.memory_data_source.wait_closed()


class CacheAlreadyNotFound:
    ...


CACHE_ALREADY_NOT_FOUND = CacheAlreadyNotFound()
