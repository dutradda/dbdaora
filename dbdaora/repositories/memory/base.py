from dataclasses import dataclass
from typing import TypeVar, Generic, Iterable, Any, get_args, Optional, Type

from circuitbreaker import CircuitBreakerError

from ...data_sources import MemoryDataSource, FallbackDataSource
from ...exceptions import EntityNotFoundError
from ..entity import Entity, EntityData


@dataclass
class MemoryRepository(Generic[Entity, EntityData]):
    memory_data_source: MemoryDataSource
    fallback_data_source: FallbackDataSource
    expire_time: int
    key_separator: str = ':'

    async def get_memory_data(self, query: 'Query') -> Optional[EntityData]:
        raise NotImplementedError()

    async def get_fallback_data(self, query: 'Query') -> Optional[EntityData]:
        raise NotImplementedError()

    def make_data(self, entity: Entity) -> EntityData:
        raise NotImplementedError()

    async def add_memory_data(self, key: str, data: EntityData) -> None:
        raise NotImplementedError()

    async def add_fallback_data(self, key: str, data: EntityData) -> None:
        raise NotImplementedError()

    def make_entity(self, data: EntityData) -> Entity:
        raise NotImplementedError()

    async def get(self, query: 'Query') -> Entity:
        try:
            return await self.get_memory(query)
        except CircuitBreakerError:
            return await self.get_fallback(query)

    async def get_memory(self, query: 'Query') -> Entity:
        data = await self.get_memory_data(query)

        if data is None and not await self.get_fallback_not_found(query.key):
            data = await self.get_fallback_data(query)

        if data is None:
            raise EntityNotFoundError(query)

        return self.make_entity(data)

    async def get_fallback(self, query: 'Query') -> Entity:
        data = await self.get_fallback_data(query)

        if data is None:
            raise EntityNotFoundError(query)

        return self.make_entity(data)

    async def add(self, entity: Entity) -> None:
        key = self.get_query_cls().key_from_entity(entity)

        try:
            return await self.add_memory(key, entity)
        except CircuitBreakerError:
            return await self.add_fallback(key, entity)

    async def add_memory(self, key: str, entity: Entity) -> None:
        data = self.make_data(entity)
        await self.add_memory_data(key, data)
        await self.set_expire_time(key)
        await self.add_fallback_data(key, data)
        await self.delete_fallback_not_found(key)

    async def add_fallback(self, key, entity: Entity) -> None:
        data = self.make_data(entity)
        await self.add_fallback_data(key, data)

    async def get_fallback_not_found(self, key: str) -> bool:
        return bool(
            await self.memory_data_source.get(
                self.get_fallback_not_found_key(key)
            )
        )

    async def set_fallback_not_found(self, key: str) -> None:
        await self.memory_data_source.set(
            self.get_fallback_not_found_key(key), '1'
        )
        await self.set_expire_time(key)

    async def delete_fallback_not_found(self, key: str) -> None:
        await self.memory_data_source.delete(
            self.get_fallback_not_found_key(key)
        )

    async def set_expire_time(self, key: str) -> None:
        await self.memory_data_source.expire(key, self.expire_time)

    def query(self, *args: Any, **kwargs: Any) -> 'Query':
        return self.get_query_cls()(self, *args, **kwargs)

    def get_query_cls(self) -> Type['Query']:
        return get_args(type(self).__orig_bases__[0])[1]  # type: ignore

    async def delete(self, query: 'Query') -> None:
        await self.memory_data_source.delete(query.key)
        await self.fallback_data_source.delete(query.key)

    def get_entity_cls(self) -> Type[Entity]:
        return get_args(type(self).__orig_bases__[0])[0]  # type: ignore

    def get_fallback_not_found_key(self, key: str):
        return f'{key}{self.key_separator}not-found'


from .query.base import Query  # noqa isort: skip
