from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
    Generic,
    Iterable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
)

from circuitbreaker import CircuitBreakerError

from dbdaora.data import FallbackData, MemoryData
from dbdaora.data_sources import FallbackDataSource, MemoryDataSource
from dbdaora.entity import Entity
from dbdaora.exceptions import EntityNotFoundError, QueryClassRequiredError
from dbdaora.keys import FallbackKey


@dataclass
class MemoryRepository(Generic[Entity, MemoryData, FallbackKey, FallbackData]):
    entity_name: ClassVar[str]
    query_cls: ClassVar[
        Type['Query[Entity, MemoryData, FallbackKey, FallbackData]']
    ]
    expire_time: int
    memory_data_source: MemoryDataSource
    fallback_data_source: FallbackDataSource[FallbackKey, FallbackData]

    async def get_memory_data(
        self,
        key: str,
        query: 'Query[Entity, MemoryData, FallbackKey, FallbackData]',
    ) -> Optional[MemoryData]:
        raise NotImplementedError()

    async def get_fallback_data(
        self,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
        for_memory: bool = False,
    ) -> Optional[MemoryData]:
        raise NotImplementedError()

    def response(
        self,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
        data: MemoryData,
    ) -> Union[Entity, Iterable[Entity]]:
        raise NotImplementedError()

    async def add_memory_data(self, key: str, data: MemoryData) -> None:
        raise NotImplementedError()

    def make_memory_data(self, entity: Entity) -> MemoryData:
        raise NotImplementedError()

    async def add_memory_data_from_fallback(
        self,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
        data: FallbackData,
        for_memory: bool = False,
    ) -> MemoryData:
        raise NotImplementedError()

    def make_fallback_data(self, entity: Entity) -> FallbackData:
        raise NotImplementedError()

    def memory_key(
        self,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
    ) -> str:
        raise NotImplementedError()

    def fallback_key(
        self,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
    ) -> FallbackKey:
        raise NotImplementedError()

    def make_fallback_not_found_key(
        self,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
    ) -> str:
        raise NotImplementedError()

    async def get(
        self, query: 'Query[Entity, MemoryData, FallbackKey, FallbackData]'
    ) -> Union[Entity, Iterable[Entity]]:
        try:
            return await self.get_memory(query)
        except CircuitBreakerError:
            return await self.get_fallback(query)

    async def get_memory(
        self, query: 'Query[Entity, MemoryData, FallbackKey, FallbackData]'
    ) -> Union[Entity, Iterable[Entity]]:
        memory_key = self.memory_key(query)
        memory_data = await self.get_memory_data(memory_key, query)

        if memory_data is None and not await self.already_got_not_found(query):
            fallback_data = await self.get_fallback_data(
                query, for_memory=True
            )

            if fallback_data is None:
                await self.set_fallback_not_found(memory_key, query)
            else:
                await self.add_memory_data(memory_key, fallback_data)
                memory_data = await self.get_memory_data(memory_key, query)

        if memory_data is None:
            raise EntityNotFoundError(query)

        return self.response(query, memory_data)

    async def get_fallback(
        self,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
    ) -> Union[Entity, Iterable[Entity]]:
        data = await self.get_fallback_data(query)

        if data is None:
            raise EntityNotFoundError(query)

        return self.response(query, data)

    async def add(self, entity: Entity) -> None:
        try:
            return await self.add_memory(entity)
        except CircuitBreakerError:
            return await self.add_fallback(entity)

    async def add_memory(self, entity: Entity) -> None:
        memory_key = self.memory_key(entity)
        memory_data = self.make_memory_data(entity)
        await self.add_memory_data(memory_key, memory_data)
        await self.set_expire_time(memory_key)
        await self.add_fallback(entity)
        await self.delete_fallback_not_found(entity)

    async def add_fallback(self, entity: Entity) -> None:
        await self.fallback_data_source.put(
            self.fallback_key(entity), self.make_fallback_data(entity)
        )

    async def set_expire_time(self, key: str) -> None:
        await self.memory_data_source.expire(key, self.expire_time)

    def query(
        self, *args: Any, **kwargs: Any
    ) -> 'Query[Entity, MemoryData, FallbackKey, FallbackData]':
        return self.query_cls(self, *args, **kwargs)

    async def delete(
        self, query: 'Query[Entity, MemoryData, FallbackKey, FallbackData]'
    ) -> None:
        try:
            await self.memory_data_source.delete(self.memory_key(query))
        except CircuitBreakerError:
            ...
        finally:
            await self.fallback_data_source.delete(self.fallback_key(query))

    async def already_got_not_found(
        self,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
    ) -> bool:
        return bool(
            await self.memory_data_source.exists(
                self.make_fallback_not_found_key(query)
            )
        )

    async def delete_fallback_not_found(
        self,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
    ) -> None:
        await self.memory_data_source.delete(
            self.make_fallback_not_found_key(query)
        )

    async def set_fallback_not_found(
        self,
        key: str,
        query: Union[
            'Query[Entity, MemoryData, FallbackKey, FallbackData]', Entity
        ],
    ) -> None:
        await self.memory_data_source.set(
            self.make_fallback_not_found_key(query), '1'
        )
        await self.set_expire_time(key)


from dbdaora.query import Query  # noqa isort:skip
