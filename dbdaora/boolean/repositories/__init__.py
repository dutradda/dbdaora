from typing import Any, AsyncGenerator, Optional, Union

from dbdaora import EntityNotFoundError, FallbackKey
from dbdaora.entity import Entity
from dbdaora.query import BaseQuery, Query, QueryMany
from dbdaora.query import make as query_factory
from dbdaora.repository import MemoryRepository


class BooleanRepository(MemoryRepository[Entity, bool, FallbackKey]):
    __skip_cls_validation__ = ('BooleanRepository',)

    async def get_memory_data(
        self, key: str, query: BaseQuery[Entity, bool, FallbackKey],
    ) -> Optional[bool]:
        value = await self.memory_data_source.get(key)
        return None if value is None else bool(int(value))

    async def get_fallback_data(  # type: ignore
        self,
        query: Union[Query[Entity, bool, FallbackKey], Entity],
        *,
        for_memory: bool = False,
    ) -> Optional[bool]:
        data = await self.fallback_data_source.get(self.fallback_key(query))

        if data is not None:
            return True

        return None

    def make_entity(  # type: ignore
        self, data: bool, query: Query[Entity, bool, FallbackKey],
    ) -> Any:
        return query.key_parts[-1]

    def make_entity_from_fallback(  # type: ignore
        self, data: bool, query: Query[Entity, bool, FallbackKey],
    ) -> Any:
        return self.make_entity(data, query)

    async def add_memory_data(
        self, key: str, data: bool, from_fallback: bool = False
    ) -> None:
        await self.memory_data_source.set(key, '1')

    async def add_memory_data_from_fallback(
        self,
        key: str,
        query: Union[BaseQuery[Entity, bool, FallbackKey], Any],
        data: bool,
    ) -> bool:
        await self.add_memory_data(key, data)
        return True

    def make_memory_data_from_entity(self, entity: Any) -> bool:
        return True

    async def add_fallback(
        self, entity: Any, *entities: Any, **kwargs: Any
    ) -> None:
        await self.fallback_data_source.put(
            self.fallback_key(entity), {'value': True}, **kwargs
        )

    def make_query(
        self, *args: Any, **kwargs: Any
    ) -> BaseQuery[Any, bool, FallbackKey]:
        return query_factory(self, *args, **kwargs)

    async def add_memory(
        self,
        entity: Entity,
        *entities: Entity,
        fallback_ttl: Optional[int] = None,
        memory_always: bool = False,
    ) -> None:
        memory_key = self.memory_key(entity)
        memory_data = self.make_memory_data_from_entity(entity)

        await self.add_memory_data(memory_key, memory_data)
        await self.set_expire_time(memory_key)
        await self.add_fallback(entity, fallback_ttl=fallback_ttl)

    async def set_fallback_not_found(
        self, query: Union[Query[Entity, bool, FallbackKey], Entity],
    ) -> None:
        memory_key = self.memory_key(query)
        await self.memory_data_source.set(memory_key, '0')
        await self.set_expire_time(memory_key)

    async def get_memory_many(
        self, query: QueryMany[Entity, bool, FallbackKey],
    ) -> AsyncGenerator[Any, None]:
        for query_i in query.queries:
            memory_key = self.memory_key(query_i)
            memory_data = await self.get_memory_data(memory_key, query_i)

            if memory_data:
                yield self.make_entity(memory_data, query_i)

            else:
                fallback_data = await self.get_fallback_data(
                    query_i, for_memory=True
                )

                if fallback_data is None:
                    await self.set_fallback_not_found(query_i)
                else:
                    memory_data = await self.add_memory_data_from_fallback(
                        memory_key, query_i, fallback_data
                    )
                    await self.set_expire_time(memory_key)
                    yield self.make_entity(memory_data, query_i)

    async def get_memory(self, query: Query[Entity, bool, FallbackKey]) -> Any:
        memory_key = self.memory_key(query)
        memory_data = await self.get_memory_data(memory_key, query)

        if memory_data is None:
            fallback_data = await self.get_fallback_data(
                query, for_memory=True
            )

            if fallback_data is None:
                await self.set_fallback_not_found(query)
            else:
                memory_data = await self.add_memory_data_from_fallback(
                    memory_key, query, fallback_data
                )
                await self.set_expire_time(memory_key)

        if not memory_data:
            raise EntityNotFoundError(query)

        return self.make_entity(memory_data, query)

    async def delete(self, query: Query[Entity, bool, FallbackKey],) -> None:
        if query.memory:
            await self.set_fallback_not_found(query)

        await self.fallback_data_source.delete(self.fallback_key(query))

    def fallback_not_found_key(
        self, query: Union[Query[Entity, bool, FallbackKey], Entity],
    ) -> str:
        return self.memory_key(query)

    async def delete_fallback_not_found(
        self, query: Union[BaseQuery[Entity, bool, FallbackKey], Entity],
    ) -> None:
        ...
