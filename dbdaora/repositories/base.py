from dataclasses import dataclass
from typing import Any, ClassVar, Generic, List, Optional, Type, Union

from dbdaora import FallbackDataSource, MemoryDataSource
from dbdaora.entity import Entity, EntityData
from dbdaora.exceptions import EntityNotFoundError
from dbdaora.keys import FallbackKey


@dataclass
class MemoryRepository(Generic[Entity, EntityData, FallbackKey]):
    entity_name: ClassVar[str]
    query_cls: ClassVar[Type['Query[Entity, EntityData, FallbackKey]']]
    memory_data_source: MemoryDataSource
    fallback_data_source: FallbackDataSource[FallbackKey]
    expire_time: int

    async def get_memory_data(
        self,
        key: str,
        query: 'Query[Entity, EntityData, FallbackKey]',
        *,
        many: bool = False,
    ) -> Optional[EntityData]:
        raise NotImplementedError()

    async def get_memory_many(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> List[Optional[Entity]]:
        raise NotImplementedError()

    async def get_fallback_data(
        self,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        *,
        for_memory: bool = False,
        many: bool = False,
    ) -> Optional[EntityData]:
        raise NotImplementedError()

    async def get_fallback_many(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> List[Optional[Entity]]:
        raise NotImplementedError()

    def make_entity(
        self,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        data: EntityData,
    ) -> Entity:
        raise NotImplementedError()

    def make_entity_from_fallback(
        self,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        data: EntityData,
    ) -> Entity:
        raise NotImplementedError()

    async def add_memory_data(self, key: str, data: EntityData) -> None:
        raise NotImplementedError()

    async def add_memory_data_from_fallback(
        self,
        key: str,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        data: EntityData,
    ) -> EntityData:
        raise NotImplementedError()

    def make_memory_data(self, entity: Entity) -> EntityData:
        raise NotImplementedError()

    def memory_key(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> str:
        raise NotImplementedError()

    async def add_fallback(self, entity: Entity, *entities: Entity) -> None:
        raise NotImplementedError()

    def fallback_key(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> FallbackKey:
        raise NotImplementedError()

    def fallback_not_found_key(
        self,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        **kwargs: Any,
    ) -> str:
        raise NotImplementedError()

    async def entity(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> Entity:
        if query.memory:
            return await self.get_memory(query)
        else:
            return await self.get_fallback(query)

    async def entities(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> List[Optional[Entity]]:
        if query.memory:
            return await self.get_memory_many(query)
        else:
            return await self.get_fallback_many(query)

    async def get_memory(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> Entity:
        memory_key = self.memory_key(query)
        memory_data = await self.get_memory_data(memory_key, query)
        from_fallback = False

        if memory_data is None and not await self.already_got_not_found(query):
            fallback_data = await self.get_fallback_data(
                query, for_memory=True
            )

            if fallback_data is None:
                await self.set_fallback_not_found(query)
            else:
                memory_data = await self.add_memory_data_from_fallback(
                    memory_key, query, fallback_data
                )
                from_fallback = True

        if memory_data is None:
            raise EntityNotFoundError(query)

        return (
            self.make_entity_from_fallback(query, memory_data)
            if from_fallback
            else self.make_entity(query, memory_data)
        )

    async def get_fallback(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> Entity:
        data = await self.get_fallback_data(query)

        if data is None:
            raise EntityNotFoundError(query)

        return self.make_entity_from_fallback(query, data)

    async def add(
        self,
        *entities: Entity,
        memory: bool = True,
        query: Optional['Query[Entity, EntityData, FallbackKey]'] = None,
    ) -> None:
        if memory:
            return await self.add_memory(*entities)
        else:
            return await self.add_fallback(*entities)

    async def add_memory(self, entity: Entity, *entities: Entity) -> None:
        memory_key = self.memory_key(entity)
        memory_data = self.make_memory_data(entity)
        await self.add_memory_data(memory_key, memory_data)
        await self.set_expire_time(memory_key)
        await self.add_fallback(entity)
        await self.delete_fallback_not_found(entity)

    async def set_expire_time(self, key: str) -> None:
        await self.memory_data_source.expire(key, self.expire_time)

    def query(
        self, *args: Any, memory: bool = True, **kwargs: Any
    ) -> 'Query[Entity, EntityData, FallbackKey]':
        return self.query_cls(self, memory, *args, **kwargs)

    async def delete(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> None:
        if query.memory:
            await self.memory_data_source.delete(self.memory_key(query))

        await self.fallback_data_source.delete(self.fallback_key(query))

    async def already_got_not_found(
        self,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        **kwargs: Any,
    ) -> bool:
        return bool(
            await self.memory_data_source.exists(
                self.fallback_not_found_key(query, **kwargs)
            )
        )

    async def delete_fallback_not_found(
        self,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        **kwargs: Any,
    ) -> None:
        await self.memory_data_source.delete(
            self.fallback_not_found_key(query, **kwargs)
        )

    async def set_fallback_not_found(
        self,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        **kwargs: Any,
    ) -> None:
        key = self.fallback_not_found_key(query, **kwargs)
        await self.memory_data_source.set(key, '1')
        await self.set_expire_time(key)


from dbdaora.query import Query  # noqa isort:skip
