from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
    Generic,
    Iterable,
    List,
    Optional,
    Type,
    Union,
)

from dbdaora.data_sources import FallbackDataSource, MemoryDataSource
from dbdaora.entity import Entity, EntityData
from dbdaora.exceptions import EntityNotFoundError
from dbdaora.keys import FallbackKey


@dataclass
class MemoryRepository(Generic[Entity, EntityData, FallbackKey]):
    entity_name: ClassVar[str]
    query_cls: ClassVar[Type['Query[Entity, EntityData, FallbackKey]']]
    memory_data_source: MemoryDataSource
    fallback_data_source: FallbackDataSource[FallbackKey, EntityData]
    expire_time: int

    async def get_memory_data(
        self,
        key: str,
        query: 'Query[Entity, EntityData, FallbackKey]',
        *,
        many: bool = False,
    ) -> Optional[EntityData]:
        raise NotImplementedError()

    async def get_fallback_data(
        self,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        *,
        for_memory: bool = False,
        many: bool = False,
    ) -> Optional[EntityData]:
        raise NotImplementedError()

    def make_entity(
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

    def make_fallback_data(self, entity: Entity) -> EntityData:
        raise NotImplementedError()

    def memory_key(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> str:
        raise NotImplementedError()

    def fallback_key(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> FallbackKey:
        raise NotImplementedError()

    def make_fallback_not_found_key(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> str:
        raise NotImplementedError()

    async def entity(
        self,
        query: 'Query[Entity, EntityData, FallbackKey]',
        *,
        memory: bool = True,
    ) -> Entity:
        if memory:
            return await self.get_memory(query)  # type: ignore
        else:
            return await self.get_fallback(query)  # type: ignore

    async def entities(
        self,
        query: 'Query[Entity, EntityData, FallbackKey]',
        *,
        memory: bool = True,
    ) -> List[Entity]:
        if memory:
            return await self.get_memory(query, many=True)  # type: ignore
        else:
            return await self.get_fallback(query, many=True)  # type: ignore

    async def get_memory(
        self,
        query: 'Query[Entity, EntityData, FallbackKey]',
        *,
        many: bool = False,
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
                memory_data = await self.add_memory_data_from_fallback(
                    memory_key, query, fallback_data
                )

        if memory_data is None:
            raise EntityNotFoundError(query)

        if many and isinstance(memory_data, Iterable):
            return [
                self.make_entity(query, entity_data)
                for entity_data in memory_data
            ]

        return self.make_entity(query, memory_data)

    async def get_fallback(
        self,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
        *,
        many: bool = False,
    ) -> Union[Entity, Iterable[Entity]]:
        data = await self.get_fallback_data(query)

        if data is None:
            raise EntityNotFoundError(query)

        return self.make_entity(query, data)

    async def add(
        self, entity: Entity, *entities: Entity, memory: bool = True
    ) -> None:
        if memory:
            return await self.add_memory(entity, *entities)
        else:
            return await self.add_fallback(entity, *entities)

    async def add_memory(self, entity: Entity, *entities: Entity) -> None:
        memory_key = self.memory_key(entity)
        memory_data = self.make_memory_data(entity)
        await self.add_memory_data(memory_key, memory_data)
        await self.set_expire_time(memory_key)
        await self.add_fallback(entity)
        await self.delete_fallback_not_found(entity)

    async def add_fallback(self, entity: Entity, *entities: Entity) -> None:
        await self.fallback_data_source.put(
            self.fallback_key(entity), self.make_fallback_data(entity)
        )

    async def set_expire_time(self, key: str) -> None:
        await self.memory_data_source.expire(key, self.expire_time)

    def query(
        self, *args: Any, memory: bool = True, **kwargs: Any
    ) -> 'Query[Entity, EntityData, FallbackKey]':
        return self.query_cls(self, memory, *args, **kwargs)  # type: ignore

    async def delete(
        self,
        query: 'Query[Entity, EntityData, FallbackKey]',
        *,
        memory: bool = True,
    ) -> None:
        if memory:
            await self.memory_data_source.delete(self.memory_key(query))

        await self.fallback_data_source.delete(self.fallback_key(query))

    async def already_got_not_found(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> bool:
        return bool(
            await self.memory_data_source.exists(
                self.make_fallback_not_found_key(query)
            )
        )

    async def delete_fallback_not_found(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> None:
        await self.memory_data_source.delete(
            self.make_fallback_not_found_key(query)
        )

    async def set_fallback_not_found(
        self,
        key: str,
        query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> None:
        await self.memory_data_source.set(
            self.make_fallback_not_found_key(query), '1'
        )
        await self.set_expire_time(key)


from dbdaora.query import Query  # noqa isort:skip
