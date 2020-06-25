import asyncio
import dataclasses
import re
from typing import (  # type: ignore
    Any,
    ClassVar,
    Generic,
    List,
    Optional,
    Sequence,
    Type,
    Union,
    _TypedDictMeta,
)

from dbdaora import FallbackDataSource, MemoryDataSource
from dbdaora.entity import EntityData
from dbdaora.exceptions import (
    EntityNotFoundError,
    InvalidKeyAttributeError,
    InvalidQueryError,
    RequiredClassAttributeError,
)
from dbdaora.keys import FallbackKey

from .entity import Entity


@dataclasses.dataclass
class MemoryRepository(Generic[Entity, EntityData, FallbackKey]):
    memory_data_source: MemoryDataSource
    fallback_data_source: FallbackDataSource[FallbackKey]
    expire_time: int
    entity_cls: ClassVar[Type[Entity]]
    name: ClassVar[str]
    id_name: ClassVar[str]
    key_attrs: ClassVar[Sequence[str]]
    many_key_attrs: ClassVar[Sequence[str]]
    __skip_cls_validation__: Sequence[str] = ()

    def __init_subclass__(
        cls,
        entity_cls: Optional[Type[Entity]] = None,
        name: Optional[str] = None,
        id_name: Optional[str] = None,
        key_attrs: Optional[Sequence[str]] = None,
        many_key_attrs: Optional[Sequence[str]] = None,
    ):
        super().__init_subclass__()

        if cls.__name__ not in cls.__skip_cls_validation__:
            entity_cls = entity_cls or getattr(cls, 'entity_cls', None)
            name = name or getattr(cls, 'name', None)
            id_name = id_name or getattr(cls, 'id_name', 'id')
            key_attrs = key_attrs or getattr(cls, 'key_attrs', None)
            many_key_attrs = many_key_attrs or getattr(
                cls, 'many_key_attrs', None
            )

            if name is None:
                name = re.sub(r'(?<!^)(?=[A-Z])', '_', cls.__name__).lower()
                name = name.replace('_repository', '')

            if key_attrs is None:
                key_attrs = (id_name,)

            has_entity_type = any(
                [
                    cls.get_entity_type != base.get_entity_type  # type: ignore
                    for base in cls.__bases__
                ]
            )

            if not has_entity_type and not entity_cls:
                raise RequiredClassAttributeError(
                    cls.__name__, 'entity_cls or get_entity_type'
                )

            cls.entity_cls = entity_cls
            cls.name = name
            cls.id_name = id_name
            cls.key_attrs = key_attrs
            cls.many_key_attrs = many_key_attrs or cls.key_attrs

    async def get_memory_data(
        self, key: str, query: 'BaseQuery[Entity, EntityData, FallbackKey]',
    ) -> Optional[EntityData]:
        raise NotImplementedError()  # pragma: no cover

    async def get_fallback_data(
        self,
        query: Union['BaseQuery[Entity, EntityData, FallbackKey]', Entity],
        *,
        for_memory: bool = False,
    ) -> Optional[EntityData]:
        raise NotImplementedError()  # pragma: no cover

    def make_entity(
        self,
        data: EntityData,
        query: 'BaseQuery[Entity, EntityData, FallbackKey]',
    ) -> Entity:
        raise NotImplementedError()  # pragma: no cover

    def make_entity_from_fallback(
        self,
        data: EntityData,
        query: 'BaseQuery[Entity, EntityData, FallbackKey]',
    ) -> Entity:
        raise NotImplementedError()  # pragma: no cover

    async def add_memory_data(self, key: str, data: EntityData) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def add_memory_data_from_fallback(
        self,
        key: str,
        query: Union['BaseQuery[Entity, EntityData, FallbackKey]', Entity],
        data: EntityData,
    ) -> EntityData:
        raise NotImplementedError()  # pragma: no cover

    def make_memory_data_from_entity(self, entity: Entity) -> EntityData:
        raise NotImplementedError()  # pragma: no cover

    async def add_fallback(self, entity: Entity, *entities: Entity) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def entity(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> Entity:
        if query.memory:
            return await self.get_memory(query)
        else:
            return await self.get_fallback(query)

    async def entities(
        self, query: 'QueryMany[Entity, EntityData, FallbackKey]',
    ) -> List[Entity]:
        tasks = []
        entities = []

        if query.memory:
            for query_ in query.queries:
                task = asyncio.create_task(self.get_memory(query_))
                task.add_done_callback(task_done_callback)
                tasks.append(task)
        else:
            for query_ in query.queries:
                task = asyncio.create_task(self.get_fallback(query_))
                task.add_done_callback(task_done_callback)
                tasks.append(task)

        for t in tasks:
            try:
                entities.append(await t)
            except EntityNotFoundError:
                continue

        if not entities:
            raise EntityNotFoundError(query)

        return entities

    async def get_memory(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> Entity:
        memory_key = self.memory_key(query)
        memory_data = await self.get_memory_data(memory_key, query)

        if not memory_data and not await self.already_got_not_found(query):
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

    async def get_fallback(
        self,
        query: Union['BaseQuery[Entity, EntityData, FallbackKey]', Entity],
    ) -> Entity:
        data = await self.get_fallback_data(query)

        if data is None:
            raise EntityNotFoundError(query)

        return self.make_entity_from_fallback(data, query)  # type: ignore

    async def add(
        self,
        *entities: Entity,
        memory: bool = True,
        query: Optional['BaseQuery[Entity, EntityData, FallbackKey]'] = None,
    ) -> None:
        if memory:
            return await self.add_memory(*entities)
        else:
            return await self.add_fallback(*entities)

    async def add_memory(self, entity: Entity, *entities: Entity) -> None:
        memory_key = self.memory_key(entity)

        if await self.memory_data_source.exists(memory_key):
            memory_data = self.make_memory_data_from_entity(entity)
            await self.add_memory_data(memory_key, memory_data)
            await self.set_expire_time(memory_key)

        await self.add_fallback(entity)
        await self.delete_fallback_not_found(entity)

    async def set_expire_time(self, key: str) -> None:
        await self.memory_data_source.expire(key, self.expire_time)

    def query(
        self,
        *args: Any,
        memory: bool = True,
        many: Optional[Sequence[Any]] = None,
        **kwargs: Any,
    ) -> 'BaseQuery[Entity, EntityData, FallbackKey]':
        return self.make_query(memory=memory, many=many, *args, **kwargs)

    def make_query(
        self, *args: Any, **kwargs: Any
    ) -> 'BaseQuery[Entity, EntityData, FallbackKey]':
        return query_factory(self, *args, **kwargs)

    async def delete(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> None:
        if query.memory:
            await self.memory_data_source.delete(self.memory_key(query))
            await self.set_fallback_not_found(query)

        await self.fallback_data_source.delete(self.fallback_key(query))

    async def exists(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> bool:
        if query.memory:
            memory_key = self.memory_key(query)
            entity_exists = await self.memory_data_source.exists(memory_key)

            if not entity_exists:
                if not await self.already_got_not_found(query):
                    try:
                        await self.get_memory(query)
                    except EntityNotFoundError:
                        return False
                else:
                    return False

        else:
            try:
                await self.get_fallback(query)
            except EntityNotFoundError:
                return False

        return True

    async def already_got_not_found(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> bool:
        return bool(
            await self.memory_data_source.exists(
                self.fallback_not_found_key(query)
            )
        )

    async def delete_fallback_not_found(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> None:
        await self.memory_data_source.delete(
            self.fallback_not_found_key(query)
        )

    async def set_fallback_not_found(
        self, query: Union['Query[Entity, EntityData, FallbackKey]', Entity],
    ) -> None:
        key = self.fallback_not_found_key(query)
        await self.memory_data_source.set(key, '1')
        await self.set_expire_time(key)

    def memory_key(
        self, query: 'Union[Query[Entity, EntityData, FallbackKey], Entity]',
    ) -> str:
        if isinstance(query, Query):
            return self.memory_data_source.make_key(
                self.name, *query.key_parts
            )

        elif isinstance(self.get_entity_type(query), _TypedDictMeta):
            return self.memory_data_source.make_key(
                self.name, *self.key_parts(query)
            )

        elif isinstance(query, self.get_entity_type(query)):
            return self.memory_data_source.make_key(
                self.name, *self.key_parts(query)
            )

        raise InvalidQueryError(query)

    def fallback_key(
        self, query: 'Union[Query[Entity, EntityData, FallbackKey], Entity]',
    ) -> FallbackKey:
        if isinstance(query, Query):
            return self.fallback_data_source.make_key(
                self.name, *query.key_parts
            )

        elif isinstance(self.get_entity_type(query), _TypedDictMeta):
            return self.fallback_data_source.make_key(
                self.name, *self.key_parts(query)
            )

        elif isinstance(query, self.get_entity_type(query)):
            return self.fallback_data_source.make_key(
                self.name, *self.key_parts(query)
            )

        raise InvalidQueryError(query)

    @classmethod
    def key_parts(cls, entity: Entity) -> List[Any]:
        if hasattr(entity, '__getitem__'):
            try:
                return [
                    entity[attr_name]  # type: ignore
                    for attr_name in cls.key_attrs
                ]
            except KeyError as error:
                raise InvalidKeyAttributeError(
                    cls.__name__, cls.key_attrs, *error.args,
                )

        try:
            return [getattr(entity, attr_name) for attr_name in cls.key_attrs]
        except AttributeError as error:
            raise InvalidKeyAttributeError(
                cls.__name__,
                cls.key_attrs,
                type(entity).__name__,
                *error.args,
            )

    def fallback_not_found_key(
        self, query: 'Union[Query[Entity, EntityData, FallbackKey], Entity]',
    ) -> str:
        if isinstance(query, Query):
            return self.memory_data_source.make_key(
                self.name, 'not-found', *query.key_parts
            )

        elif isinstance(self.get_entity_type(query), _TypedDictMeta):
            return self.memory_data_source.make_key(
                self.name, 'not-found', *self.key_parts(query)
            )

        elif isinstance(query, self.get_entity_type(query)):
            return self.memory_data_source.make_key(
                self.name, 'not-found', *self.key_parts(query)
            )

        raise InvalidQueryError(query)

    def get_entity_type(
        self,
        query: 'Union[BaseQuery[Entity, EntityData, FallbackKey], Entity]',
    ) -> Type[Entity]:
        return self.entity_cls


def task_done_callback(f: Any) -> None:
    try:
        f.result()
    except EntityNotFoundError:
        ...


from .query import BaseQuery, Query, QueryMany  # noqa isort:skip
from .query import make as query_factory  # noqa isort:skip
