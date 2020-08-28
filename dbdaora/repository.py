import asyncio
import dataclasses
import re
from logging import Logger, getLogger
from typing import (  # type: ignore
    Any,
    AsyncGenerator,
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
    timeout: int = 1
    logger: Logger = getLogger(__name__)

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

    async def get_memory_data_timeout(
        self, key: str, query: 'BaseQuery[Entity, EntityData, FallbackKey]',
    ) -> Optional[EntityData]:
        try:
            return await asyncio.wait_for(
                self.get_memory_data(key, query), self.timeout
            )
        except asyncio.TimeoutError:
            self.logger.warning(
                'skip memory_data; timeout for '
                f'key={key}, timeout={self.timeout}'
            )
            return None

    async def get_fallback_data_timeout(
        self,
        query: Union['BaseQuery[Entity, EntityData, FallbackKey]', Entity],
        *,
        for_memory: bool = False,
    ) -> Optional[EntityData]:
        try:
            return await asyncio.wait_for(
                self.get_fallback_data(query, for_memory=for_memory),
                self.timeout,
            )
        except asyncio.TimeoutError:
            self.logger.warning(
                'skip fallback_data; timeout for '  # type: ignore
                f'key={self.memory_key(query)}, '
                f'timeout={self.timeout}'
            )
            raise

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

    async def add_fallback(
        self,
        entity: Entity,
        *entities: Entity,
        fallback_ttl: Optional[int] = None,
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def entity(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> Entity:
        if query.memory:
            return await self.get_memory(query)
        else:
            return await self.get_fallback(query)

    def entities(
        self, query: 'QueryMany[Entity, EntityData, FallbackKey]',
    ) -> AsyncGenerator[Entity, None]:
        if query.memory:
            return self.get_memory_many(query)
        else:
            return self.get_fallback_many(query)

    async def get_fallback_many(
        self, query: 'QueryMany[Entity, EntityData, FallbackKey]',
    ) -> AsyncGenerator[Entity, None]:
        for query_ in query.queries:
            try:
                yield await self.get_fallback(query_)
            except EntityNotFoundError:
                continue

    async def get_memory_many(
        self, query: 'QueryMany[Entity, EntityData, FallbackKey]',
    ) -> AsyncGenerator[Entity, None]:
        for query_i in query.queries:
            memory_key = self.memory_key(query_i)
            memory_data = await self.get_memory_data_timeout(
                memory_key, query_i
            )

            if memory_data:
                yield self.make_entity(memory_data, query_i)

            elif not await self.already_got_not_found(query_i):
                try:
                    fallback_data = await self.get_fallback_data_timeout(
                        query_i, for_memory=True
                    )
                except asyncio.TimeoutError:
                    ...
                else:
                    if fallback_data is None:
                        await self.set_fallback_not_found(query_i)
                    else:
                        memory_data = await self.add_memory_data_from_fallback(
                            memory_key, query_i, fallback_data
                        )
                        await self.set_expire_time(memory_key)
                        yield self.make_entity(memory_data, query_i)

    async def get_memory(
        self, query: 'Query[Entity, EntityData, FallbackKey]',
    ) -> Entity:
        memory_key = self.memory_key(query)
        memory_data = await self.get_memory_data_timeout(memory_key, query)

        if not memory_data and not await self.already_got_not_found(query):
            try:
                fallback_data = await self.get_fallback_data_timeout(
                    query, for_memory=True
                )
            except asyncio.TimeoutError:
                ...
            else:
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
        try:
            data = await self.get_fallback_data_timeout(query)
        except asyncio.TimeoutError as error:
            raise EntityNotFoundError(query) from error

        if data is None:
            raise EntityNotFoundError(query)

        return self.make_entity_from_fallback(data, query)  # type: ignore

    async def add(
        self,
        *entities: Entity,
        memory: bool = True,
        query: Optional['BaseQuery[Entity, EntityData, FallbackKey]'] = None,
        fallback_ttl: Optional[int] = None,
    ) -> None:
        if memory:
            return await self.add_memory(*entities, fallback_ttl=fallback_ttl)
        else:
            return await self.add_fallback(
                *entities, fallback_ttl=fallback_ttl
            )

    async def add_memory(
        self,
        entity: Entity,
        *entities: Entity,
        fallback_ttl: Optional[int] = None,
    ) -> None:
        memory_key = self.memory_key(entity)

        if await self.memory_data_source.exists(memory_key):
            memory_data = self.make_memory_data_from_entity(entity)
            await self.add_memory_data(memory_key, memory_data)
            await self.set_expire_time(memory_key)

        await self.add_fallback(entity, fallback_ttl=fallback_ttl)
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
        key = self.fallback_not_found_key(query)
        try:
            return bool(
                await asyncio.wait_for(
                    self.memory_data_source.exists(key), self.timeout,
                )
            )
        except asyncio.TimeoutError:
            self.logger.warning(
                'skip already_got_not_found; timeout for '
                f'key={key}, timeout={self.timeout}'
            )
            return True

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
