import dataclasses
from typing import (
    Any,
    ClassVar,
    Generic,
    List,
    Optional,
    Sequence,
    Type,
    Union,
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
    entity_name: ClassVar[str]
    entity_cls: ClassVar[Type[Entity]]
    key_attrs: ClassVar[Sequence[str]]
    many_key_attrs: ClassVar[Union[Sequence[str], str]]
    __skip_cls_validation__: Sequence[str] = ()

    def __init_subclass__(
        cls,
        entity_name: Optional[str] = None,
        entity_cls: Optional[Type[Entity]] = None,
        key_attrs: Optional[Sequence[str]] = None,
        many_key_attrs: Optional[Type[Entity]] = None,
    ):
        super().__init_subclass__()
        entity_name = getattr(cls, 'entity_name', entity_name)
        entity_cls = getattr(cls, 'entity_cls', entity_cls)
        key_attrs = getattr(cls, 'key_attrs', key_attrs)
        many_key_attrs = getattr(cls, 'many_key_attrs', many_key_attrs)

        if cls.__name__ not in cls.__skip_cls_validation__:
            missing_attrs = []

            for name, value in (
                ('entity_name', entity_name),
                ('entity_cls', entity_cls),
                ('key_attrs', key_attrs),
            ):
                if value is None:
                    if name == 'entity_cls':
                        has_entity_type = any(
                            [
                                cls.get_entity_type != base.get_entity_type  # type: ignore
                                for base in cls.__bases__
                            ]
                        )
                        name = 'entity_cls or get_entity_type'
                    else:
                        has_entity_type = False

                    if not has_entity_type:
                        missing_attrs.append(name)

            if missing_attrs:
                raise RequiredClassAttributeError(cls.__name__, missing_attrs)

        cls.entity_name = entity_name  # type: ignore
        cls.entity_cls = entity_cls  # type: ignore
        cls.key_attrs = key_attrs  # type: ignore
        cls.many_key_attrs = many_key_attrs or key_attrs  # type: ignore

    async def get_memory_data(
        self,
        key: str,
        query: 'BaseQuery[Entity, EntityData, FallbackKey]',
        *,
        many: bool = False,
    ) -> Optional[EntityData]:
        raise NotImplementedError()  # pragma: no cover

    async def get_memory_many(
        self, query: 'QueryMany[Entity, EntityData, FallbackKey]',
    ) -> List[Entity]:
        raise NotImplementedError()  # pragma: no cover

    async def get_fallback_data(
        self,
        query: Union['BaseQuery[Entity, EntityData, FallbackKey]', Entity],
        *,
        for_memory: bool = False,
        many: bool = False,
    ) -> Optional[EntityData]:
        raise NotImplementedError()  # pragma: no cover

    async def get_fallback_many(
        self, query: 'QueryMany[Entity, EntityData, FallbackKey]',
    ) -> List[Entity]:
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
        if query.memory:
            return await self.get_memory_many(query)
        else:
            return await self.get_fallback_many(query)

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

        await self.fallback_data_source.delete(self.fallback_key(query))

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
                self.entity_name, *query.key_parts
            )

        if isinstance(query, self.get_entity_type(query)):
            return self.memory_data_source.make_key(
                self.entity_name, *self.key_parts(query)
            )

        raise InvalidQueryError(query)

    def memory_keys(
        self, query: 'QueryMany[Entity, EntityData, FallbackKey]',
    ) -> List[str]:
        return [
            self.memory_data_source.make_key(self.entity_name, *key_parts)
            for key_parts in query.many_key_parts
        ]

    def fallback_key(
        self, query: 'Union[Query[Entity, EntityData, FallbackKey], Entity]',
    ) -> FallbackKey:
        if isinstance(query, Query):
            return self.fallback_data_source.make_key(
                self.entity_name, *query.key_parts
            )

        if isinstance(query, self.get_entity_type(query)):
            return self.fallback_data_source.make_key(
                self.entity_name, *self.key_parts(query)
            )

        raise InvalidQueryError(query)

    def fallback_keys(
        self, query: 'QueryMany[Entity, EntityData, FallbackKey]',
    ) -> List[FallbackKey]:
        return [
            self.fallback_data_source.make_key(self.entity_name, *key_parts)
            for key_parts in query.many_key_parts
        ]

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
                    cls.__name__,
                    cls.key_attrs,
                    type(entity).__name__,
                    *error.args,
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
                self.entity_name, 'not-found', *query.key_parts
            )

        if isinstance(query, self.get_entity_type(query)):
            return self.memory_data_source.make_key(
                self.entity_name, 'not-found', *self.key_parts(query)
            )

        raise InvalidQueryError(query)

    def get_entity_type(
        self,
        query: 'Union[BaseQuery[Entity, EntityData, FallbackKey], Entity]',
    ) -> Type[Entity]:
        return self.entity_cls


from .query import BaseQuery, Query, QueryMany  # noqa isort:skip
from .query import make as query_factory  # noqa isort:skip
