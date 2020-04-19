import dataclasses
import itertools
from typing import (
    ClassVar,
    Dict,
    Iterable,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
)

from jsondaora import dataclasses as jdataclasses

from dbdaora.entity import Entity
from dbdaora.exceptions import InvalidEntityAnnotationError, InvalidQueryError
from dbdaora.keys import FallbackKey
from dbdaora.query import Query

from ..entity_based import EntityBasedRepository
from .query import HashQuery


HashData = Dict[Union[bytes, str], Union[bytes, str]]


class HashRepository(EntityBasedRepository[Entity, HashData, FallbackKey]):
    query_cls = HashQuery

    def __init_subclass__(cls) -> None:
        for generic in cls.__orig_bases__:  # type: ignore
            origin = get_origin(generic)
            if isinstance(origin, type) and issubclass(
                origin, EntityBasedRepository
            ):
                cls.entity_cls = get_args(generic)[0]
                return

        raise InvalidEntityAnnotationError(
            cls, f'Should be: {cls.__name__}[MyEntity]'
        )

    async def get_memory_data(  # type: ignore
        self, key: str, query: HashQuery[Entity, HashData, FallbackKey],
    ) -> Optional[HashData]:
        if query.fields:
            data = {
                f.encode(): v
                for f, v in zip(
                    query.fields,
                    await self.memory_data_source.hmget(key, *query.fields),
                )
                if v is not None
            }

            if not data:
                return None

            return data  # type: ignore

        data = await self.memory_data_source.hgetall(key)

        if not data:
            return None

        return data  # type: ignore

    async def get_fallback_data(
        self,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        *,
        for_memory: bool = False,
        many: bool = False,
    ) -> Optional[Union[HashData]]:
        data = await self.fallback_data_source.get(self.fallback_key(query))

        if data is None:
            return None

        if not for_memory and isinstance(query, HashQuery) and query.fields:
            return {
                f.encode(): v  # type: ignore
                for f, v in data.items()
                if isinstance(query, HashQuery)
                and query.fields
                and f in query.fields
            }

        return data

    def make_entity(
        self,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        data: HashData,
        from_fallback: bool = False,
    ) -> Entity:
        return jdataclasses.asdataclass(  # type: ignore
            data, self.entity_cls, encode=not from_fallback  # type: ignore
        )

    async def add_memory_data(
        self, key: str, data: HashData, from_fallback: bool = False
    ) -> None:
        input_data = itertools.chain(*data.items())
        await self.memory_data_source.hmset(key, *input_data)  # type: ignore

    def make_fallback_data(self, entity: Entity) -> HashData:
        return jdataclasses.asdict(entity)  # type: ignore

    def make_fallback_not_found_key(
        self, query: Union[Query[Entity, HashData, FallbackKey], Entity],
    ) -> str:
        if isinstance(query, HashQuery):
            return self.memory_data_source.make_key(
                self.entity_name, 'not-found', query.entity_id
            )

        if isinstance(query, self.entity_cls):
            return self.memory_data_source.make_key(
                self.entity_name, 'not-found', query.id  # type: ignore
            )

        raise InvalidQueryError(query)

    async def add_memory_data_from_fallback(
        self,
        key: str,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        data: HashData,
    ) -> HashData:
        await self.add_memory_data(key, data)

        if isinstance(query, HashQuery) and query.fields:
            return {
                f: v for f, v in data.items() if f.decode() in query.fields  # type: ignore
            }

        return data

    def make_memory_data(self, entity: Entity) -> HashData:
        return {
            k: '1' if v is True else '0' if v is False else v
            for k, v in dataclasses.asdict(entity).items()
        }
