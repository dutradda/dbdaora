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

from dbdaora.entity import Entity
from dbdaora.exceptions import InvalidQueryError
from dbdaora.keys import FallbackKey
from dbdaora.query import Query

from ..base import MemoryRepository
from . import serializer
from .query import HashQuery


HashData = Dict[Union[bytes, str], Union[bytes, str]]


@dataclasses.dataclass
class HashRepository(MemoryRepository[Entity, HashData, FallbackKey]):
    entity_cls: ClassVar[Type[Entity]]
    query_cls = HashQuery

    def __init_subclass__(cls) -> None:
        for generic in cls.__orig_bases__:  # type: ignore
            origin = get_origin(generic)
            if isinstance(origin, type) and issubclass(
                origin, MemoryRepository
            ):
                cls.entity_cls = get_args(generic)[0]
                break

    def memory_key(
        self, query: Union[Query[Entity, HashData, FallbackKey], Entity],
    ) -> str:
        if isinstance(query, HashQuery):
            return self.memory_data_source.make_key(
                self.entity_name, query.entity_id
            )

        if isinstance(query, self.entity_cls):
            return self.memory_data_source.make_key(
                self.entity_name, query.id  # type: ignore
            )

        raise InvalidQueryError(query)

    def fallback_key(
        self, query: Union[Query[Entity, HashData, FallbackKey], Entity],
    ) -> FallbackKey:
        if isinstance(query, HashQuery):
            return self.fallback_data_source.make_key(
                self.entity_name, query.entity_id
            )

        if isinstance(query, self.entity_cls):
            return self.fallback_data_source.make_key(
                self.entity_name, query.id  # type: ignore
            )

        raise InvalidQueryError(query)

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

        data = await self.memory_data_source.hgetall(key)

        if not data:
            return None

        return data  # type: ignore

    async def get_fallback_data(
        self,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        for_memory: bool = False,
    ) -> Optional[Union[HashData]]:
        data = await self.fallback_data_source.get(self.fallback_key(query))

        if data is None:
            return None

        if not for_memory and isinstance(query, HashQuery) and query.fields:
            return {
                f: v
                for f, v in data.items()
                if isinstance(query, HashQuery)
                and query.fields
                and f in query.fields
            }

        return data

    def response(
        self,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        data: HashData,
    ) -> Union[Entity, Iterable[Entity]]:
        return self.entity_cls(**serializer.deserialize(data))  # type: ignore

    async def add_memory_data(
        self, key: str, data: HashData, from_fallback: bool = False
    ) -> None:
        input_data = itertools.chain(*data.items())
        await self.memory_data_source.hmset(key, *input_data)  # type: ignore

    async def add_fallback(self, entity: Entity) -> None:
        raise NotImplementedError()

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
            return {f: v for f, v in data.items() if f in query.fields}

        return data
