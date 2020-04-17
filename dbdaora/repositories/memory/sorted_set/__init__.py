import itertools
from dataclasses import dataclass
from typing import TypeVar, Generic, Iterable, Any, get_args, Optional, Type, ClassVar, Dict, Union

from dbdaora.data_sources import MemoryDataSource, FallbackDataSource, SortedSetData
from dbdaora.exceptions import EntityNotFoundError
from ..base import MemoryRepository
from .query import SortedSetByPageQuery, SortedSetByScoreQuery, SortedSetQueryBase
from .entity import SortedSetEntity
from dbdaora.query import Query
from dbdaora.exceptions import InvalidQueryError
from dbdaora.data import MemoryData, FallbackData
from dbdaora.keys import MemoryKey, FallbackKey


class SortedSetRepository(MemoryRepository[SortedSetEntity, SortedSetData, FallbackKey, SortedSetData]):
    query_cls: ClassVar[Type[Query[SortedSetEntity, SortedSetData, FallbackKey, SortedSetData]]]

    def memory_key(
        self,
        query: Union[Query[SortedSetEntity, SortedSetData, FallbackKey, SortedSetData], SortedSetEntity],
    ) -> str:
        if isinstance(query, SortedSetQueryBase):
            return self.memory_data_source.make_key(self.entity_name, query.entity_id)

        if isinstance(query, SortedSetEntity):
            return self.memory_data_source.make_key(self.entity_name, query.id)

        raise InvalidQueryError(query)

    def fallback_key(
        self,
        query: Union[Query[SortedSetEntity, SortedSetData, FallbackKey, SortedSetData], SortedSetEntity],
    ) -> FallbackKey:
        if isinstance(query, SortedSetQueryBase):
            return self.fallback_data_source.make_key(self.entity_name, query.entity_id)

        if isinstance(query, SortedSetEntity):
            return self.fallback_data_source.make_key(self.entity_name, query.id)

        raise InvalidQueryError(query)

    async def get_memory_data(  # type: ignore
        self, key: str, query: SortedSetQueryBase[SortedSetEntity, SortedSetData, FallbackKey, SortedSetData]
    ) -> Optional[SortedSetData]:
        if query.reverse:
            return await self.memory_data_source.zrevrange(key)

        return await self.memory_data_source.zrange(key)

    async def get_fallback_data(  # type: ignore
        self, query: Union[SortedSetQueryBase[SortedSetEntity, SortedSetData, FallbackKey, SortedSetData], SortedSetEntity], for_memory: bool = False
    ) -> Optional[SortedSetData]:
        data = await self.fallback_data_source.get(self.fallback_key(query))

        if data is None:
            return None

        if for_memory or isinstance(query, SortedSetQueryBase) and query.withscores:
            return data

        return [i[0] for i in data]  # type: ignore


    def response(  # type: ignore
        self,
        query: SortedSetQueryBase[SortedSetEntity, SortedSetData, FallbackKey, SortedSetData],
        data: SortedSetData,
    ) -> Union[SortedSetEntity, Iterable[SortedSetEntity]]:
        return SortedSetEntity(id=query.entity_id, data=data)

    async def add_memory_data(self, key: str, data: SortedSetData) -> None:
        input_data = list(itertools.chain(*data))
        input_data.reverse()
        await self.memory_data_source.zadd(key, *input_data)

    async def add_fallback(self, entity: SortedSetEntity) -> None:
        raise NotImplementedError()

    def make_fallback_not_found_key(  # type: ignore
        self,
        query: Union[SortedSetQueryBase[SortedSetEntity, SortedSetData, FallbackKey, SortedSetData], SortedSetEntity]
    ) -> str:
        if isinstance(query, SortedSetQueryBase):
            return self.memory_data_source.make_key(self.entity_name, 'not-found', query.entity_id)

        if isinstance(query, SortedSetEntity):
            return self.memory_data_source.make_key(self.entity_name, 'not-found', query.id)
