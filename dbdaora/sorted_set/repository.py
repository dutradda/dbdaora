import itertools
from typing import Any, Optional, Sequence, Tuple, TypedDict, Union

from dbdaora.keys import FallbackKey
from dbdaora.repository import MemoryRepository

from .entity import SortedSetData, SortedSetEntity
from .query import SortedSetQuery


class FallbackSortedSetData(TypedDict):
    data: Sequence[Tuple[str, float]]


class SortedSetRepository(
    MemoryRepository[SortedSetEntity, SortedSetData, FallbackKey]
):
    entity_cls = SortedSetEntity

    async def get_memory_data(  # type: ignore
        self, key: str, query: SortedSetQuery[FallbackKey],
    ) -> Optional[SortedSetData]:
        if query.reverse:
            return await self.memory_data_source.zrevrange(key)

        return await self.memory_data_source.zrange(key)

    async def get_fallback_data(  # type: ignore
        self,
        query: Union[SortedSetQuery[FallbackKey], SortedSetEntity],
        for_memory: bool = False,
    ) -> Optional[SortedSetData]:
        data: Optional[FallbackSortedSetData]

        data = await self.fallback_data_source.get(  # type: ignore
            self.fallback_key(query)
        )

        if data is None:
            return None

        if (
            for_memory
            or isinstance(query, SortedSetQuery)
            and query.withscores
        ):
            return data['data']

        return [i[0] for i in data['data']]

    def make_entity(  # type: ignore
        self, data: SortedSetData, query: SortedSetQuery[FallbackKey]
    ) -> SortedSetEntity:
        return self.entity_cls(id=query.attribute_from_key('id'), data=data)

    def make_entity_from_fallback(  # type: ignore
        self, data: SortedSetData, query: SortedSetQuery[FallbackKey]
    ) -> SortedSetEntity:
        return self.entity_cls(id=query.attribute_from_key('id'), data=data)

    async def add_memory_data(self, key: str, data: SortedSetData) -> None:
        await self.memory_data_source.zadd(key, *data)

    async def add_fallback(
        self, entity: SortedSetEntity, *entities: SortedSetEntity
    ) -> None:
        await self.fallback_data_source.put(
            self.fallback_key(entity), {'data': entity.data}
        )

    def fallback_not_found_key(  # type: ignore
        self, query: Union[SortedSetQuery[FallbackKey], SortedSetEntity],
    ) -> str:
        if isinstance(query, SortedSetQuery):
            return self.memory_data_source.make_key(
                self.entity_name, 'not-found', query.attribute_from_key('id')
            )

        if isinstance(query, SortedSetEntity):
            return self.memory_data_source.make_key(
                self.entity_name, 'not-found', query.id
            )

    async def add_memory_data_from_fallback(  # type: ignore
        self,
        key: str,
        query: Union[SortedSetQuery[FallbackKey], SortedSetEntity],
        data: Sequence[Tuple[str, float]],
    ) -> SortedSetData:
        await self.add_memory_data(key, self.format_memory_data(data))

        if isinstance(query, SortedSetQuery) and query.withscores:
            return data

        return [i[0] for i in data]

    def make_query(
        self, *args: Any, **kwargs: Any
    ) -> SortedSetQuery[FallbackKey]:
        return SortedSetQuery(self, *args, **kwargs)

    def make_memory_data(self, entity: SortedSetEntity) -> SortedSetData:
        return self.format_memory_data(entity.data)

    def format_memory_data(self, data: SortedSetData) -> SortedSetData:
        data = list(itertools.chain(*data))
        data.reverse()
        return data
