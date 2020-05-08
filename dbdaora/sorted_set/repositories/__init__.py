import itertools
from typing import Any, Optional, Sequence, Tuple, TypedDict, Union

from dbdaora.keys import FallbackKey
from dbdaora.repository import MemoryRepository

from ..entity import SortedSetData
from ..query import SortedSetQuery


class FallbackSortedSetData(TypedDict):
    values: Sequence[Tuple[str, float]]


class SortedSetRepository(MemoryRepository[Any, SortedSetData, FallbackKey],):
    __skip_cls_validation__ = ('SortedSetRepository',)

    async def get_memory_data(  # type: ignore
        self, key: str, query: SortedSetQuery[FallbackKey],
    ) -> Optional[SortedSetData]:
        if query.reverse:
            return await self.memory_data_source.zrevrange(key)

        return await self.memory_data_source.zrange(key)

    async def get_fallback_data(
        self,
        query: Union[SortedSetQuery[FallbackKey], Any],
        for_memory: bool = False,
    ) -> Optional[SortedSetData]:
        data: Optional[FallbackSortedSetData]

        data = await self.fallback_data_source.get(  # type: ignore
            self.fallback_key(query)
        )

        if data is None:
            return None

        elif for_memory or (
            isinstance(query, SortedSetQuery) and query.withscores
        ):
            return [  # type: ignore
                (data['values'][i], data['values'][i + 1])
                for i in range(0, len(data['values']), 2)
            ]

        return [data['values'][i] for i in range(0, len(data['values']), 2)]

    def make_entity(  # type: ignore
        self, data: SortedSetData, query: SortedSetQuery[FallbackKey]
    ) -> Any:
        return self.get_entity_type(query)(
            values=data,
            **{self.id_name: query.attribute_from_key(self.id_name)},
        )

    def make_entity_from_fallback(  # type: ignore
        self, data: SortedSetData, query: SortedSetQuery[FallbackKey]
    ) -> Any:
        return self.make_entity(data, query)

    async def add_memory_data(self, key: str, data: SortedSetData) -> None:
        transaction = self.memory_data_source.multi_exec()
        transaction.delete(key)
        transaction.zadd(key, *data)
        await transaction.execute()

    async def add_fallback(
        self, entity: Any, *entities: Any, **kwargs: Any
    ) -> None:
        await self.fallback_data_source.put(
            self.fallback_key(entity),
            {
                'values': list(itertools.chain(*entity['values']))
                if isinstance(entity, dict)
                else list(itertools.chain(*entity.values))
            },
            **kwargs,
        )

    async def add_memory_data_from_fallback(  # type: ignore
        self,
        key: str,
        query: Union[SortedSetQuery[FallbackKey], Any],
        data: Sequence[Tuple[str, float]],
    ) -> SortedSetData:
        await self.memory_data_source.zadd(key, *self.format_memory_data(data))

        if isinstance(query, SortedSetQuery) and query.withscores:
            return data

        return [i[0] for i in data]

    def make_query(
        self, *args: Any, **kwargs: Any
    ) -> SortedSetQuery[FallbackKey]:
        return SortedSetQuery(self, *args, **kwargs)

    def make_memory_data_from_entity(self, entity: Any) -> SortedSetData:
        if isinstance(entity, dict):
            return self.format_memory_data(entity['values'])

        return self.format_memory_data(entity.values)

    def format_memory_data(self, data: SortedSetData) -> SortedSetData:
        data = list(itertools.chain(*data))
        data.reverse()
        return data
