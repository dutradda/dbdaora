import asyncio
import itertools
from typing import (
    Any,
    Awaitable,
    ClassVar,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypedDict,
    Union,
)

from dbdaora.keys import FallbackKey
from dbdaora.repository import MemoryRepository

from ..entity import SortedSetData, SortedSetEntity
from ..query import SortedSetQuery


class FallbackSortedSetData(TypedDict):
    values: Sequence[Union[str, float]]


class SortedSetRepository(MemoryRepository[Any, SortedSetData, FallbackKey]):
    __skip_cls_validation__ = ('SortedSetRepository',)
    entity_cls: ClassVar[Type[Any]] = SortedSetEntity

    async def get_memory_data(  # type: ignore
        self, key: str, query: SortedSetQuery[FallbackKey],
    ) -> Optional[SortedSetData]:
        size_task: Optional[asyncio.Task[Any]] = None
        data_task: Awaitable[Any]

        if query.withmaxsize:
            size_task = asyncio.create_task(self.memory_data_source.zcard(key))

        if query.max_score is not None or query.min_score is not None:
            max_score, min_score = self.parse_score_limits(query)

            if query.reverse:
                data_task = asyncio.create_task(
                    self.memory_data_source.zrevrangebyscore(
                        key,
                        max=max_score,
                        min=min_score,
                        withscores=query.withscores,
                    )
                )
            else:
                data_task = asyncio.create_task(
                    self.memory_data_source.zrangebyscore(
                        key,
                        max=max_score,
                        min=min_score,
                        withscores=query.withscores,
                    )
                )

        else:
            start, stop = self.parse_page(query)

            if query.reverse:
                data_task = asyncio.create_task(
                    self.memory_data_source.zrevrange(
                        key,
                        start=start,
                        stop=stop,
                        withscores=query.withscores,
                    )
                )
            else:
                data_task = asyncio.create_task(
                    self.memory_data_source.zrange(
                        key,
                        start=start,
                        stop=stop,
                        withscores=query.withscores,
                    )
                )

        data = await data_task

        if not data:
            if size_task:
                size_task.cancel()
            return None

        return data, await size_task if size_task else None

    def parse_page(
        self, query: SortedSetQuery[FallbackKey]
    ) -> Tuple[int, int]:
        if query.page_size:
            if query.page == 1 or query.page is None:
                return 0, query.page_size - 1
            else:
                return (
                    (query.page - 1) * query.page_size,
                    query.page * query.page_size - 1,
                )

        return 0, -1

    def parse_score_limits(
        self, query: SortedSetQuery[FallbackKey]
    ) -> Tuple[float, float]:
        return (
            float('inf') if query.max_score is None else query.max_score,
            float('-inf') if query.min_score is None else query.min_score,
        )

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

        data_withscores = [
            (
                data['values'][i].encode()  # type: ignore
                if isinstance(data['values'][i], str)
                else data['values'][i],
                data['values'][i + 1],
            )
            for i in range(0, len(data['values']), 2)
        ]

        if for_memory:
            return data_withscores  # type: ignore

        return self.parse_data_from_fallback(data_withscores, query)

    def parse_data_from_fallback(
        self, data_withscores: Any, query: Any
    ) -> Optional[SortedSetData]:
        sorted_data = sorted(
            data_withscores, key=lambda v: v[1], reverse=query.reverse
        )
        maxsize = len(sorted_data) if query.withmaxsize else None

        if query.max_score is not None or query.min_score is not None:
            max_score, min_score = self.parse_score_limits(query)

            if max_score != float('inf') or min_score != float('-inf'):
                sorted_data = [
                    (member, score)
                    for member, score in sorted_data
                    if min_score <= score <= max_score
                ]

        else:
            start, stop = self.parse_page(query)

            if start != 0 or stop != -1:
                sorted_data = sorted_data[start : stop + 1]  # noqa

        if not sorted_data:
            return None

        if query.withscores:
            return (sorted_data, maxsize)  # type: ignore

        return ([member for member, score in sorted_data], maxsize)  # type: ignore

    def make_entity(  # type: ignore
        self, data: SortedSetData, query: SortedSetQuery[FallbackKey]
    ) -> Any:
        return self.get_entity_type(query)(
            values=data[0],
            max_size=data[1],
            **{
                id_name: id_value
                for id_name, id_value in zip(self.key_attrs, query.key_parts)
            },
        )

    def make_entity_from_fallback(  # type: ignore
        self, data: SortedSetData, query: SortedSetQuery[FallbackKey]
    ) -> Any:
        return self.make_entity(data, query)

    async def add_memory_data(self, key: str, data: SortedSetData) -> None:
        delete_task = asyncio.create_task(self.memory_data_source.delete(key))
        delete_task.add_done_callback(task_done_callback)
        zadd_task = asyncio.create_task(
            self.memory_data_source.zadd(key, *data)
        )
        zadd_task.add_done_callback(task_done_callback)
        await delete_task
        await zadd_task

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
    ) -> Optional[SortedSetData]:
        await self.memory_data_source.zadd(key, *self.format_memory_data(data))
        return self.parse_data_from_fallback(data, query)

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


def task_done_callback(f: Any) -> None:
    try:
        f.result()
    except Exception:  # pragma: no cover
        ...  # pragma: no cover
