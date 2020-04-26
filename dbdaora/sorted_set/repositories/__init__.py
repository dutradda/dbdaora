import itertools
from typing import (
    Any,
    ClassVar,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypedDict,
    Union,
)

from dbdaora.exceptions import RequiredClassAttributeError
from dbdaora.keys import FallbackKey
from dbdaora.repository import MemoryRepository

from ..entity import Entity, SortedSetData
from ..query import SortedSetQuery


class FallbackSortedSetData(TypedDict):
    values: Sequence[Tuple[str, float]]


class SortedSetRepository(
    MemoryRepository[Entity, SortedSetData, FallbackKey],
):
    entity_id_name: ClassVar[str]
    __skip_cls_validation__ = ('SortedSetRepository',)

    def __init_subclass__(
        cls,
        entity_name: Optional[str] = None,
        entity_cls: Optional[Type[Entity]] = None,
        key_attrs: Optional[Sequence[str]] = None,
        many_key_attrs: Optional[Type[Entity]] = None,
        entity_id_name: Optional[Type[str]] = None,
    ):
        super().__init_subclass__(
            entity_name, entity_cls, key_attrs, many_key_attrs,
        )

        entity_id_name = getattr(cls, 'entity_id_name', entity_id_name)

        if (
            cls.__name__ not in cls.__skip_cls_validation__
            and not entity_id_name
        ):
            if len(cls.key_attrs) == 1:
                cls.entity_id_name = cls.key_attrs[0]
            else:
                raise RequiredClassAttributeError(
                    cls.__name__, 'entity_id_name'
                )

    async def get_memory_data(  # type: ignore
        self, key: str, query: SortedSetQuery[FallbackKey],
    ) -> Optional[SortedSetData]:
        if query.reverse:
            return await self.memory_data_source.zrevrange(key)

        return await self.memory_data_source.zrange(key)

    async def get_fallback_data(  # type: ignore
        self,
        query: Union[SortedSetQuery[FallbackKey], Entity],
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
    ) -> Entity:
        return self.get_entity_type(query)(
            values=data,
            **{
                self.entity_id_name: query.attribute_from_key(
                    self.entity_id_name
                )
            },
        )

    def make_entity_from_fallback(  # type: ignore
        self, data: SortedSetData, query: SortedSetQuery[FallbackKey]
    ) -> Entity:
        return self.make_entity(data, query)

    async def add_memory_data(self, key: str, data: SortedSetData) -> None:
        await self.memory_data_source.zadd(key, *data)

    async def add_fallback(
        self, entity: Entity, *entities: Entity, **kwargs: Any
    ) -> None:
        await self.fallback_data_source.put(
            self.fallback_key(entity),
            {
                'values': list(itertools.chain(*entity['values']))  # type: ignore
                if isinstance(entity, dict)
                else list(itertools.chain(*entity.values))
            },
            **kwargs,
        )

    def fallback_not_found_key(  # type: ignore
        self, query: Union[SortedSetQuery[FallbackKey], Entity],
    ) -> str:
        if isinstance(query, SortedSetQuery):
            return self.memory_data_source.make_key(
                self.entity_name, 'not-found', query.attribute_from_key('id')
            )

        return self.memory_data_source.make_key(
            self.entity_name,
            'not-found',
            query[self.entity_id_name]  # type: ignore
            if isinstance(query, dict)
            else getattr(query, self.entity_id_name),
        )

    async def add_memory_data_from_fallback(  # type: ignore
        self,
        key: str,
        query: Union[SortedSetQuery[FallbackKey], Entity],
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

    def make_memory_data_from_entity(self, entity: Entity) -> SortedSetData:
        if isinstance(entity, dict):
            return self.format_memory_data(entity['values'])

        return self.format_memory_data(entity.values)

    def format_memory_data(self, data: SortedSetData) -> SortedSetData:
        data = list(itertools.chain(*data))
        data.reverse()
        return data
