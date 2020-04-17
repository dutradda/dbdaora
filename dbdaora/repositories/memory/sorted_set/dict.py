import dataclasses
from typing import Union

from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory import SortedSetData
from dbdaora.data_sources.memory.dict import DictMemoryDataSource

from . import SortedSetRepository
from .entity import SortedSetEntity
from .query import SortedSetQueryBase


@dataclasses.dataclass
class DictSortedSetRepository(SortedSetRepository[str]):
    memory_data_source: DictMemoryDataSource[
        SortedSetEntity
    ] = dataclasses.field(default_factory=DictMemoryDataSource)
    fallback_data_source: DictFallbackDataSource[
        SortedSetData
    ] = dataclasses.field(default_factory=DictFallbackDataSource)

    def make_memory_data_from_fallback(
        self,
        query: Union[
            SortedSetQueryBase[
                SortedSetEntity, SortedSetData, str, SortedSetData
            ],
            SortedSetEntity,
        ],
        data: SortedSetData,
        for_memory: bool = False,
    ) -> SortedSetData:
        if (
            for_memory
            and isinstance(query, SortedSetQueryBase)
            and not query.withscores
        ):
            return [d[0] for d in data]  # type: ignore

        return data

    def response(  # type: ignore
        self,
        query: SortedSetQueryBase[
            SortedSetEntity, SortedSetData, str, SortedSetData
        ],
        data: SortedSetData,
    ) -> SortedSetEntity:
        return SortedSetEntity(query.entity_id, data)
