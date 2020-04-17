import pytest
import dataclasses
from typing import Optional, Dict, Any, TypeVar, Union

from . import SortedSetRepository
from .query import SortedSetQueryBase
from .entity import SortedSetEntity
from dbdaora.entity import Entity
from dbdaora.data_sources.fallback import FallbackDataSource
from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory import MemoryDataSource, SortedSetData
from dbdaora.data_sources.memory.dict import DictMemoryDataSource
from dbdaora.query import Query
from dbdaora.keys import MemoryKey, FallbackKey
from dbdaora.data import MemoryData, FallbackData


@dataclasses.dataclass
class DictSortedSetRepository(SortedSetRepository[str]):
    memory_data_source: DictMemoryDataSource[SortedSetEntity] = dataclasses.field(default_factory=DictMemoryDataSource)
    fallback_data_source: DictFallbackDataSource[SortedSetData] = dataclasses.field(default_factory=DictFallbackDataSource)

    def make_memory_data_from_fallback(
        self,
        query: Union[SortedSetQueryBase[SortedSetEntity, SortedSetData, str, SortedSetData], SortedSetEntity],
        data: SortedSetData,
        for_memory: bool = False,
        ) -> SortedSetData:
        if for_memory and isinstance(query, SortedSetQueryBase) and not query.withscores:
            return [d[0] for d in data]  # type: ignore

        return data

    def response(  # type: ignore
        self,
        query: SortedSetQueryBase[SortedSetEntity, SortedSetData, str, SortedSetData],
        data: SortedSetData,
    ) -> SortedSetEntity:
        return SortedSetEntity(query.entity_id, data)
