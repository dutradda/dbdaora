import dataclasses

from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory import SortedSetData
from dbdaora.data_sources.memory.dict import DictMemoryDataSource

from . import SortedSetRepository
from .entity import SortedSetEntity


@dataclasses.dataclass
class DictSortedSetRepository(SortedSetRepository[str]):
    memory_data_source: DictMemoryDataSource[
        SortedSetEntity
    ] = dataclasses.field(default_factory=DictMemoryDataSource)
    fallback_data_source: DictFallbackDataSource[
        SortedSetData
    ] = dataclasses.field(default_factory=DictFallbackDataSource)
