import dataclasses

from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource
from dbdaora.entity import Entity

from . import HashData, HashRepository


@dataclasses.dataclass
class DictHashRepository(HashRepository[Entity, str]):
    memory_data_source: DictMemoryDataSource[Entity] = dataclasses.field(
        default_factory=DictMemoryDataSource
    )
    fallback_data_source: DictFallbackDataSource[HashData] = dataclasses.field(
        default_factory=DictFallbackDataSource
    )
