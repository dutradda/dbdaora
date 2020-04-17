import dataclasses

from dbdaora.entity import Entity
from dbdaora.query import Query
from dbdaora.keys import MemoryKey, FallbackKey
from dbdaora.data import MemoryData, FallbackData



@dataclasses.dataclass
class SortedSetQueryBase(Query[Entity, MemoryData, FallbackKey, FallbackData]):
    entity_id: str
    reverse: bool = False
    withscores: bool = False


@dataclasses.dataclass
class SortedSetByPageQuery(SortedSetQueryBase[Entity, MemoryData, FallbackKey, FallbackData]):
    page: int = 0
    page_size: int = -1


@dataclasses.dataclass
class SortedSetByScoreQuery(SortedSetQueryBase[Entity, MemoryData, FallbackKey, FallbackData]):
    min: float = float('-inf')
    max: float = float('inf')
