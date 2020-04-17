import dataclasses

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey
from dbdaora.query import Query


@dataclasses.dataclass
class SortedSetQueryBase(Query[Entity, EntityData, FallbackKey]):
    entity_id: str
    reverse: bool = False
    withscores: bool = False


@dataclasses.dataclass
class SortedSetByPageQuery(
    SortedSetQueryBase[Entity, EntityData, FallbackKey]
):
    page: int = 0
    page_size: int = -1


@dataclasses.dataclass
class SortedSetByScoreQuery(
    SortedSetQueryBase[Entity, EntityData, FallbackKey]
):
    min: float = float('-inf')
    max: float = float('inf')
