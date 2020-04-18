import dataclasses

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey

from ..entity_based.query import EntityBasedQuery


@dataclasses.dataclass
class SortedSetQueryBase(EntityBasedQuery[Entity, EntityData, FallbackKey]):
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
