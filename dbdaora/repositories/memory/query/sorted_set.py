import dataclasses

from .base import Query

from dbdaora.entity import Entity, EntityData


@dataclasses.dataclass
class SortedSetQueryBase(Query[Entity, EntityData]):
    reverse: bool = False
    withscores: bool = False


@dataclasses.dataclass
class SortedSetByPageQuery(SortedSetQueryBase[Entity, EntityData]):
    page: int = 0
    page_size: int = -1


@dataclasses.dataclass
class SortedSetByScoreQuery(SortedSetQueryBase[Entity, EntityData]):
    min: float = float('-inf')
    max: float = float('inf')
