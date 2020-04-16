import dataclasses

from .base import Query


@dataclasses.dataclass
class SortedSetQueryBase(Query):
    reverse: bool = False
    withscores: bool = False


@dataclasses.dataclass
class SortedSetByPageQuery(SortedSetQueryBase):
    page: int = 0
    page_size: int = -1


@dataclasses.dataclass
class SortedSetByScoreQuery(SortedSetQueryBase):
    min: float = float('-inf')
    max: float = float('inf')
