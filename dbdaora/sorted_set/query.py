import dataclasses
from typing import Optional

from dbdaora.data_sources.memory import SortedSetData
from dbdaora.keys import FallbackKey

from ..entity_based.query import EntityBasedQuery


@dataclasses.dataclass
class SortedSetQuery(EntityBasedQuery[SortedSetData, FallbackKey]):
    reverse: bool = False
    withscores: bool = False
    page: Optional[int] = None
    page_size: Optional[int] = None
    min: Optional[float] = None
    max: Optional[float] = None