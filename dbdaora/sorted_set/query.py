import dataclasses
from typing import Any, List, Optional

from dbdaora.keys import FallbackKey
from dbdaora.query import Query

from .entity import Entity, SortedSetData


@dataclasses.dataclass
class SortedSetQuery(Query[Entity, SortedSetData, FallbackKey]):
    repository: 'SortedSetRepository[FallbackKey]'
    reverse: bool = False
    withscores: bool = False
    page: Optional[int] = None
    page_size: Optional[int] = None
    min_score: Optional[float] = None
    max_score: Optional[float] = None
    withmaxsize: bool = False

    def __init__(
        self,
        repository: 'SortedSetRepository[FallbackKey]',
        *args: Any,
        memory: bool = True,
        key_parts: Optional[List[Any]] = None,
        reverse: bool = False,
        withscores: bool = False,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        withmaxsize: bool = False,
        **kwargs: Any,
    ):
        super().__init__(
            repository, memory=memory, key_parts=key_parts, *args, **kwargs,
        )
        self.reverse = reverse
        self.withscores = withscores
        self.page = page
        self.page_size = page_size
        self.min_score = min_score
        self.max_score = max_score
        self.withmaxsize = withmaxsize


from .repositories import SortedSetRepository  # noqa isort:skip
