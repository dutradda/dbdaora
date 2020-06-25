import dataclasses
from typing import Any, List, Optional, Sequence

from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query


@dataclasses.dataclass(init=False)
class HashQuery(Query[Any, 'HashData', FallbackKey]):
    repository: 'HashRepository[FallbackKey]'
    fields: Optional[Sequence[str]]

    def __init__(
        self,
        repository: 'HashRepository[FallbackKey]',
        *args: Any,
        memory: bool = True,
        key_parts: Optional[List[Any]] = None,
        fields: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ):
        super().__init__(
            repository, memory=memory, key_parts=key_parts, *args, **kwargs,
        )
        self.fields = fields


def make(*args: Any, **kwargs: Any) -> BaseQuery[Any, 'HashData', FallbackKey]:
    return HashQuery(*args, **kwargs)


from .repositories import HashRepository, HashData  # noqa isort:skip
