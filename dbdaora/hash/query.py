import dataclasses
from typing import Any, List, Optional, Sequence, Tuple, Union

from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query, QueryMany


@dataclasses.dataclass(init=False)
class HashQuery(Query[Any, 'HashData', FallbackKey]):
    repository: 'HashRepository[FallbackKey]'
    fields: Sequence[str]

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
        self.fields = fields or []


@dataclasses.dataclass(init=False)
class HashQueryMany(
    QueryMany[Any, 'HashData', FallbackKey], HashQuery[FallbackKey]
):
    repository: 'HashRepository[FallbackKey]'
    fields: Sequence[str]

    def __init__(
        self,
        repository: 'HashRepository[FallbackKey]',
        *args: Any,
        many: List[Union[Any, Tuple[Any, ...]]],
        memory: bool = True,
        many_key_parts: Optional[List[List[Any]]] = None,
        fields: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ):
        super().__init__(
            repository,
            memory=memory,
            many=many,
            many_key_parts=many_key_parts,
            *args,
            **kwargs,
        )
        self.fields = fields or []


def make(*args: Any, **kwargs: Any) -> BaseQuery[Any, 'HashData', FallbackKey]:
    if kwargs.get('many') or kwargs.get('many_key_parts'):
        return HashQueryMany(*args, **kwargs)

    return HashQuery(*args, **kwargs)


from .repositories import HashRepository, HashData  # noqa isort:skip
