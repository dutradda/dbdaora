import dataclasses
from typing import Any, ClassVar, List, Optional, Sequence, Tuple, Type, Union

from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query, QueryMany


@dataclasses.dataclass(init=False)
class GeoSpatialQuery(Query[Any, 'GeoSpatialData', FallbackKey]):
    repository: 'GeoSpatialRepository[FallbackKey]'
    fields: Optional[Sequence[str]] = None

    def __init__(
        self,
        repository: 'GeoSpatialRepository[FallbackKey]',
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


@dataclasses.dataclass(init=False)
class GeoSpatialQueryMany(QueryMany[Any, 'GeoSpatialData', FallbackKey]):
    query_cls: ClassVar[Type[GeoSpatialQuery[FallbackKey]]] = GeoSpatialQuery[
        FallbackKey
    ]
    queries: Sequence[GeoSpatialQuery[FallbackKey]]  # type: ignore
    repository: 'GeoSpatialRepository[FallbackKey]'
    fields: Optional[Sequence[str]] = None

    def __init__(
        self,
        repository: 'GeoSpatialRepository[FallbackKey]',
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
        self.fields = fields

        for query in self.queries:
            query.fields = fields


def make(
    *args: Any, **kwargs: Any
) -> BaseQuery[Any, 'GeoSpatialData', FallbackKey]:
    if kwargs.get('many') or kwargs.get('many_key_parts'):
        return GeoSpatialQueryMany(*args, **kwargs)

    return GeoSpatialQuery(*args, **kwargs)


from .repositories import (  # noqa isort:skip
    GeoSpatialData,
    GeoSpatialRepository,
)
