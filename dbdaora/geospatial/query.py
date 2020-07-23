import dataclasses
from enum import Enum
from typing import Any, ClassVar, List, Optional, Tuple, Type, Union

from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query, QueryMany


class GeoSpatialQueryType(Enum):
    RADIUS = 'radius'


@dataclasses.dataclass(init=False)
class GeoSpatialQuery(Query[Any, 'GeoSpatialData', FallbackKey]):
    repository: 'GeoSpatialRepository[FallbackKey]'
    type: GeoSpatialQueryType = GeoSpatialQueryType.RADIUS
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    max_distance: Optional[float] = None
    distance_unit: str = 'km'
    with_dist: bool = True
    with_coord: bool = True

    def __init__(
        self,
        repository: 'GeoSpatialRepository[FallbackKey]',
        *args: Any,
        memory: bool = True,
        key_parts: Optional[List[Any]] = None,
        type: GeoSpatialQueryType = GeoSpatialQueryType.RADIUS,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        max_distance: Optional[float] = None,
        distance_unit: str = 'km',
        with_dist: bool = True,
        with_coord: bool = True,
        **kwargs: Any,
    ):
        super().__init__(
            repository, memory=memory, key_parts=key_parts, *args, **kwargs,
        )
        self.type = type
        self.latitude = latitude
        self.longitude = longitude
        self.max_distance = max_distance
        self.distance_unit = distance_unit
        self.with_dist = with_dist
        self.with_coord = with_coord


@dataclasses.dataclass(init=False)
class GeoSpatialQueryMany(QueryMany[Any, 'GeoSpatialData', FallbackKey]):
    query_cls: ClassVar[Type[GeoSpatialQuery[FallbackKey]]] = GeoSpatialQuery[
        FallbackKey
    ]
    queries: List[GeoSpatialQuery[FallbackKey]]  # type: ignore
    repository: 'GeoSpatialRepository[FallbackKey]'
    type: GeoSpatialQueryType = GeoSpatialQueryType.RADIUS
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    max_distance: Optional[float] = None
    distance_unit: str = 'km'
    with_dist: bool = True
    with_coord: bool = True

    def __init__(
        self,
        repository: 'GeoSpatialRepository[FallbackKey]',
        *args: Any,
        many: List[Union[Any, Tuple[Any, ...]]],
        memory: bool = True,
        many_key_parts: Optional[List[List[Any]]] = None,
        type: GeoSpatialQueryType = GeoSpatialQueryType.RADIUS,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        max_distance: Optional[float] = None,
        distance_unit: str = 'km',
        with_dist: bool = True,
        with_coord: bool = True,
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
        self.type = type
        self.latitude = latitude
        self.longitude = longitude
        self.max_distance = max_distance
        self.distance_unit = distance_unit
        self.with_dist = with_dist
        self.with_coord = with_coord

        for query in self.queries:
            query.type = type
            query.latitude = latitude
            query.longitude = longitude
            query.max_distance = max_distance
            query.distance_unit = distance_unit
            query.with_dist = with_dist
            query.with_coord = with_coord


def make(
    *args: Any, **kwargs: Any
) -> BaseQuery[Any, 'GeoSpatialData', FallbackKey]:
    if kwargs.get('many') or kwargs.get('many_key_parts'):
        return GeoSpatialQueryMany(*args, **kwargs)

    return GeoSpatialQuery(*args, **kwargs)


from .repositories import GeoSpatialRepository  # noqa isort:skip
from .entity import GeoSpatialData  # noqa isort:skip
