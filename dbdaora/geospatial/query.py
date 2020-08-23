import dataclasses
from enum import Enum
from typing import Any, List, Optional

from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query


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
    count: Optional[int] = None

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
        count: Optional[int] = None,
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
        self.count = count


def make(
    *args: Any, **kwargs: Any
) -> BaseQuery[Any, 'GeoSpatialData', FallbackKey]:
    return GeoSpatialQuery(*args, **kwargs)


from .repositories import GeoSpatialRepository  # noqa isort:skip
from .entity import GeoSpatialData  # noqa isort:skip
