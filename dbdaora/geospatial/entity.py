import dataclasses
from typing import Any, Protocol, TypeVar, Union

from dbdaora.data_sources.memory import GeoMember, GeoRadiusOutput


GeoSpatialData = Union[GeoMember, GeoRadiusOutput]


class GeoSpatialEntity(Protocol):
    data: GeoSpatialData

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls.__annotations__['data'] = GeoSpatialData
        dataclasses.dataclass(cls)

    def __init__(self, *, data: GeoSpatialData, **kwargs: Any):
        ...


GeoSpatialEntityHint = TypeVar('GeoSpatialEntityHint', bound=GeoSpatialEntity)
