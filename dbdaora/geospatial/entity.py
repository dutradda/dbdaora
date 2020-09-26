from typing import (  # type: ignore
    Any,
    Dict,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    TypedDict,
    Union,
    _TypedDictMeta,
)

from dbdaora.data_sources.memory import GeoMember, GeoRadiusOutput
from dbdaora.entity import init_subclass


GeoSpatialData = Union[GeoMember, GeoRadiusOutput]


class GeoSpatialEntityProtocol(Protocol):
    data: GeoSpatialData

    def __init__(self, *, data: GeoSpatialData, **kwargs: Any):
        ...


class GeoSpatialEntity(GeoSpatialEntityProtocol):
    data: GeoSpatialData

    def __init_subclass__(cls) -> None:
        init_subclass(cls, (GeoSpatialEntity,))


class GeoSpatialDictEntityMeta(_TypedDictMeta):  # type: ignore
    def __init__(
        cls, name: str, bases: Tuple[Type[Any], ...], attrs: Dict[str, Any]
    ):
        super().__init__(name, bases, attrs)
        init_subclass(cls, bases)


class GeoSpatialDictEntity(TypedDict, metaclass=GeoSpatialDictEntityMeta):
    data: GeoSpatialData


GeoSpatialEntityHint = TypeVar(
    'GeoSpatialEntityHint',
    bound=Union[GeoSpatialEntityProtocol, GeoSpatialDictEntity],
)
