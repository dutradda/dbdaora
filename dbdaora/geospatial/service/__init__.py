from ...entity import Entity
from ...keys import FallbackKey
from ...service import Service
from ..repositories import GeoSpatialData


class GeoSpatialService(Service[Entity, GeoSpatialData, FallbackKey]):
    ...
