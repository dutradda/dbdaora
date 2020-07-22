from ...data_sources.fallback.mongodb import Key as MongoKey
from ...entity import Entity
from . import GeoSpatialService


class MongoGeoSpatialService(GeoSpatialService[Entity, MongoKey]):
    ...
