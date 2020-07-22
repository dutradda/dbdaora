from google.cloud.datastore import Key

from ...entity import Entity
from . import GeoSpatialService


class DatastoreGeoSpatialService(GeoSpatialService[Entity, Key]):
    ...
