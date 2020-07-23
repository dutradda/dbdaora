import dataclasses
from typing import Union

from dbdaora.data_sources.memory import GeoMember, GeoRadiusOutput


GeoSpatialData = Union[GeoMember, GeoRadiusOutput]


@dataclasses.dataclass
class GeoSpatialEntity:
    id: str
    data: GeoSpatialData
