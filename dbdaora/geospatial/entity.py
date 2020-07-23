import dataclasses

from dbdaora.data_sources.memory import GeoRadiusOutput


@dataclasses.dataclass
class GeoSpatialEntity:
    id: str
    data: GeoRadiusOutput
