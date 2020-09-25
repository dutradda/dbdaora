from typing import Any

from google.cloud.datastore import Entity, Key
from jsondaora.serializers import OrjsonDefaultTypes

from dbdaora.repository.datastore import DatastoreRepository

from ..entity import GeoSpatialData, GeoSpatialEntityHint
from . import GeoSpatialRepository


OrjsonDefaultTypes.types_default_map[Entity] = lambda e: dict(**e)


class DatastoreGeoSpatialRepository(
    GeoSpatialRepository[GeoSpatialEntityHint, Key],
    DatastoreRepository[GeoSpatialEntityHint, GeoSpatialData],
):
    __skip_cls_validation__ = ('DatastoreGeoSpatialRepository',)
    exclude_from_indexes = ('latitude', 'longitude', 'member')

    async def add_fallback(
        self,
        entity: GeoSpatialEntityHint,
        *entities: GeoSpatialEntityHint,
        **kwargs: Any,
    ) -> None:
        await super().add_fallback(
            entity, *entities, exclude_from_indexes=self.exclude_from_indexes
        )
