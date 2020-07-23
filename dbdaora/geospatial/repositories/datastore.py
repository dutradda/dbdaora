from typing import Any, ClassVar, Sequence

from google.cloud.datastore import Entity, Key
from jsondaora.serializers import OrjsonDefaultTypes

from . import GeoSpatialRepository


OrjsonDefaultTypes.types_default_map[Entity] = lambda e: dict(**e)


class DatastoreGeoSpatialRepository(GeoSpatialRepository[Key]):
    __skip_cls_validation__ = ('DatastoreGeoSpatialRepository',)
    exclude_from_indexes: ClassVar[Sequence[str]] = (
        'latitude',
        'longitude',
        'member',
    )

    async def add_fallback(
        self, entity: Any, *entities: Any, **kwargs: Any
    ) -> None:
        await super().add_fallback(
            entity, *entities, exclude_from_indexes=self.exclude_from_indexes
        )
