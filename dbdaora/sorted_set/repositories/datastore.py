from typing import Any

from google.cloud.datastore import Key

from . import SortedSetRepository


class DatastoreSortedSetRepository(SortedSetRepository[Key]):
    __skip_cls_validation__ = ('DatastoreSortedSetRepository',)

    async def add_fallback(
        self, entity: Any, *entities: Any, **kwargs: Any
    ) -> None:
        await super().add_fallback(
            entity, *entities, exclude_from_indexes=('values',)
        )
