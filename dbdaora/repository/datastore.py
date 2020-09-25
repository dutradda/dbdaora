from typing import Any, ClassVar, Optional, Sequence, Type, get_type_hints

from google.cloud.datastore import Entity as DatastoreEntity
from google.cloud.datastore import Key
from jsondaora.serializers import OrjsonDefaultTypes

from dbdaora.entity import Entity, EntityData

from . import MemoryRepository


OrjsonDefaultTypes.types_default_map[DatastoreEntity] = lambda e: dict(**e)


class DatastoreRepository(MemoryRepository[Entity, EntityData, Key]):
    __skip_cls_validation__ = ('DatastoreRepository',)
    exclude_from_indexes: ClassVar[Sequence[str]] = ()
    exclude_all_from_indexes: ClassVar[bool] = True
    fallback_data_source_key_cls = Key

    def __init_subclass__(
        cls,
        entity_cls: Optional[Type[Entity]] = None,
        name: Optional[str] = None,
        id_name: Optional[str] = None,
        key_attrs: Optional[Sequence[str]] = None,
        many_key_attrs: Optional[Sequence[str]] = None,
    ):
        super().__init_subclass__(
            entity_cls, name, id_name, key_attrs, many_key_attrs,
        )

        if cls.__name__ not in cls.__skip_cls_validation__:
            if (
                not cls.exclude_from_indexes
                and cls.exclude_all_from_indexes
                and cls.entity_cls
            ):
                cls.exclude_from_indexes = tuple(
                    get_type_hints(cls.entity_cls).keys()
                )

    async def add_fallback(
        self, entity: Entity, *entities: Entity, **kwargs: Any
    ) -> None:
        await super().add_fallback(
            entity, *entities, exclude_from_indexes=self.exclude_from_indexes
        )
