from typing import Any, ClassVar, Optional, Sequence, Type, get_type_hints

from google.cloud.datastore import Entity, Key
from jsondaora.serializers import OrjsonDefaultTypes

from . import HashRepository


OrjsonDefaultTypes.types_default_map[Entity] = lambda e: dict(**e)


class DatastoreHashRepository(HashRepository[Key]):
    __skip_cls_validation__ = ('DatastoreHashRepository',)
    exclude_from_indexes: ClassVar[Sequence[str]] = ()
    exclude_all_from_indexes: ClassVar[bool] = False

    def __init_subclass__(
        cls,
        entity_cls: Optional[Type[Any]] = None,
        name: Optional[str] = None,
        id_name: Optional[str] = None,
        key_attrs: Optional[Sequence[str]] = None,
        many_key_attrs: Optional[Sequence[str]] = None,
    ):
        super().__init_subclass__(
            entity_cls, name, id_name, key_attrs, many_key_attrs,
        )

        if cls.exclude_all_from_indexes:
            cls.exclude_from_indexes = tuple(
                get_type_hints(cls.entity_cls).keys()
            )

    async def add_fallback(
        self, entity: Any, *entities: Any, **kwargs: Any
    ) -> None:
        await super().add_fallback(
            entity, *entities, exclude_from_indexes=self.exclude_from_indexes
        )
