from typing import Any, ClassVar, Optional, Sequence, Type, get_type_hints

from google.cloud.datastore import Key

from ..entity import HashEntity
from . import HashRepository


class DatastoreHashRepository(HashRepository[Key]):
    __skip_cls_validation__ = ('DatastoreHashRepository',)
    exclude_from_indexes: ClassVar[Sequence[str]] = ()
    exclude_all_from_indexes: ClassVar[bool] = False

    def __init_subclass__(
        cls,
        entity_name: Optional[str] = None,
        entity_cls: Optional[Type[HashEntity]] = None,
        key_attrs: Optional[Sequence[str]] = None,
        many_key_attrs: Optional[Type[HashEntity]] = None,
    ):
        super().__init_subclass__(
            entity_name, entity_cls, key_attrs, many_key_attrs,
        )

        if cls.exclude_all_from_indexes:
            cls.exclude_from_indexes = tuple(
                get_type_hints(cls.entity_cls).keys()
            )

    async def add_fallback(
        self, entity: HashEntity, *entities: HashEntity, **kwargs: Any
    ) -> None:
        await super().add_fallback(
            entity, *entities, exclude_from_indexes=self.exclude_from_indexes
        )
