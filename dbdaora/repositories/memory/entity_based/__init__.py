import dataclasses
import itertools
from typing import (
    ClassVar,
    Dict,
    Iterable,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
)

from dbdaora.entity import Entity, EntityData
from dbdaora.exceptions import InvalidEntityAnnotationError, InvalidQueryError
from dbdaora.keys import FallbackKey
from dbdaora.query import Query

from ..base import MemoryRepository
from .query import EntityBasedQuery


@dataclasses.dataclass
class EntityBasedRepository(MemoryRepository[Entity, EntityData, FallbackKey]):
    entity_cls: ClassVar[Type[Entity]]

    def __init_subclass__(cls) -> None:
        for generic in cls.__orig_bases__:  # type: ignore
            origin = get_origin(generic)
            if isinstance(origin, type) and issubclass(
                origin, MemoryRepository
            ):
                cls.entity_cls = get_args(generic)[0]
                return

        raise InvalidEntityAnnotationError(
            cls, f'Should be: {cls.__name__}[MyEntity]'
        )

    def memory_key(
        self, query: Union[Query[Entity, EntityData, FallbackKey], Entity],
    ) -> str:
        if isinstance(query, EntityBasedQuery):
            return self.memory_data_source.make_key(
                self.entity_name, query.entity_id
            )

        if isinstance(query, self.entity_cls):
            return self.memory_data_source.make_key(
                self.entity_name, query.id  # type: ignore
            )

        raise InvalidQueryError(query)

    def fallback_key(
        self, query: Union[Query[Entity, EntityData, FallbackKey], Entity],
    ) -> FallbackKey:
        if isinstance(query, EntityBasedQuery):
            return self.fallback_data_source.make_key(
                self.entity_name, query.entity_id
            )

        if isinstance(query, self.entity_cls):
            return self.fallback_data_source.make_key(
                self.entity_name, query.id  # type: ignore
            )

        raise InvalidQueryError(query)
