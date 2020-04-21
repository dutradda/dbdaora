import dataclasses

from dbdaora.entity import EntityData
from dbdaora.keys import FallbackKey
from dbdaora.query import Query

from .entity import Entity


@dataclasses.dataclass
class EntityBasedQuery(Query[Entity, EntityData, FallbackKey]):
    entity_id: str
