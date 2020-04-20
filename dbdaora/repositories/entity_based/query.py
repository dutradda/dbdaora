import dataclasses

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey
from dbdaora.query import Query


@dataclasses.dataclass
class EntityBasedQuery(Query[Entity, EntityData, FallbackKey]):
    entity_id: str
