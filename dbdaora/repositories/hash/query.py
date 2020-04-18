import dataclasses
from typing import Optional, Sequence

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey

from ..entity_based.query import EntityBasedQuery


@dataclasses.dataclass
class HashQuery(EntityBasedQuery[Entity, EntityData, FallbackKey]):
    fields: Optional[Sequence[str]] = None
