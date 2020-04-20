import dataclasses
from typing import List, Optional, Sequence

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey

from ..entity_based.query import EntityBasedQuery


@dataclasses.dataclass
class HashQuery(EntityBasedQuery[Entity, 'HashData', FallbackKey]):
    repository: 'HashRepository[Entity, FallbackKey]'
    entity_id: Optional[str] = None  # type: ignore
    entities_ids: Optional[Sequence[str]] = None
    fields: Optional[Sequence[str]] = None

    @property
    async def entities(self) -> List[Optional[Entity]]:
        return await self.repository.entities(self)


from . import HashRepository, HashData  # noqa isort:skip
