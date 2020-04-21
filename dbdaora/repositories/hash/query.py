import dataclasses
from typing import List, Optional, Sequence

from dbdaora.keys import FallbackKey

from ..entity_based.entity import Entity
from ..entity_based.query import EntityBasedQuery


@dataclasses.dataclass
class HashQuery(EntityBasedQuery['HashData', FallbackKey]):
    repository: 'HashRepository[FallbackKey]'
    entity_id: Optional[str] = None  # type: ignore
    entities_ids: Optional[Sequence[str]] = None
    fields: Optional[Sequence[str]] = None

    @property
    async def entities(self) -> List[Entity]:
        return await self.repository.entities(self)


from . import HashRepository, HashData  # noqa isort:skip
