import pytest
import dataclasses
from typing import Optional, Dict, Any, TypeVar

from dbdaora.repositories.memory.base import MemoryRepository
from dbdaora.repositories.memory.base import MemoryRepository
from dbdaora.repositories.memory.query.base import Query
from dbdaora.entity import EntityData, Entity
from dbdaora.exceptions import EntityNotFoundError
from dbdaora.data_sources.memory import MemoryDataSource
from dbdaora.data_sources.fallback import FallbackDataSource


@dataclasses.dataclass
class DictQuery(Query[str, str]):
    entity_id: str

    @property
    def key(self) -> str:
        return self.entity_id

    @classmethod
    def key_from_entity(cls, entity: str) -> str:
        return entity

