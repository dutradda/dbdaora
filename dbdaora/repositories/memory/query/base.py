from dataclasses import dataclass
from typing import Any

from ...entity import Entity


@dataclass(init=False)
class Query:
    repository: 'MemoryRepository'

    def __init__(
        self, repository: 'MemoryRepository', *args: Any, **kwargs: Any
    ):
        self.repository = repository

    @property
    def key(self) -> str:
        raise NotImplementedError()

    @classmethod
    def key_from_entity(cls, entity: Entity) -> str:
        raise NotImplementedError()



from ..base import MemoryRepository  # noqa isort: skip
