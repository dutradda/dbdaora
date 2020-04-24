from typing import Protocol


class HashEntity(Protocol):
    @property
    def id(self) -> str:
        ...
