from typing import Protocol, Optional
from ...entity import EntityData


class FallbackDataSource(Protocol):
    async def get(self, key: str) -> Optional[EntityData]:
        ...

    async def put(self, key: str, data: EntityData) -> None:
        ...

    async def delete(self, key: str) -> None:
        ...

