from typing import Optional, Protocol

from ...entity import EntityData


class MemoryDataSource(Protocol):
    async def get(self, key: str) -> Optional[EntityData]:
        ...

    async def set(self, key: str, data: str) -> None:
        ...

    async def delete(self, key: str) -> None:
        ...

    async def expire(self, key: str, time: int) -> None:
        ...
