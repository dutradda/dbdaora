from abc import abstractmethod, ABC
from typing import Optional


class MemoryDataSource(ABC):
    @abstractmethod
    async def get(self, key: str) -> Optional[str]:
        ...

    @abstractmethod
    async def set(self, key: str, data: str) -> None:
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        ...

    @abstractmethod
    async def expire(self, key: str, time: int) -> None:
        ...
