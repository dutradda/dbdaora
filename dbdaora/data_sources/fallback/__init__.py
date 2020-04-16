from abc import ABC, abstractmethod


class FallbackDataSource(ABC):
    @abstractmethod
    async def get(self, key: str) -> None:
        ...

    @abstractmethod
    async def put(self, key: str) -> None:
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        ...

