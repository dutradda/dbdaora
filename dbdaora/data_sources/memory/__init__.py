from typing import Optional, Protocol, Union, Dict, List, Generic, ClassVar, Sequence, Tuple

from dbdaora.keys import MemoryKey
from dbdaora.data import MemoryData


SortedSetInput = Sequence[Union[float, str]]
SortedSetData = Union[Sequence[bytes], Sequence[Tuple[bytes, float]]]


class MemoryDataSource(Protocol):
    key_separator: ClassVar[str] = ':'

    def make_key(self, *key_parts: str) -> str:
        return self.key_separator.join(key_parts)

    async def get(self, key: str) -> Optional[bytes]:
        ...

    async def set(self, key: str, data: str) -> None:
        ...

    async def delete(self, key: str) -> None:
        ...

    async def expire(self, key: str, time: int) -> None:
        ...

    async def exists(self, key: str) -> int:
        ...

    async def zrevrange(self, key: str, withscores: bool = False) -> Optional[SortedSetData]:
        ...

    async def zrange(self, key: str, withscores: bool = False) -> Optional[SortedSetData]:
        ...

    async def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> None:
        ...
