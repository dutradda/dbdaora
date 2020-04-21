from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
)


RangeOutput = Sequence[Union[bytes, str]]
RangeWithScoresOutput = Sequence[Union[Tuple[str, float], Tuple[bytes, float]]]
SortedSetData = Union[RangeOutput, RangeWithScoresOutput]


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

    async def zrevrange(
        self, key: str, withscores: bool = False
    ) -> Optional[SortedSetData]:
        ...

    async def zrange(
        self, key: str, withscores: bool = False
    ) -> Optional[SortedSetData]:
        ...

    async def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> None:
        ...

    async def hmset(
        self,
        key: str,
        field: Union[str, bytes],
        value: Union[str, bytes],
        *pairs: Union[str, bytes],
    ) -> None:
        ...

    async def hmget(
        self, key: str, field: Union[str, bytes], *fields: Union[str, bytes]
    ) -> Sequence[Optional[bytes]]:
        ...

    async def hgetall(self, key: str) -> Dict[bytes, bytes]:
        ...

    def pipeline(self) -> 'Pipeline':
        ...

    def close(self) -> None:
        ...

    async def wait_closed(self) -> None:
        ...


class Pipeline(Protocol):
    async def execute(self, *, return_exceptions: bool = False) -> List[Any]:
        ...

    def hmget(
        self, key: str, field: Union[str, bytes], *fields: Union[str, bytes]
    ) -> None:
        ...

    def hgetall(self, key: str) -> None:
        ...
