from typing import (
    Any,
    ClassVar,
    Dict,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)


rangeOutput = Sequence[bytes]
rangeWithScoresOutput = Sequence[Tuple[bytes, float]]
SortedSetData = Union[rangeOutput, rangeWithScoresOutput]


class ConnectionsPool:
    ...


class Redis:
    key_separator: ClassVar[str] = ':'
    _pool_or_conn: ConnectionsPool

    def make_key(self, *key_parts: str) -> str: ...

    async def get(self, key: str) -> Optional[bytes]: ...

    async def set(self, key: str, data: str) -> None: ...

    async def delete(self, key: str) -> None: ...

    async def expire(self, key: str, time: int) -> None: ...

    async def exists(self, key: str) -> int: ...

    async def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> Optional[SortedSetData]: ...

    async def zrange(
        self,
        key: str,
        start: int = 0,
        stop: int = -1,
        withscores: bool = False,
    ) -> Optional[SortedSetData]: ...

    async def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> None: ...

    async def hmset(
        self,
        key: str,
        field: Union[str, bytes],
        value: Union[str, bytes],
        *pairs: Union[str, bytes],
    ) -> None: ...

    async def hmget(
        self, key: str, field: Union[str, bytes], *fields: Union[str, bytes]
    ) -> Sequence[Optional[bytes]]: ...

    async def hgetall(self, key: str) -> Dict[bytes, bytes]: ...

    def pipeline(self) -> Any: ...

    async def zrevrangebyscore(
        self,
        key: str,
        max: float = float('inf'),
        min: float = float('-inf'),
        withscores: bool = False,
    ) -> Optional[SortedSetData]:
        ...

    async def zrangebyscore(
        self,
        key: str,
        min: float = float('-inf'),
        max: float = float('inf'),
        withscores: bool = False,
    ) -> Optional[SortedSetData]:
        ...

    async def zcard(self, key: str) -> int:
        ...


async def create_redis_pool(
    address: str, *, commands_factory: Optional[Type[Redis]] = None
) -> Redis: ...


class GeoPoint(NamedTuple):
    longitude: float
    latitude: float


class GeoMember(NamedTuple):
    member: bytes
    dist: Optional[float]
    coord: Optional[GeoPoint]
