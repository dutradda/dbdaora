from typing import (
    Any,
    ClassVar,
    Dict,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Type,
    Union,
)

from .. import DataSource


class GeoPoint(Protocol):
    longitude: float
    latitude: float

    def __init__(self, longitude: float, latitude: float):
        ...


class GeoMember(Protocol):
    member: bytes
    dist: Optional[float]
    coord: Optional[GeoPoint]

    def __init__(
        self,
        member: bytes,
        dist: Optional[float],
        coord: Optional[GeoPoint],
        hash: Optional[str] = None,
    ):
        ...


GeoRadiusOutput = Union[Sequence[GeoMember], Sequence[str]]


RangeOutput = Union[
    Sequence[Tuple[str, float]],
    Sequence[Tuple[bytes, float]],
    Sequence[Union[bytes, str]],
]


class MemoryMultiExec:
    def delete(self, key: str) -> Any:
        raise NotImplementedError()  # pragma: no cover

    def hmset(
        self,
        key: str,
        field: Union[str, bytes],
        value: Union[str, bytes],
        *pairs: Union[str, bytes],
    ) -> Any:
        raise NotImplementedError()  # pragma: no cover

    def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> Any:
        raise NotImplementedError()  # pragma: no cover

    async def execute(self, *, return_exceptions: bool = False) -> Any:
        raise NotImplementedError()  # pragma: no cover


class MemoryDataSource(DataSource):
    key_separator: ClassVar[str] = ':'
    geopoint_cls: ClassVar[Type[GeoPoint]]
    geomember_cls: ClassVar[Type[GeoMember]]

    def make_key(self, *key_parts: str) -> str:
        return self.key_separator.join(key_parts)

    async def get(self, key: str) -> Optional[bytes]:
        raise NotImplementedError()  # pragma: no cover

    async def set(self, key: str, data: str) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def delete(self, key: str) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def expire(self, key: str, time: int) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def exists(self, key: str) -> int:
        raise NotImplementedError()  # pragma: no cover

    async def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> Optional[RangeOutput]:
        raise NotImplementedError()  # pragma: no cover

    async def zrange(
        self,
        key: str,
        start: int = 0,
        stop: int = -1,
        withscores: bool = False,
    ) -> Optional[RangeOutput]:
        raise NotImplementedError()  # pragma: no cover

    async def zrevrangebyscore(
        self,
        key: str,
        max: float = float('inf'),
        min: float = float('-inf'),
        withscores: bool = False,
    ) -> Optional[RangeOutput]:
        raise NotImplementedError()  # pragma: no cover

    async def zrangebyscore(
        self,
        key: str,
        min: float = float('-inf'),
        max: float = float('inf'),
        withscores: bool = False,
    ) -> Optional[RangeOutput]:
        raise NotImplementedError()  # pragma: no cover

    async def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def zcard(self, key: str) -> int:
        raise NotImplementedError()  # pragma: no cover

    async def hmset(
        self,
        key: str,
        field: Union[str, bytes],
        value: Union[str, bytes],
        *pairs: Union[str, bytes],
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def hmget(
        self, key: str, field: Union[str, bytes], *fields: Union[str, bytes]
    ) -> Sequence[Optional[bytes]]:
        raise NotImplementedError()  # pragma: no cover

    async def hgetall(self, key: str) -> Dict[bytes, bytes]:
        raise NotImplementedError()  # pragma: no cover

    def close(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def wait_closed(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    def multi_exec(self) -> MemoryMultiExec:
        raise NotImplementedError()  # pragma: no cover

    async def georadius(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str = 'm',
        *,
        with_dist: bool = False,
        with_coord: bool = False,
        count: Optional[int] = None,
    ) -> GeoRadiusOutput:
        raise NotImplementedError()  # pragma: no cover

    async def geoadd(
        self,
        key: str,
        longitude: float,
        latitude: float,
        member: Union[str, bytes],
        *args: Any,
        **kwargs: Any,
    ) -> int:
        raise NotImplementedError()  # pragma: no cover
