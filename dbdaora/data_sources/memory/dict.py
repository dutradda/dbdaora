import dataclasses
from typing import Any, Dict, Optional, Sequence, Union

from . import MemoryDataSource, RangeOutput


@dataclasses.dataclass
class DictMemoryDataSource(MemoryDataSource):
    db: Dict[str, Any] = dataclasses.field(default_factory=dict)

    async def set(self, key: str, data: str) -> None:
        self.db[key] = data.encode()

    async def delete(self, key: str) -> None:
        self.db.pop(key, None)

    async def expire(self, key: str, time: int) -> None:
        ...

    async def exists(self, key: str) -> bool:
        return key in self.db

    async def zrange(
        self,
        key: str,
        start: int = 0,
        stop: int = -1,
        withscores: bool = False,
    ) -> Optional[RangeOutput]:
        data: Optional[RangeOutput] = self.db.get(key)

        if data is None:
            return None

        return [i[0] for i in self.db[key]]

    async def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> None:
        data = [score, member] + list(pairs)
        self.db[key] = sorted(
            [
                (
                    data[i].encode() if isinstance(data[i], str) else data[i],  # type: ignore
                    data[i - 1],
                )
                for i in range(1, len(data), 2)
            ],
            key=lambda d: d[1],
        )

    async def hmset(
        self,
        key: str,
        field: Union[str, bytes],
        value: Union[str, bytes],
        *pairs: Union[str, bytes],
    ) -> None:
        data = [field, value] + list(pairs)
        self.db[key] = {
            f.encode()
            if isinstance(f := data[i - 1], str)  # noqa
            else (f if isinstance(f, bytes) else str(f).encode()): v.encode()
            if isinstance(v := data[i], str)  # noqa
            else (v if isinstance(v, bytes) else str(v).encode())
            for i in range(1, len(data), 2)
        }

    async def hmget(
        self, key: str, field: Union[str, bytes], *fields: Union[str, bytes]
    ) -> Sequence[Optional[bytes]]:
        data: Dict[bytes, Any] = self.db.get(key, {})

        return [
            None
            if (d := data.get(f.encode() if isinstance(f, str) else f))  # noqa
            is None
            else (
                d
                if isinstance(d, bytes)
                else (d.encode() if isinstance(d, str) else str(d).encode())
            )
            for f in (field,) + fields
        ]

    async def hgetall(self, key: str) -> Dict[bytes, bytes]:
        return {
            f: d.encode()
            if isinstance(d, str)
            else (
                d
                if isinstance(d, bytes)
                else (d.encode() if isinstance(d, str) else str(d).encode())
            )
            for f, d in self.db.get(key, {}).items()
        }
