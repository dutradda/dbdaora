import asyncio
import dataclasses
from typing import Any, Dict, List, Optional, Sequence, Union

from . import MemoryDataSource, MemoryMultiExec, RangeOutput


@dataclasses.dataclass
class DictMultiExec(MemoryMultiExec):
    client: 'DictMemoryDataSource'
    futures: List[Any] = dataclasses.field(default_factory=list)

    def delete(self, key: str) -> Any:
        future = self.client.delete(key)
        self.futures.append(future)
        return future

    def hmset(
        self,
        key: str,
        field: Union[str, bytes],
        value: Union[str, bytes],
        *pairs: Union[str, bytes],
    ) -> Any:
        future = self.client.hmset(key, field, value, *pairs)
        self.futures.append(future)
        return future

    def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> Any:
        future = self.client.zadd(key, score, member, *pairs)
        self.futures.append(future)
        return future

    async def execute(self, *, return_exceptions: bool = False) -> Any:
        results = await asyncio.gather(*self.futures)
        self.futures.clear()
        return results


@dataclasses.dataclass
class DictMemoryDataSource(MemoryDataSource):
    db: Dict[str, Any] = dataclasses.field(default_factory=dict)

    async def get(self, key: str) -> Optional[bytes]:
        return self.db.get(key)

    async def set(self, key: str, data: str) -> None:
        self.db[key] = data.encode()

    async def delete(self, key: str) -> None:
        self.db.pop(key, None)

    async def expire(self, key: str, time: int) -> None:
        ...

    async def exists(self, key: str) -> bool:
        return key in self.db

    async def zrevrange(
        self, key: str, withscores: bool = False
    ) -> Optional[RangeOutput]:
        data: RangeOutput

        if key not in self.db:
            return None

        if withscores:
            data = list(self.db[key])
        else:
            data = [i[0] for i in self.db[key]]

        data.reverse()
        return data

    async def zrange(
        self, key: str, withscores: bool = False
    ) -> Optional[RangeOutput]:
        data: Optional[RangeOutput] = self.db.get(key)

        if data is None:
            return None

        if withscores:
            return data

        return [i[0] for i in self.db[key]]

    async def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> None:
        data = [score, member] + list(pairs)
        self.db[key] = sorted(
            [(data[i], data[i - 1]) for i in range(1, len(data), 2)],
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
        data: Optional[Dict[bytes, Any]] = self.db.get(key)

        if data is None:
            return [None for i in range(len(fields) + 1)]

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

    def multi_exec(self) -> MemoryMultiExec:
        return DictMultiExec(self)
