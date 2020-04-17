import dataclasses
from typing import Any, Dict, Generic, Optional, Sequence, Union

from dbdaora.entity import Entity

from . import MemoryDataSource, SortedSetData


@dataclasses.dataclass
class DictMemoryDataSource(MemoryDataSource, Generic[Entity]):
    db: Dict[str, Any] = dataclasses.field(default_factory=dict)

    async def get(self, key: str) -> Optional[bytes]:
        return self.db.get(key)

    async def set(self, key: str, data: str) -> None:
        self.db[key] = data.encode()

    async def delete(self, key: str) -> None:
        self.db.pop(key, None)

    async def expire(self, key: str, time: int) -> None:
        ...

    def get_obj(self, key: str) -> Optional[Entity]:
        return self.db.get(key)

    def set_obj(self, key: str, entity: Entity) -> None:
        self.db[key] = entity

    async def exists(self, key: str) -> bool:
        return key in self.db

    async def zrevrange(
        self, key: str, withscores: bool = False
    ) -> Optional[SortedSetData]:
        data: SortedSetData

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
    ) -> Optional[SortedSetData]:
        data: Optional[SortedSetData] = self.db.get(key)

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
            (f.encode() if isinstance(f := data[i - 1], str) else f): (  # noqa
                v.encode() if isinstance(v := data[i], str) else v  # noqa
            )
            for i in range(1, len(data), 2)
        }

    async def hmget(
        self, key: str, field: Union[str, bytes], *fields: Union[str, bytes]
    ) -> Sequence[Optional[bytes]]:
        data: Optional[Dict[bytes, bytes]] = self.db.get(key)

        if data is None:
            return [None for i in range(len(fields) + 1)]

        return [
            data.get(f.encode() if isinstance(f, str) else f)
            for f in (field,) + fields
        ]

    async def hgetall(self, key: str) -> Dict[bytes, bytes]:
        return self.db.get(key, {})
