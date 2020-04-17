import dataclasses
from typing import Optional, Dict, Any, Generic, Iterable, Tuple, Union

from . import MemoryDataSource, SortedSetData, SortedSetInput
from dbdaora.entity import Entity


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

    async def zrevrange(self, key: str, withscores: bool = False) -> Optional[SortedSetData]:
        data: SortedSetData

        if key not in self.db:
            return None

        if withscores:
            data = list(self.db[key])
        else:
            data = [i[0] for i in self.db[key]]

        data.reverse()
        return data

    async def zrange(self, key: str, withscores: bool = False) -> Optional[SortedSetData]:
        if key not in self.db:
            return None

        if withscores:
            return self.db[key]  # type: ignore

        return [i[0] for i in self.db[key]]

    async def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> None:
        data = [score, member] + list(pairs)
        self.db[key] = sorted(
            [(data[i], data[i-1]) for i in range(1, len(data), 2)],
            key=lambda d: d[1]
        )
