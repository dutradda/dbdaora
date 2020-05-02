import dataclasses
from typing import Dict, Optional, Sequence, Type, Union

from aioredis import Redis, create_redis_pool

from dbdaora.hashring import HashRing

from . import MemoryDataSource, RangeOutput


class AioRedisDataSource(Redis, MemoryDataSource):
    ...


@dataclasses.dataclass
class ShardsAioRedisDataSource(MemoryDataSource):
    hashring: HashRing[AioRedisDataSource]

    def get_client(self, key: str) -> AioRedisDataSource:
        return self.hashring.get_node(key)

    async def get(self, key: str) -> Optional[bytes]:
        return await self.get_client(key).get(key)

    async def set(self, key: str, data: str) -> None:
        await self.get_client(key).set(key, data)

    async def delete(self, key: str) -> None:
        await self.get_client(key).delete(key)

    async def expire(self, key: str, time: int) -> None:
        await self.get_client(key).expire(key, time)

    async def exists(self, key: str) -> int:
        return await self.get_client(key).exists(key)

    async def zrevrange(
        self, key: str, withscores: bool = False
    ) -> Optional[RangeOutput]:
        return await self.get_client(key).zrevrange(key, withscores)

    async def zrange(
        self, key: str, withscores: bool = False
    ) -> Optional[RangeOutput]:
        return await self.get_client(key).zrange(key, withscores)

    async def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> None:
        await self.get_client(key).zadd(key, score, member, *pairs)

    async def hmset(
        self,
        key: str,
        field: Union[str, bytes],
        value: Union[str, bytes],
        *pairs: Union[str, bytes],
    ) -> None:
        await self.get_client(key).hmset(key, field, value, *pairs)

    async def hmget(
        self, key: str, field: Union[str, bytes], *fields: Union[str, bytes]
    ) -> Sequence[Optional[bytes]]:
        return await self.get_client(key).hmget(key, field, *fields)

    async def hgetall(self, key: str) -> Dict[bytes, bytes]:
        return await self.get_client(key).hgetall(key)

    def close(self) -> None:
        for client in self.hashring.nodes:
            client.close()

    async def wait_closed(self) -> None:
        for client in self.hashring.nodes:
            await client.wait_closed()


async def make(
    *uris: str,
    hashring_cls: Type[HashRing[AioRedisDataSource]] = HashRing,
    hashring_nodes_size: Optional[int] = None,
    commands_factory: Type[AioRedisDataSource] = AioRedisDataSource,
) -> Union[Redis, ShardsAioRedisDataSource]:
    if len(uris) == 0:
        uris = ['redis://']  # type: ignore

    if len(uris) > 1:
        clients: Sequence[AioRedisDataSource] = [
            await create_redis_pool(  # type: ignore
                uri, commands_factory=AioRedisDataSource
            )
            for uri in uris
        ]
        hashring = hashring_cls(clients, hashring_nodes_size)
        return ShardsAioRedisDataSource(hashring)

    else:
        return await create_redis_pool(
            uris[0], commands_factory=AioRedisDataSource
        )
