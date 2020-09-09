import asyncio
import dataclasses
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Type,
    Union,
)

from aioredis import GeoMember, GeoPoint, Redis, create_redis_pool
from aioredis.commands.transaction import MultiExec

from dbdaora.hashring import HashRing

from . import GeoRadiusOutput, MemoryDataSource, MemoryMultiExec, RangeOutput


try:
    import newrelic.agent
except ImportError:
    newrelic = None  # type: ignore


class AioRedisDataSource(Redis, MemoryDataSource):
    geopoint_cls: ClassVar[Type[GeoPoint]] = GeoPoint  # type: ignore
    geomember_cls: ClassVar[Type[GeoMember]] = GeoMember  # type: ignore


class AioRedisMultiExec(MultiExec):
    geopoint_cls: ClassVar[Type[GeoPoint]] = GeoPoint
    geomember_cls: ClassVar[Type[GeoMember]] = GeoMember


@dataclasses.dataclass
class ShardsAioRedisMultiExec(MemoryMultiExec):
    hashring: HashRing[AioRedisMultiExec]
    futures: List[Any] = dataclasses.field(default_factory=list)
    clients_to_execute: Set[AioRedisMultiExec] = dataclasses.field(
        default_factory=set
    )
    geopoint_cls: ClassVar[Type[GeoPoint]] = GeoPoint
    geomember_cls: ClassVar[Type[GeoMember]] = GeoMember

    def get_client(self, key: str) -> AioRedisMultiExec:
        return self.hashring.get_node(key)

    def delete(self, key: str) -> Any:
        client = self.get_client(key)
        future = client.delete(key)
        self.clients_to_execute.add(client)
        self.futures.append(future)
        return future

    def hmset(
        self,
        key: str,
        field: Union[str, bytes],
        value: Union[str, bytes],
        *pairs: Union[str, bytes],
    ) -> Any:
        client = self.get_client(key)
        future = client.hmset(key, field, value, *pairs)
        self.clients_to_execute.add(client)
        self.futures.append(future)
        return future

    def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> Any:
        client = self.get_client(key)
        future = client.zadd(key, score, member, *pairs)
        self.clients_to_execute.add(client)
        self.futures.append(future)
        return future

    async def execute(self, *, return_exceptions: bool = False) -> Any:
        await asyncio.gather(
            *[
                client.execute(return_exceptions=return_exceptions)
                for client in self.clients_to_execute
            ]
        )
        self.clients_to_execute.clear()

        results = await asyncio.gather(*self.futures)
        self.futures.clear()
        return results


@dataclasses.dataclass
class ShardsAioRedisDataSource(MemoryDataSource):
    hashring: HashRing[AioRedisDataSource]
    geopoint_cls: ClassVar[Type[GeoPoint]] = GeoPoint  # type: ignore
    geomember_cls: ClassVar[Type[GeoMember]] = GeoMember  # type: ignore

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
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> Optional[RangeOutput]:
        return await self.get_client(key).zrevrange(
            key, start=start, stop=stop, withscores=withscores
        )

    async def zrange(
        self,
        key: str,
        start: int = 0,
        stop: int = -1,
        withscores: bool = False,
    ) -> Optional[RangeOutput]:
        return await self.get_client(key).zrange(
            key, start=start, stop=stop, withscores=withscores
        )

    async def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> None:
        await self.get_client(key).zadd(key, score, member, *pairs)

    async def zcard(self, key: str) -> int:
        return await self.get_client(key).zcard(key)

    async def zrevrangebyscore(
        self,
        key: str,
        max: float = float('inf'),
        min: float = float('-inf'),
        withscores: bool = False,
    ) -> Optional[RangeOutput]:
        return await self.get_client(key).zrevrangebyscore(
            key, max=max, min=min, withscores=withscores
        )

    async def zrangebyscore(
        self,
        key: str,
        min: float = float('-inf'),
        max: float = float('inf'),
        withscores: bool = False,
    ) -> Optional[RangeOutput]:
        return await self.get_client(key).zrangebyscore(
            key, min=min, max=max, withscores=withscores
        )

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
        return await self.get_client(key).georadius(
            key=key,
            longitude=longitude,
            latitude=latitude,
            radius=radius,
            unit=unit,
            with_dist=with_dist,
            with_coord=with_coord,
            count=count,
        )

    async def geoadd(
        self,
        key: str,
        longitude: float,
        latitude: float,
        member: Union[str, bytes],
        *args: Any,
        **kwargs: Any,
    ) -> int:
        return await self.get_client(key).geoadd(
            key, longitude, latitude, member, *args, **kwargs,
        )

    def close(self) -> None:
        for client in self.hashring.nodes:
            client.close()

    async def wait_closed(self) -> None:
        for client in self.hashring.nodes:
            await client.wait_closed()

    def multi_exec(self) -> MemoryMultiExec:
        hashring = type(self.hashring)(
            [
                AioRedisMultiExec(node._pool_or_conn, node.__class__)  # type: ignore
                for node in self.hashring.nodes
            ],
            self.hashring.nodes_size,
        )
        return ShardsAioRedisMultiExec(hashring)  # type: ignore


async def make(
    *uris: str,
    hashring_cls: Type[HashRing[AioRedisDataSource]] = HashRing,
    hashring_nodes_size: Optional[int] = None,
    sharded_commands_factory: Type[
        ShardsAioRedisDataSource
    ] = ShardsAioRedisDataSource,
    single_commands_factory: Type[AioRedisDataSource] = AioRedisDataSource,
) -> Union[Redis, ShardsAioRedisDataSource]:
    if len(uris) == 0:
        uris = ['redis://']  # type: ignore

    if len(uris) > 1:
        clients: Sequence[AioRedisDataSource] = [
            await create_redis_pool(  # type: ignore
                uri, commands_factory=single_commands_factory
            )
            for uri in uris
        ]
        hashring = hashring_cls(clients, hashring_nodes_size)
        return sharded_commands_factory(hashring)

    else:
        return await create_redis_pool(
            uris[0], commands_factory=single_commands_factory
        )


if newrelic is not None:
    # KEY COMMANDS
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'set',
        product='redis',
        target=None,
        operation='set',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'get',
        product='redis',
        target=None,
        operation='get',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'expire',
        product='redis',
        target=None,
        operation='expire',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'delete',
        product='redis',
        target=None,
        operation='delete',
    )

    # HASH COMMANDS
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'hmset',
        product='redis',
        target=None,
        operation='hmset',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'hgetall',
        product='redis',
        target=None,
        operation='hgetall',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'hmget',
        product='redis',
        target=None,
        operation='hmget',
    )

    # SORTED SET COMMANDS
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'zadd',
        product='redis',
        target=None,
        operation='zadd',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'zrange',
        product='redis',
        target=None,
        operation='zrange',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'zrangebyscore',
        product='redis',
        target=None,
        operation='zrangebyscore',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'zrevrange',
        product='redis',
        target=None,
        operation='zrevrange',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'zrevrangebyscore',
        product='redis',
        target=None,
        operation='zrevrangebyscore',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'zcard',
        product='redis',
        target=None,
        operation='zcard',
    )

    # GEOSPATIAL COMMANDS
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'geoadd',
        product='redis',
        target=None,
        operation='geoadd',
    )
    newrelic.agent.wrap_datastore_trace(
        AioRedisDataSource,
        'georadius',
        product='redis',
        target=None,
        operation='georadius',
    )
