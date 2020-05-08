from typing import Union, Any, Type
from aioredis import Redis, ConnectionsPool


class MultiExec:
    def __init__(
        self,
        pool_or_connection: ConnectionsPool,
        commands_factory: Type[Redis],
    ):
        ...

    def delete(self, key: str) -> Any:
        ...

    def hmset(
        self,
        key: str,
        field: Union[str, bytes],
        value: Union[str, bytes],
        *pairs: Union[str, bytes],
    ) -> Any:
        ...

    def zadd(
        self, key: str, score: float, member: str, *pairs: Union[float, str]
    ) -> Any:
        ...

    async def execute(self, *, return_exceptions: bool = False) -> Any:
        ...
