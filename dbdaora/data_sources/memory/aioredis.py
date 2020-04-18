import dataclasses
from typing import Any, Dict, Generic, Optional, Sequence, Union

from aioredis import Redis  # type: ignore

from dbdaora.entity import Entity

from . import MemoryDataSource, SortedSetData


class AioRedisDataSource(Redis, MemoryDataSource):  # type: ignore
    ...
