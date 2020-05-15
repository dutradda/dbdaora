import dataclasses
import random
import time
from enum import Enum
from typing import Any, Dict, Tuple, Type, Union

from cachetools import Cache, LFUCache, LRUCache, TTLCache


@dataclasses.dataclass
class TTLDaoraCache:
    maxsize: int
    ttl: int
    ttl_failure_threshold: int = 0
    cache: Dict[str, Tuple[Any, float]] = dataclasses.field(
        default_factory=dict
    )

    def __setitem__(self, key: str, data: Any) -> None:
        if len(self.cache) < self.maxsize:
            self.cache[key] = (data, time.time() - self.ttl_threshold)

    def get(self, key: str, default: Any = None) -> Any:
        data = self.cache.get(key)

        if data is not None:
            if data[1] + self.ttl >= time.time():
                return data[0]

            else:
                self.cache.pop(key)
                return default

        return default

    @property
    def ttl_threshold(self) -> int:
        if self.ttl_failure_threshold:
            return random.randint(0, self.ttl_failure_threshold)

        return 0


class CacheType(Enum):
    value: Union[Type[Cache]]

    TTL = TTLCache
    LFU = LFUCache
    LRU = LRUCache
    TTLDAORA = TTLDaoraCache
