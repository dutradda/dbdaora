import dataclasses
import random
import time
from enum import Enum
from typing import Any, Dict, Optional, Tuple, Type, Union

from cachetools import Cache, LFUCache, LRUCache, TTLCache


@dataclasses.dataclass(init=False)
class TTLDaoraCache:
    maxsize: int
    ttl: int
    ttl_failure_threshold: int
    cache: Dict[str, Tuple[Any, float]]
    first_set_time: float
    clean_keys_size: int

    def __init__(
        self,
        maxsize: int,
        ttl: int,
        ttl_failure_threshold: int = 0,
        cache: Optional[Dict[str, Tuple[Any, float]]] = None,
        first_set_time: float = 0,
    ):
        if cache is None:
            cache = {}

        self.maxsize = maxsize
        self.ttl = ttl
        self.ttl_failure_threshold = ttl_failure_threshold
        self.cache = cache
        self.first_set_time = first_set_time
        self.clean_keys_size = int(self.maxsize * 0.1)

    def __setitem__(self, key: str, data: Any) -> None:
        if len(self.cache) < self.maxsize:
            set_time = time.time() - self.ttl_threshold
            self.cache[key] = (data, set_time)

            if self.first_set_time <= 0:
                self.first_set_time = set_time

        elif self.first_set_time + self.ttl < time.time():
            self.first_set_time = 0

            for i, key_to_pop in enumerate(self.cache.keys()):
                self.cache.pop(key_to_pop, None)

                if i >= self.clean_keys_size:
                    break

            set_time = time.time() - self.ttl_threshold
            self.cache[key] = (data, set_time)

    def get(self, key: str, default: Any = None) -> Any:
        data = self.cache.get(key)

        if data is not None:
            if data[1] + self.ttl >= time.time():
                return data[0]

            else:
                self.cache.pop(key, None)
                return data[0]

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
