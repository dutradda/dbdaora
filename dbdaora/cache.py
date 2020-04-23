from enum import Enum
from typing import Type, Union

from cachetools import Cache, LFUCache, LRUCache, TTLCache


class CacheType(Enum):
    value: Union[Type[Cache]]

    TTL = TTLCache
    LFU = LFUCache
    LRU = LRUCache
