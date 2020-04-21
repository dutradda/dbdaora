from typing import Any, Dict


class Cache(Dict[Any, Any]):
    def __init__(self, *args: Any, **kwargs: Any): ...


class LFUCache(Cache):
    def __init__(self, maxsize: int): ...


class LRUCache(Cache):
    def __init__(self, maxsize: int): ...


class TTLCache(Cache):
    def __init__(self, maxsize: int, ttl: int): ...
