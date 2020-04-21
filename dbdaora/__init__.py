"""Generates ***SQL*** and ***NoSQL*** Database Models from @dataclass"""


__version__ = '0.0.11'


from dbdaora.circuitbreaker import AsyncCircuitBreaker
from dbdaora.data_sources.fallback import FallbackDataSource
from dbdaora.data_sources.fallback.datastore import DatastoreDataSource
from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory import MemoryDataSource
from dbdaora.data_sources.memory.aioredis import (
    AioRedisDataSource,
    ShardsAioRedisDataSource,
)
from dbdaora.data_sources.memory.aioredis import (
    make as make_aioredis_data_source,
)
from dbdaora.data_sources.memory.dict import DictMemoryDataSource
from dbdaora.hashring import HashRing
from dbdaora.repositories.base import MemoryRepository
from dbdaora.repositories.hash import HashData, HashRepository
from dbdaora.repositories.hash.query import HashQuery
from dbdaora.repositories.sorted_set import SortedSetRepository
from dbdaora.repositories.sorted_set.entity import SortedSetEntity
from dbdaora.repositories.sorted_set.query import SortedSetQuery
from dbdaora.services.hash import HashService


__all__ = [
    'MemoryRepository',
    'HashRepository',
    'HashQuery',
    'HashData',
    'SortedSetRepository',
    'SortedSetQuery',
    'SortedSetEntity',
    'DictFallbackDataSource',
    'AioRedisDataSource',
    'HashService',
    'AsyncCircuitBreaker',
    'make_aioredis_data_source',
    'ShardsAioRedisDataSource',
    'HashRing',
    'DatastoreDataSource',
    'FallbackDataSource',
    'MemoryDataSource',
]

if DictMemoryDataSource:
    __all__.append('DictMemoryDataSource')
