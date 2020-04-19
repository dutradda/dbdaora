"""Generates ***SQL*** and ***NoSQL*** Database Models from @dataclass"""


__version__ = '0.0.11'


from dbdaora.circuitbreaker import AsyncCircuitBreaker
from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory.aioredis import AioRedisDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource
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
]

if DictMemoryDataSource:
    __all__.append('DictMemoryDataSource')
