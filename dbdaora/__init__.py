"""Communicates with databases using repository pattern and service patterns"""


__version__ = '0.17.2'

from dbdaora.cache import CacheType, TTLDaoraCache
from dbdaora.circuitbreaker import AsyncCircuitBreaker
from dbdaora.data_sources.fallback import FallbackDataSource
from dbdaora.data_sources.fallback.dict import DictFallbackDataSource
from dbdaora.data_sources.memory import MemoryDataSource
from dbdaora.data_sources.memory.dict import DictMemoryDataSource
from dbdaora.exceptions import EntityNotFoundError
from dbdaora.hash.factory import make_service as make_hash_service
from dbdaora.hash.query import HashQuery, HashQueryMany
from dbdaora.hash.repositories import HashData, HashRepository
from dbdaora.hash.service import HashService
from dbdaora.hashring import HashRing
from dbdaora.query import Query, QueryMany
from dbdaora.repository import MemoryRepository
from dbdaora.service import Service
from dbdaora.service.builder import build as build_service
from dbdaora.service.builder import build_cache
from dbdaora.sorted_set.entity import (
    SortedSetData,
    SortedSetDictEntity,
    SortedSetEntity,
)
from dbdaora.sorted_set.query import SortedSetQuery
from dbdaora.sorted_set.repositories import SortedSetRepository


from dbdaora.boolean.repositories import BooleanRepository  # noqa isort:skip
from dbdaora.boolean.service import BooleanService  # noqa isort:skip
from dbdaora.boolean.repositories.datastore import (  # noqa isort:skip
    DatastoreBooleanRepository,
)

try:
    from dbdaora.data_sources.fallback.datastore import DatastoreDataSource
    from dbdaora.hash.repositories.datastore import DatastoreHashRepository
    from dbdaora.sorted_set.repositories.datastore import (
        DatastoreSortedSetRepository,
    )
except ImportError:
    DatastoreDataSource = None  # type: ignore
    DatastoreHashRepository = None  # type: ignore
    DatastoreSortedSetRepository = None  # type: ignore

try:
    from dbdaora.data_sources.memory.aioredis import (
        AioRedisDataSource,
        ShardsAioRedisDataSource,
        make as make_aioredis_data_source,
    )
except ImportError:
    AioRedisDataSource = None  # type: ignore
    ShardsAioRedisDataSource = None  # type: ignore
    make_aioredis_data_source = None  # type: ignore

try:
    from dbdaora.data_sources.fallback.mongodb import (
        MongoDataSource,
        Key as MongoKey,
    )
    from dbdaora.hash.repositories.mongodb import MongodbHashRepository
    from dbdaora.hash.service import MongoHashService
except ImportError:
    MongoDataSource = None  # type: ignore
    MongodbHashRepository = None  # type: ignore


__all__ = [
    'MemoryRepository',
    'HashRepository',
    'HashQuery',
    'HashData',
    'SortedSetRepository',
    'SortedSetQuery',
    'SortedSetEntity',
    'DictFallbackDataSource',
    'HashService',
    'AsyncCircuitBreaker',
    'HashRing',
    'FallbackDataSource',
    'MemoryDataSource',
    'DictMemoryDataSource',
    'build_service',
    'CacheType',
    'Service',
    'EntityNotFoundError',
    'HashQueryMany',
    'make_hash_service',
    'SortedSetData',
    'SortedSetDictEntity',
    'TTLDaoraCache',
    'build_cache',
    'BooleanRepository',
    'DatastoreBooleanRepository',
    'BooleanService',
    'Query',
    'QueryMany',
]

if AioRedisDataSource:
    __all__.append('AioRedisDataSource')

if ShardsAioRedisDataSource:
    __all__.append('ShardsAioRedisDataSource')

if make_aioredis_data_source:
    __all__.append('make_aioredis_data_source')

if DatastoreDataSource:
    __all__.append('DatastoreDataSource')

if DatastoreHashRepository:
    __all__.append('DatastoreHashRepository')

if DatastoreSortedSetRepository:
    __all__.append('DatastoreSortedSetRepository')

if MongoDataSource:
    __all__.append('MongoDataSource')

if MongodbHashRepository:
    __all__.append('MongodbHashRepository')

if MongoHashService:
    __all__.append('MongoHashService')

if MongoKey:
    __all__.append('MongoKey')
