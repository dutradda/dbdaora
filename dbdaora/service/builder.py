from logging import Logger, getLogger
from typing import Any, Optional, Type

from cachetools import Cache

from ..cache import CacheType
from ..circuitbreaker import AsyncCircuitBreaker
from ..entity import Entity, EntityData
from ..keys import FallbackKey
from ..repository import MemoryRepository
from . import Service


async def build(
    service_cls: Type[Service[Entity, EntityData, FallbackKey]],
    repository_cls: Type[MemoryRepository[Entity, EntityData, FallbackKey]],
    memory_data_source_factory: Any,
    fallback_data_source_factory: Any,
    repository_expire_time: int,
    cache_type: Optional[CacheType] = None,
    cache_ttl: Optional[int] = None,
    cache_max_size: Optional[int] = None,
    cb_failure_threshold: Optional[int] = None,
    cb_recovery_timeout: Optional[int] = None,
    cb_expected_exception: Optional[Type[Exception]] = None,
    logger: Logger = getLogger(__name__),
    cache_ttl_failure_threshold: int = 0,
) -> Service[Entity, EntityData, FallbackKey]:
    repository = await build_repository(
        repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time,
    )
    circuit_breaker = build_circuit_breaker(
        repository_cls.name,
        cb_failure_threshold,
        cb_recovery_timeout,
        cb_expected_exception,
    )
    cache = build_cache(
        cache_type, cache_ttl, cache_max_size, cache_ttl_failure_threshold
    )
    exists_cache = build_cache(
        cache_type, cache_ttl, cache_max_size, cache_ttl_failure_threshold
    )
    return service_cls(
        repository, circuit_breaker, cache, exists_cache, logger
    )


async def build_repository(
    cls: Type[MemoryRepository[Entity, EntityData, FallbackKey]],
    memory_data_source_factory: Any,
    fallback_data_source_factory: Any,
    expire_time: int,
) -> MemoryRepository[Entity, EntityData, FallbackKey]:
    return cls(
        memory_data_source=await memory_data_source_factory(),
        fallback_data_source=await fallback_data_source_factory(),
        expire_time=expire_time,
    )


def build_cache(
    cache_type: Optional[CacheType] = None,
    ttl: Optional[int] = None,
    max_size: Optional[int] = None,
    ttl_failure_threshold: int = 0,
) -> Optional[Cache]:
    if cache_type:
        if max_size is None:
            raise Exception()

        if cache_type == CacheType.TTL or cache_type == CacheType.TTLDAORA:
            if ttl is None:
                raise Exception()

            if cache_type == CacheType.TTL:
                return cache_type.value(max_size, ttl)

            if cache_type == CacheType.TTLDAORA:
                return cache_type.value(max_size, ttl, ttl_failure_threshold)

        else:
            return cache_type.value(max_size)

    return None


def build_circuit_breaker(
    name: str,
    failure_threshold: Optional[int] = None,
    recovery_timeout: Optional[int] = None,
    expected_exception: Optional[Type[Exception]] = None,
) -> AsyncCircuitBreaker:
    return AsyncCircuitBreaker(
        failure_threshold, recovery_timeout, expected_exception, name=name,
    )
