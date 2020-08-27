from logging import Logger, getLogger
from typing import Any, Optional, Tuple, Type, Union

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
    cb_expected_exception: Optional[
        Union[Type[Exception], Tuple[Type[Exception], ...]]
    ] = None,
    cb_expected_fallback_exception: Optional[
        Union[Type[Exception], Tuple[Type[Exception], ...]]
    ] = None,
    logger: Logger = getLogger(__name__),
    cache_ttl_failure_threshold: int = 0,
    repository_timeout: Optional[int] = None,
) -> Service[Entity, EntityData, FallbackKey]:
    repository = await build_repository(
        repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time,
        repository_timeout,
        logger,
    )

    if cb_expected_exception and cb_expected_fallback_exception:
        if not isinstance(cb_expected_exception, tuple):
            cb_expected_exception = (cb_expected_exception,)

        if not isinstance(cb_expected_fallback_exception, tuple):
            cb_expected_fallback_exception = (cb_expected_fallback_exception,)

        cb_expected_exception += cb_expected_fallback_exception

    circuit_breaker = build_circuit_breaker(
        f'{repository_cls.name}_memory',
        cb_failure_threshold,
        cb_recovery_timeout,
        cb_expected_exception,
    )
    fallback_circuit_breaker = build_circuit_breaker(
        f'{repository_cls.name}_fallback',
        cb_failure_threshold,
        cb_recovery_timeout,
        cb_expected_fallback_exception,
    )
    cache = build_cache(
        cache_type, cache_ttl, cache_max_size, cache_ttl_failure_threshold
    )
    exists_cache = build_cache(
        cache_type, cache_ttl, cache_max_size, cache_ttl_failure_threshold
    )
    return service_cls(
        repository,
        circuit_breaker,
        fallback_circuit_breaker,
        cache,
        exists_cache,
        logger,
    )


async def build_repository(
    cls: Type[MemoryRepository[Entity, EntityData, FallbackKey]],
    memory_data_source_factory: Any,
    fallback_data_source_factory: Any,
    expire_time: int,
    timeout: Optional[int],
    logger: Optional[Logger],
) -> MemoryRepository[Entity, EntityData, FallbackKey]:
    optional_args = {
        k: v
        for k, v in zip(['timeout', 'logger'], [timeout, logger])
        if v is not None
    }
    return cls(
        memory_data_source=await memory_data_source_factory(),
        fallback_data_source=await fallback_data_source_factory(),
        expire_time=expire_time,
        **optional_args,  # type: ignore
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
    expected_exception: Optional[
        Union[Type[Exception], Tuple[Type[Exception], ...]]
    ] = None,
) -> AsyncCircuitBreaker:
    return AsyncCircuitBreaker(
        failure_threshold, recovery_timeout, expected_exception, name=name,
    )
