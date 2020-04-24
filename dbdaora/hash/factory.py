from logging import Logger, getLogger
from typing import Any, Optional, Type

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey
from dbdaora.service.builder import build as build_base_service

from ..cache import CacheType
from ..repository import MemoryRepository
from ..service import Service
from .service import HashService


async def make_service(
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
) -> Service[Entity, EntityData, FallbackKey]:
    return await build_base_service(
        HashService,  # type: ignore
        repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time,
        cache_type=cache_type,
        cache_ttl=cache_ttl,
        cache_max_size=cache_max_size,
        cb_failure_threshold=cb_failure_threshold,
        cb_recovery_timeout=cb_recovery_timeout,
        cb_expected_exception=cb_expected_exception,
        logger=logger,
    )
