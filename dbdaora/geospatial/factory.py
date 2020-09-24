from logging import Logger, getLogger
from typing import Any, Optional, Type

from dbdaora.entity import Entity, EntityData
from dbdaora.keys import FallbackKey
from dbdaora.service.builder import build as build_base_service

from ..repository import MemoryRepository
from ..service import Service
from .service import GeoSpatialService


async def make_service(
    repository_cls: Type[MemoryRepository[Entity, EntityData, FallbackKey]],
    memory_data_source_factory: Any,
    fallback_data_source_factory: Any,
    repository_expire_time: int,
    cb_failure_threshold: Optional[int] = None,
    cb_recovery_timeout: Optional[int] = None,
    cb_expected_exception: Optional[Type[Exception]] = None,
    cb_expected_fallback_exception: Optional[Type[Exception]] = None,
    logger: Logger = getLogger(__name__),
    has_add_circuit_breaker: bool = False,
    has_delete_circuit_breaker: bool = False,
) -> Service[Entity, EntityData, FallbackKey]:
    return await build_base_service(
        GeoSpatialService,  # type: ignore
        repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time,
        cb_failure_threshold=cb_failure_threshold,
        cb_recovery_timeout=cb_recovery_timeout,
        cb_expected_exception=cb_expected_exception,
        cb_expected_fallback_exception=cb_expected_fallback_exception,
        logger=logger,
        has_add_circuit_breaker=has_add_circuit_breaker,
        has_delete_circuit_breaker=has_delete_circuit_breaker,
    )
