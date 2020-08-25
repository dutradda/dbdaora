from typing import Any, AsyncGenerator, Optional, Sequence

from cachetools import Cache

from ...keys import FallbackKey
from ...service import Service
from ..entity import SortedSetData, SortedSetEntity


class SortedSetService(Service[SortedSetEntity, SortedSetData, FallbackKey]):
    def get_many(
        self, *ids: str, **filters: Any
    ) -> AsyncGenerator[SortedSetEntity, None]:
        raise NotImplementedError()  # pragma: no cover

    def get_many_cached(
        self,
        ids: Sequence[str],
        cache: Cache,
        memory: bool = True,
        **filters: Any,
    ) -> AsyncGenerator[SortedSetEntity, None]:
        raise NotImplementedError()  # pragma: no cover

    async def delete(
        self, entity_id: Optional[str] = None, **filters: Any
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def exists(self, id: Optional[str] = None, **filters: Any) -> bool:
        raise NotImplementedError()  # pragma: no cover

    async def exists_cached(
        self, cache: Cache, memory: bool = True, **filters: Any,
    ) -> bool:
        raise NotImplementedError()  # pragma: no cover
