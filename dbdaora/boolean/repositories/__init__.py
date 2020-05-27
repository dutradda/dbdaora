from typing import Any, Optional, Union

from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query
from dbdaora.query import make as query_factory
from dbdaora.repository import MemoryRepository


class BooleanRepository(MemoryRepository[Any, bool, FallbackKey]):
    __skip_cls_validation__ = ('BooleanRepository',)

    async def get_memory_data(
        self, key: str, query: BaseQuery[Any, bool, FallbackKey],
    ) -> Optional[bool]:
        return bool(await self.memory_data_source.exists(key)) or None

    async def get_fallback_data(
        self,
        query: BaseQuery[Any, bool, FallbackKey],
        *,
        for_memory: bool = False,
    ) -> Optional[bool]:
        data = await self.fallback_data_source.get(self.fallback_key(query))

        if data is not None:
            return True

        return None

    def make_entity(  # type: ignore
        self, data: bool, query: Query[Any, bool, FallbackKey],
    ) -> Any:
        return query.key_parts[-1]

    def make_entity_from_fallback(  # type: ignore
        self, data: bool, query: Query[Any, bool, FallbackKey],
    ) -> Any:
        return self.make_entity(data, query)

    async def add_memory_data(
        self, key: str, data: bool, from_fallback: bool = False
    ) -> None:
        await self.memory_data_source.set(key, '1')

    async def add_memory_data_from_fallback(
        self,
        key: str,
        query: Union[BaseQuery[Any, bool, FallbackKey], Any],
        data: bool,
    ) -> bool:
        await self.add_memory_data(key, data)
        return True

    def make_memory_data_from_entity(self, entity: Any) -> bool:
        return True

    async def add_fallback(
        self, entity: Any, *entities: Any, **kwargs: Any
    ) -> None:
        await self.fallback_data_source.put(
            self.fallback_key(entity), {'value': True}, **kwargs
        )

    def make_query(
        self, *args: Any, **kwargs: Any
    ) -> BaseQuery[Any, bool, FallbackKey]:
        return query_factory(self, *args, **kwargs)
