from typing import Generic, Optional, Any
from dbdaora.data import FallbackData
from dbdaora.keys import FallbackKey


class FallbackDataSource(Generic[FallbackKey, FallbackData]):
    def make_key(self, *key_parts: Any) -> FallbackKey:
        raise NotImplementedError()

    async def get(self, key: FallbackKey) -> Optional[FallbackData]:
        raise NotImplementedError()

    async def put(self, key: FallbackKey, data: FallbackData) -> None:
        raise NotImplementedError()

    async def delete(self, key: FallbackKey) -> None:
        raise NotImplementedError()
