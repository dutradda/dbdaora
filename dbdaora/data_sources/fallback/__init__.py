from typing import Any, Dict, Generic, Iterable, Optional

from dbdaora.keys import FallbackKey


class FallbackDataSource(Generic[FallbackKey]):
    def make_key(self, *key_parts: Any) -> FallbackKey:
        raise NotImplementedError()

    async def get(self, key: FallbackKey) -> Optional[Dict[str, Any]]:
        raise NotImplementedError()

    async def get_many(
        self, keys: Iterable[FallbackKey]
    ) -> Iterable[Optional[Dict[str, Any]]]:
        raise NotImplementedError()

    async def put(self, key: FallbackKey, data: Dict[str, Any]) -> None:
        raise NotImplementedError()

    async def delete(self, key: FallbackKey) -> None:
        raise NotImplementedError()
