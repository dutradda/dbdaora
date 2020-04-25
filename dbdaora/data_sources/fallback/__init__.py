from typing import Any, ClassVar, Dict, Generic, Iterable, Optional, Sequence

from dbdaora.keys import FallbackKey

from .. import DataSource


class FallbackDataSource(DataSource, Generic[FallbackKey]):
    key_separator: ClassVar[str] = ':'

    def make_key(self, *key_parts: Any) -> FallbackKey:
        raise NotImplementedError()

    async def get(self, key: FallbackKey) -> Optional[Dict[str, Any]]:
        raise NotImplementedError()

    async def get_many(
        self, keys: Iterable[FallbackKey]
    ) -> Sequence[Optional[Dict[str, Any]]]:
        raise NotImplementedError()

    async def put(self, key: FallbackKey, data: Dict[str, Any]) -> None:
        raise NotImplementedError()

    async def delete(self, key: FallbackKey) -> None:
        raise NotImplementedError()
