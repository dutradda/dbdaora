from typing import Any, ClassVar, Dict, Generic, Optional

from dbdaora.keys import FallbackKey

from .. import DataSource


class FallbackDataSource(DataSource, Generic[FallbackKey]):
    key_separator: ClassVar[str] = ':'

    def make_key(self, *key_parts: Any) -> FallbackKey:
        raise NotImplementedError()  # pragma: no cover

    async def get(self, key: FallbackKey) -> Optional[Dict[str, Any]]:
        raise NotImplementedError()  # pragma: no cover

    async def put(
        self, key: FallbackKey, data: Dict[str, Any], **kwargs: Any
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def delete(self, key: FallbackKey) -> None:
        raise NotImplementedError()  # pragma: no cover
