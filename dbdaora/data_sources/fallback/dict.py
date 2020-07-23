import dataclasses
from typing import Any, ClassVar, Dict, Iterable, Optional

from dbdaora.data_sources.fallback import FallbackDataSource


@dataclasses.dataclass
class DictFallbackDataSource(FallbackDataSource[str]):
    db: Dict[Optional[str], Dict[str, Any]] = dataclasses.field(
        default_factory=dict
    )
    key_separator: ClassVar[str] = ':'

    def make_key(self, *key_parts: str) -> str:
        return self.key_separator.join([p for p in key_parts if p])

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self.db.get(key)

    async def put(self, key: str, data: Dict[str, Any], **kwargs: Any) -> None:
        self.db[key] = data

    async def delete(self, key: str) -> None:
        self.db.pop(key, None)

    async def query(self, key: str, **kwargs: Any) -> Iterable[Dict[str, Any]]:
        return self.db.values()
