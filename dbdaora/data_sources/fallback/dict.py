import dataclasses
from typing import Optional, Dict, Any, ClassVar

from dbdaora.data_sources.fallback import FallbackDataSource
from dbdaora.data import FallbackData


@dataclasses.dataclass
class DictFallbackDataSource(FallbackDataSource[str, FallbackData]):
    db: Dict[str, Optional[FallbackData]] = dataclasses.field(default_factory=dict)
    key_separator: ClassVar[str] = ':'

    def make_key(self, *key_parts: str) -> str:
        return self.key_separator.join(key_parts)

    async def get(self, key: str) -> Optional[FallbackData]:
        return self.db.get(key)

    async def put(self, key: str, data: FallbackData) -> None:
        self.db[key] = data

    async def delete(self, key: str) -> None:
        self.db.pop(key, None)
