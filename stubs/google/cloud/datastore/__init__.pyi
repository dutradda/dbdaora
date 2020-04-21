from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


class Key:
    ...


@dataclass
class Entity(Dict[str, Any]):
    key: Key


class Client:
    def get(self, key: Key) -> Optional[Entity]: ...

    def put(self, data: Entity) -> None: ...

    def delete(self, key: Key) -> None: ...

    def get_multi(self, keys: Iterable[Key]) -> List[Entity]: ...

    def key(self, *key_parts: Any) -> Key: ...
