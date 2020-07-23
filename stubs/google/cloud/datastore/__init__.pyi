from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


class Key:
    kind: str


class Query:
    def fetch(self) -> Iterable[Dict[str, Any]]: ...


@dataclass
class Entity(Dict[str, Any]):
    key: Key
    exclude_from_indexes: Iterable[str] = ()


class Client:
    def get(self, key: Key) -> Optional[Entity]: ...

    def put(self, data: Entity) -> None: ...

    def delete(self, key: Key) -> None: ...

    def get_multi(self, keys: Iterable[Key]) -> List[Entity]: ...

    def key(self, *key_parts: Any) -> Key: ...

    def query(self, kind: str, **kwargs: Any) -> Query: ...
