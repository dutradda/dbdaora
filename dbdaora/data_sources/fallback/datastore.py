import dataclasses
from typing import Any, Dict, Iterable, Optional, Sequence

from google.cloud.datastore import Client, Entity, Key

from . import FallbackDataSource


@dataclasses.dataclass
class DatastoreDataSource(FallbackDataSource[Key]):
    client: Client = dataclasses.field(default_factory=Client)

    def make_key(self, *key_parts: Any) -> Key:
        return self.client.key(*[str(p) for p in key_parts])

    async def get(self, key: Key) -> Optional[Dict[str, Any]]:
        entity = self.client.get(key)
        return None if entity is None else entity_asdict(entity)

    async def put(
        self,
        key: Key,
        data: Dict[str, Any],
        exclude_from_indexes: Iterable[str] = [],
    ) -> None:
        entity = Entity(key)
        entity.update(data)
        self.client.put(entity)

    async def delete(self, key: Key) -> None:
        self.client.delete(key)

    async def get_many(
        self, keys: Iterable[Key]
    ) -> Sequence[Optional[Dict[str, Any]]]:
        entities = self.client.get_multi(keys)
        goted_keys = {entity.key: entity for entity in entities}
        return [goted_keys.get(key) for key in keys]


def entity_asdict(entity: Entity) -> Dict[str, Any]:
    return {
        k: entity_asdict(v) if isinstance(v, Entity) else v
        for k, v in entity.items()
    }
