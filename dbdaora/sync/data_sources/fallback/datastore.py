import dataclasses
from typing import Any, Dict, Iterable, Optional

from google.cloud.datastore import Client, Entity, Key


@dataclasses.dataclass
class DatastoreDataSource:
    client: Client = dataclasses.field(default_factory=Client)

    def make_key(self, *key_parts: Any) -> Key:
        return self.client.key(key_parts[0], key_parts[1])

    def get(self, key: Key) -> Optional[Dict[str, Any]]:
        entity = self.client.get(key)
        return None if entity is None else entity_asdict(entity)

    def put(
        self,
        key: Key,
        data: Dict[str, Any],
        exclude_from_indexes: Iterable[str] = (),
        **kwargs: Any,
    ) -> None:
        entity = Entity(key, exclude_from_indexes=exclude_from_indexes)
        entity.update(data)
        self.client.put(entity)

    def delete(self, key: Key) -> None:
        self.client.delete(key)


def entity_asdict(entity: Entity) -> Dict[str, Any]:
    return {
        k: entity_asdict(v) if isinstance(v, Entity) else v
        for k, v in entity.items()
    }
