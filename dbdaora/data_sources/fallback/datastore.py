import asyncio
import dataclasses
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Dict, Iterable, Optional

from google.cloud.datastore import Client, Entity, Key

from . import FallbackDataSource


@dataclasses.dataclass
class DatastoreDataSource(FallbackDataSource[Key]):
    client: Client = dataclasses.field(default_factory=Client)
    executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=100)

    def make_key(self, *key_parts: Any) -> Key:
        return self.client.key(
            key_parts[0],
            self.key_separator.join([str(k) for k in key_parts[1:]]),
        )

    async def get(self, key: Key) -> Optional[Dict[str, Any]]:
        loop = asyncio.get_running_loop()
        entity = await loop.run_in_executor(
            self.executor, partial(self.client.get, key)
        )
        return None if entity is None else entity_asdict(entity)

    async def put(
        self,
        key: Key,
        data: Dict[str, Any],
        exclude_from_indexes: Iterable[str] = (),
        **kwargs: Any,
    ) -> None:
        entity = Entity(key, exclude_from_indexes=exclude_from_indexes)
        entity.update(data)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self.executor, partial(self.client.put, entity)
        )

    async def delete(self, key: Key) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self.executor, partial(self.client.delete, key)
        )

    async def query(self, key: Key, **kwargs: Any) -> Iterable[Dict[str, Any]]:
        return self.client.query(kind=key.kind, **kwargs).fetch()


def entity_asdict(entity: Entity) -> Dict[str, Any]:
    return {
        k: entity_asdict(v) if isinstance(v, Entity) else v
        for k, v in entity.items()
    }


class KindKeyDatastoreDataSource(DatastoreDataSource):
    def make_key(self, *key_parts: Any) -> Key:
        return self.client.key(
            self.key_separator.join([str(k) for k in key_parts[:-1]]),
            key_parts[-1],
        )
