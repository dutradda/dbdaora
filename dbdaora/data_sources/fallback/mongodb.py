import dataclasses
from typing import Any, Dict, Iterable, Optional

import motor.motor_asyncio as motor

from . import FallbackDataSource


@dataclasses.dataclass
class Key:
    collection_name: str
    document_id: str


@dataclasses.dataclass
class MongoDataSource(FallbackDataSource[Key]):
    database_name: str
    client: motor.AsyncIOMotorClient = dataclasses.field(
        default_factory=motor.AsyncIOMotorClient
    )

    def make_key(self, *key_parts: Any) -> Key:
        return Key(
            key_parts[0],
            self.key_separator.join([str(k) for k in key_parts[1:]]),
        )

    async def get(self, key: Key) -> Optional[Dict[str, Any]]:
        collection = self.collection(key)
        return await collection.find_one({'_id': key.document_id})

    async def put(
        self,
        key: Key,
        data: Dict[str, Any],
        exclude_from_indexes: Iterable[str] = (),
        **kwargs: Any,
    ) -> None:
        collection = self.collection(key)
        await collection.replace_one(
            {'_id': key.document_id}, data, upsert=True,
        )

    async def delete(self, key: Key) -> None:
        collection = self.collection(key)
        await collection.delete_one({'_id': key.document_id})

    def collection(self, key: Key) -> motor.AsyncIOMotorCollection:
        return self.client[self.database_name][key.collection_name]
