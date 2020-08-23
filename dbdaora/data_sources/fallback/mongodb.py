import dataclasses
import datetime
from typing import Any, ClassVar, Dict, Iterable, Optional, Set

import motor.motor_asyncio as motor
from pymongo.errors import OperationFailure

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
    collections_has_ttl_index: ClassVar[Set[str]] = set()

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
        document_ttl = kwargs.get('fallback_ttl')

        if document_ttl:
            data['last_modified'] = datetime.datetime.now()

            if key.collection_name not in type(self).collections_has_ttl_index:
                try:
                    await self.create_ttl_index(key, document_ttl)
                except OperationFailure:
                    if await self.drop_ttl_index(key, document_ttl):
                        await self.create_ttl_index(key, document_ttl)

                type(self).collections_has_ttl_index.add(key.collection_name)

        collection = self.collection(key)
        await collection.replace_one(
            {'_id': key.document_id}, data, upsert=True,
        )

    async def delete(self, key: Key) -> None:
        collection = self.collection(key)
        await collection.delete_one({'_id': key.document_id})

    def collection(self, key: Key) -> motor.AsyncIOMotorCollection:
        return self.client[self.database_name][key.collection_name]

    async def query(self, key: Key, **kwargs: Any) -> Iterable[Dict[str, Any]]:
        return [i async for i in self.collection(key).find(**kwargs)]

    async def create_ttl_index(self, key: Key, document_ttl: int) -> None:
        await self.collection(key).create_index(
            'last_modified', expireAfterSeconds=document_ttl,
        )

    async def drop_ttl_index(self, key: Key, document_ttl: int) -> bool:
        collection = self.collection(key)

        async for index in collection.list_indexes():
            if 'last_modified' in index['key']:
                await collection.drop_index(index['name'])
                return True

        return False


class CollectionKeyMongoDataSource(MongoDataSource):
    def make_key(self, *key_parts: Any) -> Key:
        return Key(
            self.key_separator.join([str(k) for k in key_parts[:-1]]),
            key_parts[-1],
        )
