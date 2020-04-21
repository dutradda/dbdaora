import dataclasses
import itertools
from typing import Any, ClassVar, Dict, List, Optional, Sequence, Type, Union

from jsondaora import dataclasses as jdataclasses

from dbdaora.exceptions import EntityNotFoundError, InvalidQueryError
from dbdaora.keys import FallbackKey
from dbdaora.query import Query

from ..entity_based import EntityBasedRepository
from ..entity_based.entity import Entity


HashData = Dict[str, Any]


class HashRepository(EntityBasedRepository[HashData, FallbackKey]):
    query_cls: ClassVar[Type['HashQuery[FallbackKey]']]

    async def get_memory_data(  # type: ignore
        self, key: str, query: 'HashQuery[FallbackKey]',
    ) -> Optional[Union[HashData, Dict[bytes, Any]]]:
        data: Optional[Union[HashData, Dict[bytes, Any]]]

        if query.fields:
            data = self.make_hmget_dict(
                query.fields,
                await self.memory_data_source.hmget(key, *query.fields),
            )

            if not data:
                return None

            return data

        data = await self.memory_data_source.hgetall(key)

        if not data:
            return None

        return data

    def make_hmget_dict(
        self, fields: Sequence[str], data: Sequence[Optional[bytes]]
    ) -> Dict[bytes, Any]:
        return {f.encode(): v for f, v in zip(fields, data) if v is not None}

    async def get_fallback_data(
        self,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        *,
        for_memory: bool = False,
        many: bool = False,
    ) -> Optional[Union[HashData]]:
        data = await self.fallback_data_source.get(self.fallback_key(query))

        if data is None:
            return None

        if not for_memory and isinstance(query, HashQuery) and query.fields:
            return self.make_fallback_data_fields(query, data)

        elif for_memory:
            return {
                k: int(v) if isinstance(v, bool) else v
                for k, v in data.items()
            }

        return data

    def make_fallback_data_fields(
        self,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        data: HashData,
    ) -> HashData:
        return {
            f: v
            for f, v in data.items()
            if isinstance(query, HashQuery)
            and query.fields
            and f in query.fields
        }

    def make_entity(
        self,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        data: HashData,
        from_fallback: bool = False,
    ) -> Entity:
        return jdataclasses.asdataclass(  # type: ignore
            data, self.entity_cls, encode=True
        )

    def make_entity_from_fallback(
        self,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        data: HashData,
    ) -> Entity:
        return jdataclasses.asdataclass(  # type: ignore
            data, self.entity_cls
        )

    async def add_memory_data(
        self, key: str, data: HashData, from_fallback: bool = False
    ) -> None:
        input_data = itertools.chain(*data.items())
        await self.memory_data_source.hmset(key, *input_data)

    async def add_memory_data_from_fallback(
        self,
        key: str,
        query: Union[Query[Entity, HashData, FallbackKey], Entity],
        data: HashData,
    ) -> HashData:
        await self.add_memory_data(key, data)

        if isinstance(query, HashQuery) and query.fields:
            return {f: v for f, v in data.items() if f in query.fields}

        return data

    def make_memory_data(self, entity: Entity) -> HashData:
        return {
            k: '1' if v is True else '0' if v is False else v
            for k, v in dataclasses.asdict(entity).items()
        }

    async def get_memory_many(  # type: ignore
        self, query: 'HashQuery[FallbackKey]',
    ) -> List[Entity]:
        memory_data: Sequence[Union[HashData, Dict[bytes, Any]]]

        if query.entities_ids is None:
            raise InvalidQueryError(query, 'entities_ids is required')

        keys = [
            self.memory_key(query, entity_id=id_) for id_ in query.entities_ids
        ]

        pipeline = self.memory_data_source.pipeline()

        if query.fields:
            for key in keys:
                pipeline.hmget(key, *query.fields)

        else:
            for key in keys:
                pipeline.hgetall(key)

        if query.fields:
            memory_data = [
                self.make_hmget_dict(query.fields, data)
                for data in await pipeline.execute()
            ]
        else:
            memory_data = await pipeline.execute()

        not_found_keys = {}

        for i, data in enumerate(memory_data):
            entity_id = query.entities_ids[i]
            if not data and not await self.already_got_not_found(
                query, entity_id=entity_id
            ):
                not_found_keys[(i, entity_id)] = self.fallback_key(
                    query, entity_id
                )

        if not_found_keys:
            fallback_data = await self.fallback_data_source.get_many(
                not_found_keys.values()
            )

            for (i, entity_id), fb_data in zip(
                not_found_keys.keys(), fallback_data
            ):
                if fb_data is None:
                    await self.set_fallback_not_found(
                        query, entity_id=entity_id
                    )
                else:
                    memory_data[i] = await self.add_memory_data_from_fallback(
                        self.memory_key(query, entity_id), query, fb_data
                    )

        if not any(memory_data):
            raise EntityNotFoundError(query)

        return [
            self.make_entity(query, entity_data)
            for entity_data in memory_data
            if entity_data is not None
        ]

    async def get_fallback_many(  # type: ignore
        self, query: 'HashQuery[FallbackKey]',
    ) -> List[Optional[Entity]]:
        if query.entities_ids is None:
            raise InvalidQueryError(query, 'entities_ids is required')

        keys = [
            self.fallback_key(query, entity_id=id_)
            for id_ in query.entities_ids
        ]

        data = await self.fallback_data_source.get_many(keys)

        if not any(data):
            raise EntityNotFoundError(query)

        if query.fields:
            return [
                self.make_entity_from_fallback(
                    query, self.make_fallback_data_fields(query, entity_data),
                )
                for entity_data in data
            ]

        return [
            self.make_entity_from_fallback(query, entity_data)
            for entity_data in data
        ]

    async def add_fallback(self, entity: Entity, *entities: Entity) -> None:
        data = jdataclasses.asdict(entity)
        await self.fallback_data_source.put(self.fallback_key(entity), data)


from .query import HashQuery  # noqa isort:skip


HashRepository.query_cls = HashQuery  # type: ignore
