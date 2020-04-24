import dataclasses
import itertools
from typing import Any, Dict, List, Optional, Sequence, Union

from jsondaora import dataclasses as jdataclasses

from dbdaora.exceptions import EntityNotFoundError, InvalidEntityTypeError
from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query
from dbdaora.repository import MemoryRepository

from ..entity import HashEntity


HashData = Dict[str, Any]


class HashRepository(MemoryRepository[HashEntity, HashData, FallbackKey]):
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

    async def get_fallback_data(  # type: ignore
        self, query: 'HashQuery[FallbackKey]', *, for_memory: bool = False,
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
        self, query: 'HashQuery[FallbackKey]', data: HashData,
    ) -> HashData:
        return {f: v for f, v in data.items() if f in query.fields}

    def make_entity(
        self,
        data: HashData,
        query: 'BaseQuery[HashEntity, HashData, FallbackKey]',
    ) -> HashEntity:
        if dataclasses.is_dataclass(self.get_entity_type(query)):
            return jdataclasses.asdataclass(  # type: ignore
                data, self.get_entity_type(query), encode_field_name=True
            )

        raise InvalidEntityTypeError(self.get_entity_type(query))

    def make_entity_from_fallback(
        self,
        data: HashData,
        query: 'BaseQuery[HashEntity, HashData, FallbackKey]',
    ) -> HashEntity:
        return jdataclasses.asdataclass(  # type: ignore
            data, self.get_entity_type(query)
        )

    async def add_memory_data(
        self, key: str, data: HashData, from_fallback: bool = False
    ) -> None:
        input_data = itertools.chain(*data.items())
        await self.memory_data_source.hmset(key, *input_data)

    async def add_memory_data_from_fallback(
        self,
        key: str,
        query: Union[BaseQuery[HashEntity, HashData, FallbackKey], HashEntity],
        data: HashData,
    ) -> HashData:
        await self.add_memory_data(key, data)

        if isinstance(query, HashQuery) and query.fields:
            return self.make_fallback_data_fields(query, data)

        return data

    def make_memory_data(self, entity: HashEntity) -> HashData:
        return {
            k: int(v) if isinstance(v, bool) else v
            for k, v in jdataclasses.asdict(entity, dumps_value=True).items()
            if v is not None
        }

    async def get_memory_many(  # type: ignore
        self, query: 'HashQueryMany[FallbackKey]',
    ) -> List[HashEntity]:
        memory_data: Sequence[Union[HashData, Dict[bytes, Any]]]

        keys = self.memory_keys(query)

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

        pipeline = self.memory_data_source.pipeline()
        queries = []

        for i, data in enumerate(memory_data):
            single_query: Query[
                HashEntity, HashData, FallbackKey
            ] = self.make_query(  # type: ignore
                memory=query.memory, key_parts=query.many_key_parts[i],
            )
            queries.append(single_query)
            pipeline.exists(self.fallback_not_found_key(single_query))

        fallback_keys = []

        for i, (already_not_found, data) in enumerate(
            zip(await pipeline.execute(), memory_data)
        ):
            if not data and not already_not_found:
                fallback_keys.append(self.fallback_key(queries[i]))

        if fallback_keys:
            fallback_data = await self.fallback_data_source.get_many(
                fallback_keys
            )
            fb_data_index = 0

            for i, data in enumerate(memory_data):
                if data is None:
                    fb_data = fallback_data[fb_data_index]
                    fb_data_index += 1

                    if fb_data is None:
                        await self.set_fallback_not_found(queries[i])

                    else:
                        memory_data[
                            i
                        ] = await self.add_memory_data_from_fallback(
                            self.memory_key(queries[i]), query, fb_data
                        )

        if not any(memory_data):
            raise EntityNotFoundError(query)

        return [
            self.make_entity(entity_data, query)
            for entity_data in memory_data
            if entity_data is not None
        ]

    async def get_fallback_many(  # type: ignore
        self, query: 'HashQueryMany[FallbackKey]',
    ) -> List[Optional[HashEntity]]:
        keys = self.fallback_keys(query)

        data = await self.fallback_data_source.get_many(keys)

        if not any(data):
            raise EntityNotFoundError(query)

        if query.fields:
            return [
                self.make_entity_from_fallback(
                    self.make_fallback_data_fields(query, entity_data), query,
                )
                for entity_data in data
                if entity_data
            ]

        return [
            self.make_entity_from_fallback(entity_data, query)
            for entity_data in data
            if entity_data
        ]

    async def add_fallback(
        self, entity: HashEntity, *entities: HashEntity
    ) -> None:
        data = jdataclasses.asdict(entity, dumps_value=True)
        await self.fallback_data_source.put(self.fallback_key(entity), data)

    def make_query(
        self, *args: Any, **kwargs: Any
    ) -> 'BaseQuery[HashEntity, HashData, FallbackKey]':
        return query_factory(self, *args, **kwargs)


from ..query import HashQuery, HashQueryMany  # noqa isort:skip
from ..query import make as query_factory  # noqa isort:skip
