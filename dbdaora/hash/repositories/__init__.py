import dataclasses
import itertools
from typing import Any, Dict, List, Optional, Sequence, Union

from jsondaora import dataclasses as jdataclasses

from dbdaora.exceptions import EntityNotFoundError, InvalidEntityTypeError
from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query
from dbdaora.repository import MemoryRepository


HashData = Union[
    Dict[str, Any], Dict[bytes, Any],
]


class HashRepository(MemoryRepository[Any, HashData, FallbackKey]):
    __skip_cls_validation__ = ('HashRepository',)

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

        if not for_memory and query.fields:
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
        return {f: v for f, v in data.items() if f in query.fields}  # type: ignore

    def make_fallback_data_fields_with_bytes_keys(
        self, query: 'HashQuery[FallbackKey]', data: HashData,
    ) -> HashData:
        return {
            f: v for f, v in data.items() if f.decode() in query.fields  # type: ignore
        }

    def make_entity(
        self, data: HashData, query: 'BaseQuery[Any, HashData, FallbackKey]',
    ) -> Any:
        if dataclasses.is_dataclass(self.get_entity_type(query)):
            return jdataclasses.asdataclass(
                data, self.get_entity_type(query), has_bytes_keys=True
            )

        raise InvalidEntityTypeError(self.get_entity_type(query))

    def make_entity_from_fallback(
        self, data: HashData, query: 'BaseQuery[Any, HashData, FallbackKey]',
    ) -> Any:
        return jdataclasses.asdataclass(data, self.get_entity_type(query))

    async def add_memory_data(
        self, key: str, data: HashData, from_fallback: bool = False
    ) -> None:
        input_data = itertools.chain(*data.items())
        await self.memory_data_source.hmset(key, *input_data)

    async def add_memory_data_from_fallback(
        self,
        key: str,
        query: Union[BaseQuery[Any, HashData, FallbackKey], Any],
        data: HashData,
    ) -> HashData:
        data = self.make_memory_data_from_fallback(query, data)  # type: ignore
        await self.add_memory_data(key, data)

        if isinstance(query, HashQuery) and query.fields:
            return self.make_fallback_data_fields_with_bytes_keys(query, data)

        return data

    def make_memory_data_from_fallback(
        self,
        query: Union[BaseQuery[Any, HashData, FallbackKey], Any],
        data: Dict[str, Any],
    ) -> Dict[bytes, Any]:
        return {
            k.encode(): int(v) if isinstance(v, bool) else v
            for k, v in jdataclasses.asdict(data, dumps_value=True).items()
            if v is not None
        }

    def make_memory_data_from_entity(self, entity: Any) -> HashData:
        return {
            k: int(v) if isinstance(v, bool) else v
            for k, v in jdataclasses.asdict(entity, dumps_value=True).items()
            if v is not None
        }

    async def get_memory_many(  # type: ignore
        self, query: 'HashQueryMany[FallbackKey]',
    ) -> List[Any]:
        memory_data: Sequence[Optional[HashData]]

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
                self.make_hmget_dict(query.fields, data) if data else None
                for data in await pipeline.execute()
            ]
        else:
            memory_data = await pipeline.execute()

        pipeline = self.memory_data_source.pipeline()
        queries = []

        for i, data in enumerate(memory_data):
            single_query: Query[
                Any, HashData, FallbackKey
            ] = self.make_query(  # type: ignore
                memory=query.memory, key_parts=query.many_key_parts[i],
            )
            queries.append(single_query)
            pipeline.exists(self.fallback_not_found_key(single_query))

        fallback_keys: List[Optional[FallbackKey]] = []
        fallback_not_found_keys = await pipeline.execute()

        for i, (already_not_found, data) in enumerate(
            zip(fallback_not_found_keys, memory_data)
        ):
            if not data and not already_not_found:
                fallback_keys.append(self.fallback_key(queries[i]))
            elif already_not_found:
                fallback_keys.append(None)

        if fallback_keys:
            fallback_data = await self.fallback_data_source.get_many(
                fallback_keys
            )
            fb_data_index = 0

            set_not_found_tasks = []
            add_memory_data_from_fallback_tasks = []

            for i, data in enumerate(memory_data):
                if not data:
                    fb_data = fallback_data[fb_data_index]
                    fb_data_index += 1

                    if not fb_data and not fallback_not_found_keys[i]:
                        set_not_found_tasks.append(
                            self.set_fallback_not_found(queries[i])
                        )

                    elif fb_data and not fallback_not_found_keys[i]:
                        add_memory_data_from_fallback_tasks.append(
                            (
                                i,
                                self.add_memory_data_from_fallback(
                                    self.memory_key(queries[i]),
                                    query,
                                    fb_data,
                                ),
                            )
                        )

            for task in set_not_found_tasks:
                await task

            for i, task_ in add_memory_data_from_fallback_tasks:
                data = await task_
                memory_data[i] = data

        if not any(memory_data):
            raise EntityNotFoundError(query)

        return [
            self.make_entity(entity_data, query)
            for entity_data in memory_data
            if entity_data
        ]

    async def get_fallback_many(  # type: ignore
        self, query: 'HashQueryMany[FallbackKey]',
    ) -> List[Optional[Any]]:
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
        self, entity: Any, *entities: Any, **kwargs: Any
    ) -> None:
        data = jdataclasses.asdict(entity)
        await self.fallback_data_source.put(
            self.fallback_key(entity), data, **kwargs
        )

    def make_query(
        self, *args: Any, **kwargs: Any
    ) -> 'BaseQuery[Any, HashData, FallbackKey]':
        return query_factory(self, *args, **kwargs)


from ..query import HashQuery, HashQueryMany  # noqa isort:skip
from ..query import make as query_factory  # noqa isort:skip
