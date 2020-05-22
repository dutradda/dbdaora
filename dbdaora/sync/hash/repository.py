from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
)

from jsondaora import dataclasses as jdataclasses

from dbdaora.exceptions import EntityNotFoundError

from ..data_sources.fallback import FallbackDataSource
from ..data_sources.redis import RedisDataSource


Entity = TypeVar('Entity')


@dataclass
class HashRepository(Generic[Entity]):
    entity_name: ClassVar[str]
    memory: RedisDataSource
    fallback: FallbackDataSource
    expire_time: int
    executor: ThreadPoolExecutor
    cache: Optional[Dict[str, Union[Entity, 'EntityNotFound']]] = None
    key_separator: str = ':'
    get_many_callback_error: Optional[Callable[[Exception], None]] = None

    def make_memory_key_from_entity(self, entity: Entity) -> str:
        raise NotImplementedError()

    def make_fallback_key_from_entity(self, entity: Entity) -> str:
        raise NotImplementedError()

    def get_entity_type(self, *ids: str) -> Type[Entity]:
        raise NotImplementedError()

    def add(self, entity: Entity) -> None:
        memory_key = self.make_memory_key_from_entity(entity)
        memory_data = self.make_memory_data_from_entity(entity)
        pipeline = self.memory.pipeline(transaction=False)  # type: ignore

        pipeline.delete(memory_key)
        pipeline.hmset(memory_key, memory_data)
        pipeline.expire(memory_key, self.expire_time)
        pipeline.execute()

        self.add_fallback(entity)

    def add_fallback(self, entity: Entity) -> None:
        fallback_key = self.make_fallback_key_from_entity(entity)
        fallback_data = self.make_fallback_data_from_entity(entity)
        self.fallback.put(fallback_key, fallback_data)

    def get(self, *ids: str, fields: Sequence[str]) -> Entity:
        memory_key = self.make_memory_key_from_ids(*ids)

        if self.cache is not None:
            entity = self.cache.get(memory_key)

            if entity is not None:
                if entity is ENTITY_NOT_FOUND:
                    raise EntityNotFoundError(*ids)

                return entity  # type: ignore

        memory_data = self.get_memory_data(memory_key, fields)
        not_found_key = self.make_not_found_key(*ids)

        if not memory_data:
            if self.memory.exists(not_found_key):
                raise EntityNotFoundError(*ids)

            fallback_key = self.make_fallback_key_from_ids(*ids)
            fallback_data = self.fallback.get(fallback_key)

            if fallback_data:
                memory_data = self.make_memory_data_from_fallback(
                    fallback_data
                )
                pipeline = self.memory.pipeline(transaction=False)  # type: ignore
                pipeline.hmset(memory_key, memory_data)
                pipeline.expire(memory_key, self.expire_time)
                pipeline.execute()

                if fields:
                    memory_data = self.make_memory_data_from_fallback(
                        fallback_data, fields=fields
                    )

            else:
                pipeline = self.memory.pipeline(transaction=False)  # type: ignore
                pipeline.set(not_found_key, '1')
                pipeline.expire(not_found_key, self.expire_time)
                pipeline.execute()

                if self.cache is not None:
                    self.cache[memory_key] = ENTITY_NOT_FOUND

                raise EntityNotFoundError(*ids)

        entity = self.make_entity(memory_data, *ids)

        if self.cache is not None:
            self.cache[memory_key] = entity

        return entity

    def get_memory_data(self, key: str, fields: Sequence[str]) -> Any:
        data = self.memory.hmget(key, fields)  # type: ignore
        return {k: v.decode() for k, v in zip(fields, data) if v is not None}

    def get_many(
        self, *many_ids: Iterable[str], fields: Iterable[str]
    ) -> List[Entity]:
        entities = []
        futures = [
            self.executor.submit(self.get, *ids, fields=fields)
            for ids in many_ids
        ]
        error: Optional[Exception] = None

        for future in futures:
            try:
                entities.append(future.result())

            except EntityNotFoundError:
                continue

            except Exception as e:
                error = e
                if self.get_many_callback_error is not None:
                    self.get_many_callback_error(error)

        if not entities:
            if error:
                raise error

            raise EntityNotFoundError(*many_ids)

        return entities

    def make_memory_key_from_ids(self, *ids: str) -> str:
        return self.key_separator.join((self.entity_name,) + ids)

    def make_not_found_key(self, *ids: str) -> str:
        return self.key_separator.join((self.entity_name, 'not-found') + ids)

    def make_fallback_key_from_ids(self, *ids: str) -> Any:
        return self.fallback.make_key(
            self.entity_name, self.key_separator.join(ids)
        )

    def make_memory_data_from_fallback(
        self, fallback_data: Any, fields: Optional[Sequence[str]] = None
    ) -> Any:
        return {
            k: int(v) if isinstance(v, bool) else v
            for k, v in jdataclasses.asdict(
                fallback_data, dumps_value=True
            ).items()
            if v is not None
            and (fields is None or (fields is not None and k in fields))
        }

    def make_memory_data_from_entity(self, entity: Any) -> Any:
        return {
            k: int(v) if isinstance(v, bool) else v
            for k, v in jdataclasses.asdict(entity, dumps_value=True).items()
            if v is not None
        }

    def make_fallback_data_from_entity(self, entity: Any) -> Any:
        return jdataclasses.asdict(entity, dumps_value=True)

    def make_entity(self, memory_data: Any, *ids: str) -> Entity:
        return jdataclasses.asdataclass(  # type: ignore
            memory_data, self.get_entity_type(*ids)
        )


class EntityNotFound:
    ...


ENTITY_NOT_FOUND = EntityNotFound()
