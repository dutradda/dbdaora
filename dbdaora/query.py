import dataclasses
from typing import Any, ClassVar, Generic, List, Optional, Tuple, Type, Union

from dbdaora.entity import Entity, EntityData
from dbdaora.exceptions import RequiredKeyAttributeError
from dbdaora.keys import FallbackKey


@dataclasses.dataclass(init=False)
class BaseQuery(Generic[Entity, EntityData, FallbackKey]):
    repository: 'MemoryRepository[Entity, EntityData, FallbackKey]'
    memory: bool

    def __init__(
        self,
        repository: 'MemoryRepository[Entity, EntityData, FallbackKey]',
        *args: Any,
        memory: bool = True,
        **kwargs: Any,
    ):
        self.repository = repository
        self.memory = memory

    def make_key_parts(self, *args: Any, **kwargs: Any) -> List[Any]:
        missed_key_attrs = []
        key_parts = [
            args[i] if i < len(args) else missed_key_attrs.append((i, key_attr))  # type: ignore
            for i, key_attr in enumerate(self.repository.key_attrs)
        ]

        try:
            for i, attr_name in missed_key_attrs:
                key_parts[i] = kwargs[attr_name]
        except KeyError:
            raise RequiredKeyAttributeError(
                type(self.repository).__name__,
                attr_name,
                self.repository.key_attrs,
            )

        return key_parts

    @property
    async def entity(self) -> Entity:
        raise NotImplementedError()  # pragma: no cover

    @property
    async def entities(self) -> List[Entity]:
        raise NotImplementedError()  # pragma: no cover

    @property
    async def delete(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    @property
    async def exists(self) -> bool:
        raise NotImplementedError()  # pragma: no cover


class Query(BaseQuery[Entity, EntityData, FallbackKey]):
    key_parts: List[Any]

    def __init__(
        self,
        repository: 'MemoryRepository[Entity, EntityData, FallbackKey]',
        *args: Any,
        memory: bool = True,
        key_parts: Optional[List[Any]] = None,
        **kwargs: Any,
    ):
        self.repository = repository
        self.memory = memory

        if key_parts:
            self.key_parts = key_parts
        else:
            self.key_parts = self.make_key_parts(*args, **kwargs)

    def attribute_from_key(self, attr_name: str) -> Any:
        return self.key_parts[self.repository.key_attrs.index(attr_name)]

    @property
    async def delete(self) -> None:
        await self.repository.delete(self)

    @property
    async def add(self) -> None:
        await self.repository.add(query=self)

    @property
    async def entity(self) -> Entity:
        return await self.repository.entity(self)

    @property
    async def exists(self) -> bool:
        return await self.repository.exists(self)


class QueryMany(BaseQuery[Entity, EntityData, FallbackKey]):
    query_cls: ClassVar[Type[Query[Entity, EntityData, FallbackKey]]] = Query[
        Entity, EntityData, FallbackKey
    ]
    queries: List[Query[Entity, EntityData, FallbackKey]]

    def __init__(
        self,
        repository: 'MemoryRepository[Entity, EntityData, FallbackKey]',
        *args: Any,
        many: List[Union[Any, Tuple[Any, ...]]],
        memory: bool = True,
        many_key_parts: Optional[List[List[Any]]] = None,
        **kwargs: Any,
    ):
        self.memory = memory
        self.repository = repository

        if many_key_parts is None:
            many_key_parts = []
            many_key_attrs = (
                (repository.many_key_attrs,)
                if isinstance(repository.many_key_attrs, str)
                else repository.many_key_attrs
            )

            for many_input in many:
                kwargs_i = {}

                if isinstance(many_input, tuple):
                    start_index = len(many_key_attrs) - len(many_input)
                    start_index = start_index if start_index >= 0 else 0
                    kwargs_i.update(
                        {
                            name: input_
                            for name, input_ in zip(
                                many_key_attrs[start_index:], many_input
                            )
                        }
                    )
                else:
                    kwargs_i[many_key_attrs[-1]] = many_input

                many_key_parts.append(
                    self.make_key_parts(*args, **kwargs, **kwargs_i)
                )

        self.queries = [
            self.query_cls(repository, memory=memory, key_parts=key_parts)
            for key_parts in many_key_parts
        ]

    @property
    async def entities(self) -> List[Entity]:
        return await self.repository.entities(self)


def make(
    *args: Any, **kwargs: Any
) -> BaseQuery[Entity, EntityData, FallbackKey]:
    if kwargs.get('many') or kwargs.get('many_key_parts'):
        return QueryMany(*args, **kwargs)

    return Query(*args, **kwargs)


from .repository import MemoryRepository  # noqa isort:skip
