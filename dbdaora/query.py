import dataclasses
from typing import Any, Generic, List, Optional

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


def make(
    *args: Any, **kwargs: Any
) -> BaseQuery[Entity, EntityData, FallbackKey]:
    return Query(*args, **kwargs)


from .repository import MemoryRepository  # noqa isort:skip
