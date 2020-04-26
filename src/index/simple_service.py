import asyncio
from dataclasses import dataclass

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    HashRepository,
    make_hash_service,
)


@dataclass
class Person:
    id: str
    name: str
    age: int


def make_person(name: str, age: int) -> Person:
    return Person(name.replace(' ', '_').lower(), name, age)


class PersonRepository(HashRepository[str], entity_cls=Person):
    ...


async def make_memory_data_source() -> DictMemoryDataSource:
    return DictMemoryDataSource()


async def make_fallback_data_source() -> DictFallbackDataSource:
    return DictFallbackDataSource()


service = asyncio.run(
    make_hash_service(
        PersonRepository,
        memory_data_source_factory=make_memory_data_source,
        fallback_data_source_factory=make_fallback_data_source,
        repository_expire_time=60,
    )
)
person = make_person('John Doe', 33)
asyncio.run(service.add(person))

geted_person = asyncio.run(service.get_one(person.id))
print(geted_person)
