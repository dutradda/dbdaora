import asyncio
from dataclasses import dataclass

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    HashRepository,
)


@dataclass
class Person:
    id: str
    name: str
    age: int


def make_person(name: str, age: int) -> Person:
    return Person(name.replace(' ', '_').lower(), name, age)


class PersonRepository(HashRepository[str]):
    entity_cls = Person
    key_attrs = ('id',)


repository = PersonRepository(
    memory_data_source=DictMemoryDataSource(),
    fallback_data_source=DictFallbackDataSource(),
    expire_time=60,
)
person = make_person('John Doe', 33)
asyncio.run(repository.add(person))

geted_person = asyncio.run(repository.query(person.id).entity)
print(geted_person)
