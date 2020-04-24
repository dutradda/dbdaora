# dbdaora

<p align="center" style="margin: 3em">
  <a href="https://github.com/dutradda/dbdaora">
    <img src="https://dutradda.github.io/dbdaora/dbdaora.svg" alt="dbdaora" width="300"/>
  </a>
</p>

<p align="center">
    <em>Communicates with <b>NoSQL</b> (and <b>SQL</b> for future) databases using repository and service patterns and python dataclasses</em>
</p>

---

**Documentation**: <a href="https://dutradda.github.io/dbdaora/" target="_blank">https://dutradda.github.io/dbdaora/</a>

**Source Code**: <a href="https://github.com/dutradda/dbdaora" target="_blank">https://github.com/dutradda/dbdaora</a>

---


## Key Features

- **Creates an optional service layer with cache and circuit breaker**

- **Supports for redis data types:**
    + Hash
    + Sorted Set
    + *(Others data types are planned)*

- **Backup redis data into other databases:**
    + Google Datastore
    + Mongodb *(soon)*
    + SQL databases with SQLAlchemy *(soon)*
    + *(Others data bases are planned)*

- *Support for other databases are in development.*


## Requirements

 - Python 3.8+
 - [jsondaora](https://github.com/dutradda/jsondaora) for data validation/parsing
 - circuitbreaker
 - cachetools

 - Optionals:
    + aioredis
    + google-cloud-datastore


## Instalation

```
$ pip install dbdaora
```


## Simple redis hash example

```python
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
    entity_name = 'person'
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

```


## Simple redis sorted set example

```python
import asyncio

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    SortedSetEntity,
    SortedSetRepository,
)


class PlayList(SortedSetEntity):
    ...


class PlaylistRepository(SortedSetRepository[str]):
    entity_name = 'playlist'
    key_attrs = ('id',)
    entity_cls = PlayList


repository = PlaylistRepository(
    memory_data_source=DictMemoryDataSource(),
    fallback_data_source=DictFallbackDataSource(),
    expire_time=60,
)
data = [('m1', 1), ('m2', 2), ('m3', 3)]
playlist = PlayList('my_plalist', data)
asyncio.run(repository.add(playlist))

geted_playlist = asyncio.run(repository.query(playlist.id).entity)
print(geted_playlist)

```


## Using the service layer

The service layer uses the backup dataset when redis is offline, opening a circuit breaker.

It has an optional cache system too.


```python
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


class PersonRepository(HashRepository[str]):
    entity_name = 'person'
    entity_cls = Person
    key_attrs = ('id',)


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

```


## Simple Domain Model Example


```python
import asyncio
from dataclasses import dataclass

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    HashRepository,
    SortedSetEntity,
    SortedSetRepository,
    make_hash_service,
)


# Data Source Layer


async def make_memory_data_source() -> DictMemoryDataSource:
    return DictMemoryDataSource()


async def make_fallback_data_source() -> DictFallbackDataSource:
    return DictFallbackDataSource()


# Domain Layer


@dataclass
class Person:
    id: str
    name: str
    age: int


def make_person(name: str, age: int) -> Person:
    return Person(name.replace(' ', '_').lower(), name, age)


class PersonRepository(HashRepository[str]):
    entity_name = 'person'
    entity_cls = Person
    key_attrs = ('id',)


person_service = asyncio.run(
    make_hash_service(
        PersonRepository,
        memory_data_source_factory=make_memory_data_source,
        fallback_data_source_factory=make_fallback_data_source,
        repository_expire_time=60,
    )
)


class PlayList(SortedSetEntity):
    ...


class PlaylistRepository(SortedSetRepository[str]):
    entity_name = 'playlist'
    key_attrs = ('id',)
    entity_cls = PlayList


playlist_repository = PlaylistRepository(
    memory_data_source=DictMemoryDataSource(),
    fallback_data_source=DictFallbackDataSource(),
    expire_time=60,
)


def make_playlist(person_id: str, *musics_ids: str) -> PlayList:
    return PlayList(
        person_id, data=[(id_, i) for i, id_ in enumerate(musics_ids)]
    )


# Application Layer


async def main() -> None:
    person = make_person('John Doe', 33)
    playlist = make_playlist(person.id, 'm1', 'm2', 'm3')

    await person_service.add(person)
    await playlist_repository.add(playlist)

    goted_person = await person_service.get_one(person.id)
    goted_playlist = await playlist_repository.query(goted_person.id).entity

    print(goted_person)
    print(goted_playlist)


asyncio.run(main())

```
