# dataclassesdb

<p align="center" style="margin: 3em">
  <a href="https://github.com/dutradda/dataclassesdb">
    <img src="dataclassesdb.svg" alt="dataclassesdb" width="300"/>
  </a>
</p>

<p align="center">
    <em>Generates <b>SQL</b> and <b>NoSQL</b> database models from @dataclass</em>
</p>

---

**Documentation**: <a href="https://dutradda.github.io/dataclassesdb" target="_blank">https://dutradda.github.io/dataclassesdb</a>

**Source Code**: <a href="https://github.com/dutradda/dataclassesdb" target="_blank">https://github.com/dutradda/dataclassesdb</a>

---


## Key Features

- Fast start data modeling with persistence

- Supports from simple database schemas to complex one

- Integrate with:
    + `SQLALchemy`
    + `aioredis` (*soon*)
    + `google-datastore` (*soon*)
    + `mongodb` (*planned*)
    + `elasticsearch` (*planned*)
    + `aws-dynamodb` (*planned*)

- Same interface as [`sqlalchemy.orm.session.Session`](https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session)

- Easy integration with other data sources

- Supports redis data structure (like hashs, sets, etc) to store objects


## Requirements

Python 3.7+


## Instalation

```
$ pip install dataclassesdb 
```


## Basic SQLAlchemy example

```python
import asyncio

from dataclassesdb import DataSourceType, SessionFactory
from dataclasses import dataclass


@dataclass
class Music:
    name: str


@dataclass
class Person:
    name: str
    age: int
    music: Music


session = SessionFactory.make(
    Music,
    Person,
    data_source=DataSourceType.RELATIONAL_SQLALCHEMY,
    data_source_args=dict(
        db_url='sqlite://',
        backrefs=True,
        create_tables=True,
    )
)

person = Person(
    name='John',
    age=40,
    music=Music('Imagine')
)
session.add(person)  # commit=True by default

musicQuery = session.query(Address)
musics = musicQuery.filter(name='Imagine').all()

loop = asyncio.get_event_loop()
print(loop.run_until_complete(musics))
```

```bash
[
  Music(
    name='Imagine',
    _id=1,
    backrefs=Backrefs(
       person=[Person(age=40, name=John, _id=1)]
    )
  )
]
```


## Basic aioredis with hash data type example

```python
import asyncio

from dataclassesdb import DataSourceType, SessionFactoryAsync
from dataclasses import dataclass


@dataclass
class Music:
    name: str


@dataclass
class Person:
    name: str
    age: int
    music: Music


async def get_musics():
    session = await SessionFactoryAsync.make(
        Address,
        Person,
        data_source=DataSourceType.MEMORY_AIOREDIS,
        data_source_args=dict(
            db_url='redis://',
            backrefs=True,
        )
    )

    person = Person(
        name='John',
        age=40,
        music=Music('Imagine')
    )
    await session.add(person)

    musicQuery = await session.query(Address)
    return await musicQuery.filter(name='Imagine').all()

loop = asyncio.get_event_loop()
print(loop.run_until_complete(musics))
```

```python
[
  Music(
    name='Imagine',
    _id=1,
    backrefs=Backrefs(
       person=[Person(age=40, name=John, _id=1)]
    )
  )
]
```


## Basic aioredis with sorted set data type example

```python
from dataclassesdb import (
    DataSourceType,
    MemorySortedSetRanked,
    ModelKey,
    SessionFactoryAsync,
    SortedValue,
)
from dataclasses import dataclass


@dataclass
class Music:
    id: ModelKey(str)
    name: str


class Playlist(MemorySortedSetRanked):
    value_type = Music


@dataclass
class Person:
    name: str
    age: int
    playlist: Playlist


session = await SessionFactoryAsync.make(
    Music,
    Playlist,
    Person,
    data_source=DataSourceType.MEMORY_AIOREDIS,
    data_source_args=dict(
        db_url='redis://',
        backrefs=True,
    )
)

person = Person(
    name='John',
    age=40,
    playlist=Playlist(
        Music(id='imagine', name='Imagine'),
        Music(id='come-together', name='Come Together'),
    )
)
await session.add(person)

playlistQuery = await session.query(Playlist)
playlist = await playlistQuery.filter(Person.name=='John').one(withrank=True)

print(playlist)
```

```python
[
  SortedValue(
    rank=1,
    key='imagine'
  ),
  SortedValue(
    rank=2,
    key='come-together'
  )
]
```
