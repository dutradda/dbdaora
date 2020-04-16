# dbdaora

<p align="center" style="margin: 3em">
  <a href="https://github.com/dutradda/dbdaora">
    <img src="https://dutradda.github.io/dbdaora/dbdaora.svg" alt="dbdaora" width="300"/>
  </a>
</p>

<p align="center">
    <em>Generates <b>SQL</b> and <b>NoSQL</b> database models from @dataclass</em>
</p>

---

**Documentation**: <a href="https://dutradda.github.io/dbdaora" target="_blank">https://dutradda.github.io/dbdaora</a>

**Source Code**: <a href="https://github.com/dutradda/dbdaora" target="_blank">https://github.com/dutradda/dbdaora</a>

---


## Key Features

- Fast start data modeling with persistence

- Supports from simple database schemas to complex one

- Integrate with:
    + `aioredis`
    + `google-datastore` (*soon*)
    + `SQLALchemy` (*soon*)
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
$ pip install dbdaora 
```


## Basic aioredis with hash data type example

```python
from dbdaora import DataStoreDataSource


class Person(TypedDict):
    name: str
    age: int
    best_musics: SortedSet[Music]
    __key_attributes__ = ('name',)


class PersonBestMusics(TypedDict):
    person: Person
    musics: SortedSetValues


repo = Repository(
    entity=Person,
    fallback_data_source=DataStoreDataSource()
)



```


```python
import asyncio
from typing import TypedDict

from dbdaora import make_aioredis_session


class Music(TypedDict):
    name: str


class Person(TypedDict):
    name: str
    age: int
    music: Music
    __key_attributes__ = ('name',)


async def get_musics():
    session = await make_aioredis_session(
        Person,
        data_source='redis://',
    )

    person = Person(
        name='John',
        age=40,
        music=Music('Imagine')
    )
    await session.add(person)

    personQuery = session.query(Person)
    return await personQuery.key(name='john').one()


loop = asyncio.get_event_loop()
print(loop.run_until_complete(musics))
```

```python
{
    'age': 40,
    'name': 'John',
    'music': {
        'name': 'Imagine',
        '_id': 1,
    }
}
```


## Basic aioredis with sorted set data type example

```python
import asyncio
from typing import TypedDict

from dbdaora import make_aioredis_session, SortedSet


class Music(TypedDict):
    name: str
    __key_attributes__ = ('name',)


class Person(TypedDict):
    name: str
    age: int
    __key_attributes__ = ('name',)


class PersonMusics(SortedSet):
    __key__ = 'musics'
    __entity_key__ = Person


async def get_musics():
    session = await make_aioredis_session(
        Person,
        data_source='redis://',
    )

    person = Person(
        name='John',
        age=40,
    )
    m1 = Music('Imagine')
    m1 = Music('Peace')
    musics = PersonMusics(person, {m1['name']: .1, m2['name']: .112})
    await session.add(musics)

    musicsQuery = session.query(PersonMusics)
    return await musicsQuery.key(name='john').all()


loop = asyncio.get_event_loop()
print(loop.run_until_complete(musics))
```

```python
[
    {
        '_id': 1,
        'score': 1,
    },
    {
        '_id': 2,
        'score': 2,
    },
]
```


## Basic SQLAlchemy example

```python
import asyncio

from dbdaora import DataSourceType, SessionFactory
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

musicQuery = session.query(Music)
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
