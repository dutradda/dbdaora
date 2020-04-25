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


@dataclass
class PlayList(SortedSetEntity):
    person_id: str


class PlaylistRepository(SortedSetRepository[str]):
    entity_name = 'playlist'
    key_attrs = ('person_id',)
    entity_cls = PlayList
    entity_id_name = 'person_id'


playlist_repository = PlaylistRepository(
    memory_data_source=DictMemoryDataSource(),
    fallback_data_source=DictFallbackDataSource(),
    expire_time=60,
)


def make_playlist(person_id: str, *musics_ids: str) -> PlayList:
    return PlayList(
        person_id=person_id,
        values=[(id_, i) for i, id_ in enumerate(musics_ids)],
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
