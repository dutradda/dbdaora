import asyncio
from typing import TypedDict

from jsondaora import jsondaora

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    HashRepository,
    SortedSetData,
    SortedSetRepository,
    make_hash_service,
)


# Data Source Layer


async def make_memory_data_source() -> DictMemoryDataSource:
    return DictMemoryDataSource()


async def make_fallback_data_source() -> DictFallbackDataSource:
    return DictFallbackDataSource()


# Domain Layer


@jsondaora
class Person(TypedDict):
    id: str
    name: str
    age: int


def make_person(name: str, age: int) -> Person:
    return Person(id=name.replace(' ', '_').lower(), name=name, age=age)


class PersonRepository(HashRepository[str], entity_cls=Person):
    ...


person_service = asyncio.run(
    make_hash_service(
        PersonRepository,
        memory_data_source_factory=make_memory_data_source,
        fallback_data_source_factory=make_fallback_data_source,
        repository_expire_time=60,
    )
)


@jsondaora
class Playlist(TypedDict):
    person_id: str
    values: SortedSetData


class PlaylistRepository(SortedSetRepository[str]):
    entity_cls = Playlist
    id_name = 'person_id'


playlist_repository = PlaylistRepository(
    memory_data_source=DictMemoryDataSource(),
    fallback_data_source=DictFallbackDataSource(),
    expire_time=60,
)


def make_playlist(person_id: str, *musics_ids: str) -> Playlist:
    return Playlist(
        person_id=person_id,
        values=[(id_, i) for i, id_ in enumerate(musics_ids)],
    )


# Application Layer


async def main() -> None:
    person = make_person('John Doe', 33)
    playlist = make_playlist(person['id'], 'm1', 'm2', 'm3')

    await person_service.add(person)
    await playlist_repository.add(playlist)

    goted_person = await person_service.get_one(person['id'])
    goted_playlist = await playlist_repository.query(goted_person['id']).entity

    print(goted_person)
    print(goted_playlist)


asyncio.run(main())
