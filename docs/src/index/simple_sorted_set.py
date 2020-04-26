import asyncio
from dataclasses import dataclass

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    SortedSetData,
    SortedSetRepository,
)


@dataclass
class Playlist:
    id: str
    values: SortedSetData


class PlaylistRepository(SortedSetRepository[str]):
    entity_cls = Playlist


repository = PlaylistRepository(
    memory_data_source=DictMemoryDataSource(),
    fallback_data_source=DictFallbackDataSource(),
    expire_time=60,
)
values = [('m1', 1), ('m2', 2), ('m3', 3)]
playlist = Playlist(id='my_plalist', values=values)
asyncio.run(repository.add(playlist))

geted_playlist = asyncio.run(repository.query(playlist.id).entity)
print(geted_playlist)
