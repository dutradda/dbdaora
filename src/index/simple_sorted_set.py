import asyncio
from dataclasses import dataclass

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    SortedSetEntity,
    SortedSetRepository,
)


@dataclass
class Playlist(SortedSetEntity):
    id: str


class PlaylistRepository(SortedSetRepository[str]):
    entity_cls = Playlist
    key_attrs = ('id',)


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
