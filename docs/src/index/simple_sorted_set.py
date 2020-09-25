import asyncio
import dataclasses
import typing

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    SortedSetData,
    SortedSetRepository,
)


@dataclasses.dataclass
class Playlist:
    id: str
    data: SortedSetData
    max_size: typing.Optional[int] = None


class PlaylistRepository(SortedSetRepository[Playlist, str]):
    ...


repository = PlaylistRepository(
    memory_data_source=DictMemoryDataSource(),
    fallback_data_source=DictFallbackDataSource(),
    expire_time=60,
)
data = [('m1', 1), ('m2', 2), ('m3', 3)]
playlist = Playlist(id='my_plalist', data=data)
asyncio.run(repository.add(playlist))

geted_playlist = asyncio.run(repository.query(playlist.id).entity)
print(geted_playlist)
