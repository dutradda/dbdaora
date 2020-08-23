import asyncio

from dbdaora import (
    DictFallbackDataSource,
    DictMemoryDataSource,
    SortedSetEntity,
    SortedSetRepository,
)


class PlaylistRepository(SortedSetRepository[str]):
    ...


repository = PlaylistRepository(
    memory_data_source=DictMemoryDataSource(),
    fallback_data_source=DictFallbackDataSource(),
    expire_time=60,
)
values = [('m1', 1), ('m2', 2), ('m3', 3)]
playlist = SortedSetEntity(id='my_plalist', values=values)
asyncio.run(repository.add(playlist))

geted_playlist = asyncio.run(repository.query(playlist.id).entity)
print(geted_playlist)
