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
