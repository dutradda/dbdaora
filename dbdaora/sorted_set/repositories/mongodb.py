from dbdaora.data_sources.fallback.mongodb import Key

from . import SortedSetRepository


class MongodbSortedSetRepository(SortedSetRepository[Key]):
    __skip_cls_validation__ = ('MongodbSortedSetRepository',)
    fallback_data_source_key_cls = Key
