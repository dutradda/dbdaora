from dbdaora.data_sources.fallback.mongodb import Key

from ..entity import SortedSetEntityHint
from . import SortedSetRepository


class MongodbSortedSetRepository(
    SortedSetRepository[SortedSetEntityHint, Key]
):
    __skip_cls_validation__ = ('MongodbSortedSetRepository',)
    fallback_data_source_key_cls = Key
