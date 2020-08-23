from ...data_sources.fallback.mongodb import Key as MongoKey
from . import SortedSetService


class MongoSortedSetService(SortedSetService[MongoKey]):
    ...
