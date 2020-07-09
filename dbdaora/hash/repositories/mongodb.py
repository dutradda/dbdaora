from dbdaora.data_sources.fallback.mongodb import Key

from . import HashRepository


class MongodbHashRepository(HashRepository[Key]):
    __skip_cls_validation__ = ('MongodbHashRepository',)
