from dbdaora.data_sources.fallback.mongodb import Key

from . import HashEntity, HashRepository


class MongodbHashRepository(HashRepository[HashEntity, Key]):
    __skip_cls_validation__ = ('MongodbHashRepository',)
