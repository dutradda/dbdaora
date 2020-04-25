from google.cloud.datastore import Key

from . import HashRepository


class DatastoreHashRepository(HashRepository[Key]):
    __skip_cls_validation__ = ('DatastoreHashRepository',)
