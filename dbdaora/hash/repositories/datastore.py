from google.cloud.datastore import Key

from dbdaora.repository.datastore import DatastoreRepository

from . import HashData, HashEntity, HashRepository


class DatastoreHashRepository(
    DatastoreRepository[HashEntity, HashData], HashRepository[HashEntity, Key],
):
    __skip_cls_validation__ = ('DatastoreHashRepository',)
