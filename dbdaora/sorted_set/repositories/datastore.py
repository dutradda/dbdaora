from google.cloud.datastore import Key

from dbdaora.repository.datastore import DatastoreRepository

from ..entity import SortedSetData, SortedSetEntityHint
from . import SortedSetRepository


class DatastoreSortedSetRepository(
    DatastoreRepository[SortedSetEntityHint, SortedSetData],
    SortedSetRepository[SortedSetEntityHint, Key],
):
    __skip_cls_validation__ = ('DatastoreSortedSetRepository',)
