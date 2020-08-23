from google.cloud.datastore import Key

from . import SortedSetService


class DatastoreSortedSetService(SortedSetService[Key]):
    ...
