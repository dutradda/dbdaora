from google.cloud.datastore import Key

from . import HashRepository


class DatastoreHashRepository(HashRepository[Key]):
    ...
