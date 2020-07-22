from google.cloud.datastore import Key as DatastoreKey

from ...entity import Entity
from . import HashService


class DatastoreHashService(HashService[Entity, DatastoreKey]):
    ...
