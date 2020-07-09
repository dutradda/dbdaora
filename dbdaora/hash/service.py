from google.cloud.datastore import Key

from ..data_sources.fallback.mongodb import Key as MongoKey
from ..entity import Entity
from ..keys import FallbackKey
from ..service import Service
from .repositories import HashData


class HashService(Service[Entity, HashData, FallbackKey]):
    ...


class DatastoreHashService(Service[Entity, HashData, Key]):
    ...


class MongoHashService(Service[Entity, HashData, MongoKey]):
    ...
