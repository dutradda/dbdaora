from ...data_sources.fallback.mongodb import Key as MongoKey
from ...entity import Entity
from . import HashService


class MongoHashService(HashService[Entity, MongoKey]):
    ...
