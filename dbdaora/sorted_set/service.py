from typing import Any

from dbdaora.service import Service

from ..keys import FallbackKey
from .entity import SortedSetData


class SortedSetService(Service[Any, SortedSetData, FallbackKey]):
    ...
