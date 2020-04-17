from dataclasses import dataclass
from typing import Dict, Iterable, Union

from dbdaora.data_sources.memory import SortedSetData


@dataclass
class SortedSetEntity:
    id: str
    data: SortedSetData
