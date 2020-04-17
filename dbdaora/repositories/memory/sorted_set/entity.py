from dataclasses import dataclass

from dbdaora.data_sources.memory import SortedSetData


@dataclass
class SortedSetEntity:
    id: str
    data: SortedSetData
