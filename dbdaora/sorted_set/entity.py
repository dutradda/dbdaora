from dataclasses import dataclass
from typing import Sequence, Union

from dbdaora.data_sources.memory import RangeOutput


SortedSetInput = Sequence[Union[str, float]]

SortedSetData = Union[RangeOutput, SortedSetInput]


@dataclass
class SortedSetEntity:
    id: str
    data: SortedSetData
