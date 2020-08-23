import dataclasses
from typing import Optional, Sequence, TypedDict, Union

from jsondaora import jsondaora

from dbdaora.data_sources.memory import RangeOutput


SortedSetInput = Sequence[Union[str, float]]

SortedSetData = Union[RangeOutput, SortedSetInput]


@dataclasses.dataclass
class SortedSetEntity:
    id: str
    values: SortedSetData
    max_size: Optional[int] = None


@jsondaora
class SortedSetDictEntity(TypedDict):
    id: str
    values: SortedSetData
    max_size: Optional[int]


Entity = Union[SortedSetEntity, SortedSetDictEntity]
