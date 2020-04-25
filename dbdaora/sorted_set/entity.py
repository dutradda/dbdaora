from dataclasses import dataclass
from typing import Any, Sequence, TypedDict, Union

from dbdaora.data_sources.memory import RangeOutput


SortedSetInput = Sequence[Union[str, float]]

SortedSetData = Union[RangeOutput, SortedSetInput]


@dataclass(init=False)
class SortedSetEntity:
    values: SortedSetData

    def __init__(self, values: SortedSetData, **kwargs: Any):
        self.values = values


class SortedSetDictEntity(TypedDict):
    values: SortedSetData


# see https://github.com/python/mypy/issues/4300
# Entity = Union[SortedSetEntity, SortedSetDictEntity]
Entity = Union[SortedSetEntity]
