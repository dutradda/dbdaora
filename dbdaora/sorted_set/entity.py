from typing import Any, Dict, Protocol, Sequence, TypedDict, Union

from dbdaora.data_sources.memory import RangeOutput


SortedSetInput = Sequence[Union[str, float]]

SortedSetData = Union[RangeOutput, SortedSetInput]


class SortedSetEntity(Protocol):
    @property
    def values(self) -> SortedSetData:
        raise NotImplementedError()  # pragma: no cover

    def __init__(self, values: SortedSetData, **kwargs: Any):
        raise NotImplementedError()  # pragma: no cover


class SortedSetDictEntity(TypedDict):
    values: SortedSetData


# see https://github.com/python/mypy/issues/4300
# Entity = Union[SortedSetEntity, SortedSetDictEntity]
Entity = Union[SortedSetEntity, Dict[str, Any]]
