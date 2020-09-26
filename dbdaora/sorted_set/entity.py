from typing import (  # type: ignore
    Any,
    Dict,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    TypedDict,
    Union,
    _TypedDictMeta,
)

from dbdaora.data_sources.memory import RangeOutput
from dbdaora.entity import init_subclass


SortedSetInput = Sequence[Union[str, float]]

SortedSetData = Union[RangeOutput, SortedSetInput]


class SortedSetEntityProtocol(Protocol):
    data: SortedSetData
    max_size: Optional[int] = None

    def __init__(
        self,
        *,
        data: SortedSetData,
        max_size: Optional[int] = None,
        **kwargs: Any,
    ):
        ...


class SortedSetEntity(SortedSetEntityProtocol):
    data: SortedSetData
    max_size: Optional[int] = None

    def __init_subclass__(cls) -> None:
        init_subclass(cls, (SortedSetEntity,))


class SortedSetDictEntityMeta(_TypedDictMeta):  # type: ignore
    def __init__(
        cls, name: str, bases: Tuple[Type[Any], ...], attrs: Dict[str, Any]
    ):
        super().__init__(name, bases, attrs)
        init_subclass(cls, bases)


class SortedSetDictEntity(TypedDict, metaclass=SortedSetDictEntityMeta):
    data: SortedSetData
    max_size: Optional[int]


SortedSetEntityHint = TypeVar(
    'SortedSetEntityHint',
    bound=Union[SortedSetEntityProtocol, SortedSetDictEntity],
)
