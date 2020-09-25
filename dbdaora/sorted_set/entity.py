import dataclasses
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


SortedSetInput = Sequence[Union[str, float]]

SortedSetData = Union[RangeOutput, SortedSetInput]


def init_subclass(cls: Type[Any], bases: Tuple[Type[Any], ...]) -> None:
    if cls not in getattr(cls, '__skip_dataclass__', ()):
        if not hasattr(cls, '__annotations__'):
            cls.__annotations__ = {'id': str}

        for base_cls in bases:
            cls.__annotations__.update(
                {
                    k: v
                    for k, v in base_cls.__annotations__.items()
                    if k not in cls.__annotations__
                }
            )

        dataclasses.dataclass(cls)


class SortedSetEntity(Protocol):
    data: SortedSetData
    max_size: Optional[int] = None

    def __init_subclass__(cls) -> None:
        init_subclass(cls, (SortedSetEntity,))

    def __init__(
        self,
        *,
        data: SortedSetData,
        max_size: Optional[int] = None,
        **kwargs: Any,
    ):
        ...


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
    'SortedSetEntityHint', bound=Union[SortedSetEntity, SortedSetDictEntity],
)
