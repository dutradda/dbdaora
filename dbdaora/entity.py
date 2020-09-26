import dataclasses
from typing import Tuple, Type, TypeVar


Entity = TypeVar('Entity')

EntityData = TypeVar('EntityData')


def init_subclass(cls: Type[Entity], bases: Tuple[Type[Entity], ...]) -> None:
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
