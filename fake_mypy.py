from dataclasses import dataclass
from typing import TypeVar, Generic, ClassVar


TV1 = TypeVar('TV1')
TV2 = TypeVar('TV2')


@dataclass
class T(Generic[TV1, TV2]):
    t: ClassVar[int]
    tv1: TV1
    tv2: TV2


class T2(T[TV1, TV2]):
    def fake(self, tv1: TV1) -> str:
        ...


class T3(T2[int, float]):
    ...


t = T3(1, .2)
t.fake('1')
