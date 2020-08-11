from datetime import datetime
from typing import (
    Any,
    Awaitable,
    Callable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)


FuncReturn = TypeVar('FuncReturn')

STATE_CLOSED = 'closed'
STATE_OPEN = 'open'


class CircuitBreaker:
    _expected_exception: Union[Type[Exception], Tuple[Type[Exception], ...]]
    _failure_threshold: int
    name: str
    FAILURE_THRESHOLD: int
    RECOVERY_TIMEOUT: int
    EXPECTED_EXCEPTION: Type[Exception]
    FALLBACK_FUNCTION: Optional[Callable[..., Awaitable[FuncReturn]]]

    def __init__(
        self,
        failure_threshold: Optional[int] = None,
        recovery_timeout: Optional[int] = None,
        expected_exception: Optional[Union[Type[Exception], Tuple[Type[Exception], ...]]] = None,
        name: Optional[str] = None,
        fallback_function: Optional[
            Callable[..., Awaitable[FuncReturn]]
        ] = None,
    ): ...

    async def call(
        self,
        func: Callable[..., Awaitable[FuncReturn]],
        *args: Any,
        **kwargs: Any,
    ) -> FuncReturn: ...

    def __call__(
        self, wrapped: Callable[..., FuncReturn]
    ) -> Callable[..., FuncReturn]: ...

    @property
    def opened(self) -> bool: ...

    @property
    def fallback_function(
        self,
    ) -> Optional[Callable[..., Awaitable[FuncReturn]]]: ...

    @property
    def open_until(self) -> datetime: ...

    @property
    def open_remaining(self) -> int: ...

    @property
    def failure_count(self) -> int: ...

    @property
    def last_failure(self) -> Optional[Exception]: ...


class CircuitBreakerError(Exception):
    def __init__(
        self, circuit_breaker: CircuitBreaker, *args: Any, **kwargs: Any
    ): ...
