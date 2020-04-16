from typing import Any, Callable, TypeVar, Type, Optional

FuncReturn = TypeVar('FuncReturn')


class CircuitBreaker:
    _expected_exception: Type[Exception]
    _failure_threshold: int
    name: str

    def __init__(
        self,
        failure_threshold: Optional[int] = None,
        recovery_timeout: Optional[int] =None,
        expected_exception: Optional[Type[Exception]] =None,
        name: Optional[str] =None,
        fallback_function: Optional[Callable[..., FuncReturn]] =None,
    ): ...

    def call(
        self, func: Callable[..., FuncReturn], *args: Any, **kwargs: Any
    ) -> FuncReturn: ...

    def __call__(
        self, wrapped: Callable[..., FuncReturn]
    ) -> Callable[..., FuncReturn]:
        ...


class CircuitBreakerError(Exception):
    ...
