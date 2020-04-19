from datetime import datetime
from typing import Any, Awaitable, Callable, Optional, Type, TypeVar

from circuitbreaker import (
    STATE_CLOSED,
    STATE_OPEN,
    CircuitBreaker,
    CircuitBreakerError,
)


FuncReturn = TypeVar('FuncReturn')


class AsyncCircuitBreaker(CircuitBreaker):
    _last_failure: Optional[Exception]

    def __init__(
        self,
        failure_threshold: Optional[int] = None,
        recovery_timeout: Optional[int] = None,
        expected_exception: Optional[Type[Exception]] = None,
        name: Optional[str] = None,
        fallback_function: Optional[
            Callable[..., Awaitable[FuncReturn]]
        ] = None,
    ):
        self._last_failure = None
        self._failure_count = 0
        self._failure_threshold = (
            failure_threshold
            if failure_threshold is not None
            else self.FAILURE_THRESHOLD
        )
        self._recovery_timeout = recovery_timeout or self.RECOVERY_TIMEOUT
        self._expected_exception = (
            expected_exception or self.EXPECTED_EXCEPTION
        )
        self._fallback_function = fallback_function or self.FALLBACK_FUNCTION
        self._name = name
        self._state = STATE_CLOSED
        self._opened = datetime.utcnow()

    async def call(
        self,
        func: Callable[..., Awaitable[FuncReturn]],
        *args: Any,
        **kwargs: Any,
    ) -> FuncReturn:
        if self.opened:
            if self.fallback_function:
                await self.fallback_function(*args, **kwargs)
            else:
                raise CircuitBreakerError(self)
        try:
            result = await func(*args, **kwargs)
        except self._expected_exception as e:
            self._last_failure = e
            self.__call_failed()

            if self._failure_threshold == 0:
                raise CircuitBreakerError(self)

            raise

        self.__call_succeeded()
        return result

    def __call_succeeded(self) -> None:
        self._state = STATE_CLOSED
        self._last_failure = None
        self._failure_count = 0

    def __call_failed(self) -> None:
        self._failure_count += 1
        if self._failure_count >= self._failure_threshold:
            self._state = STATE_OPEN
            self._opened = datetime.utcnow()
