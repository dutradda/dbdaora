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
        expected_exception: Optional[
            Union[Type[Exception], Tuple[Type[Exception], ...]]
        ] = None,
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
                return await self.fallback_function(*args, **kwargs)
            else:
                raise DBDaoraCircuitBreakerError(self, func.__name__)

        try:
            result = await func(*args, **kwargs)
        except self._expected_exception as e:
            self.set_failure(func.__name__, e)
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

    @property
    def expected_exception(
        self,
    ) -> Union[Type[Exception], Tuple[Type[Exception], ...]]:
        return self._expected_exception

    def set_failure(self, func_name: str, error: Exception) -> None:
        self._last_failure = error
        self.__call_failed()

        if self._failure_threshold == 0:
            raise DBDaoraCircuitBreakerError(self, func_name)

    def set_success(self) -> None:
        self.__call_succeeded()


class DBDaoraCircuitBreakerError(CircuitBreakerError):
    _circuit_breaker: AsyncCircuitBreaker
    last_failure: Optional[Exception]

    def __init__(
        self,
        circuit_breaker: CircuitBreaker,
        name_sufix: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(circuit_breaker, *args, **kwargs)
        self._name_sufix = name_sufix
        self.last_failure = self._circuit_breaker._last_failure

    def __str__(self, *args: Any, **kwargs: Any) -> str:
        return (
            'Circuit "%s" OPEN until %s (%d failures, %d sec remaining) (last_failure: %r)'
            % (
                self._circuit_breaker.name
                if self._name_sufix is None
                else f'{self._circuit_breaker.name}_{self._name_sufix}',
                self._circuit_breaker.open_until,
                self._circuit_breaker.failure_count,
                round(self._circuit_breaker.open_remaining),
                self._circuit_breaker.last_failure,
            )
        )
