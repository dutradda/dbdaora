import pytest
from aioredis import RedisError
from cachetools import TTLCache

from dbdaora import AsyncCircuitBreaker, HashService


@pytest.fixture
def fake_service(aioredis_repository, mocker):
    circuit_breaker = AsyncCircuitBreaker(
        failure_threshold=0,
        recovery_timeout=10,
        expected_exception=RedisError,
        name='fake',
    )
    cache = TTLCache(maxsize=1, ttl=1)
    return HashService(
        aioredis_repository, circuit_breaker, cache, logger=mocker.MagicMock(),
    )
