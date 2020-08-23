from dataclasses import dataclass
from functools import partial
from typing import Optional

import pytest
from aioredis import RedisError

from dbdaora import (
    SortedSetData,
    SortedSetRepository,
    make_aioredis_data_source,
    make_sorted_set_service,
)


@dataclass
class FakeEntity:
    fake_id: str
    values: SortedSetData
    max_size: Optional[int] = None


@pytest.fixture
def fallback_cb_error():
    return KeyError


@pytest.fixture
def fake_entity_cls():
    return FakeEntity


@pytest.fixture
def fake_repository_cls(fake_entity_cls):
    class FakeRepository(SortedSetRepository[str]):
        key_attrs = ('fake_id',)
        entity_cls = fake_entity_cls

    return FakeRepository


@pytest.fixture
def fake_entity(fake_entity_cls):
    return fake_entity_cls(fake_id='fake', values=[b'1', b'2'])


@pytest.fixture
def fake_entity_withscores(fake_entity_cls):
    return fake_entity_cls(fake_id='fake', values=[(b'1', 0), (b'2', 1)])


@pytest.mark.asyncio
@pytest.fixture
async def fake_service(
    mocker, fallback_data_source, fake_repository_cls, fallback_cb_error
):
    memory_data_source_factory = partial(
        make_aioredis_data_source,
        'redis://',
        'redis://localhost/1',
        'redis://localhost/2',
    )

    async def fallback_data_source_factory():
        return fallback_data_source

    service = await make_sorted_set_service(
        fake_repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time=1,
        cb_failure_threshold=0,
        cb_recovery_timeout=10,
        cb_expected_exception=RedisError,
        cb_expected_fallback_exception=fallback_cb_error,
        logger=mocker.MagicMock(),
    )

    yield service

    await service.shutdown()
