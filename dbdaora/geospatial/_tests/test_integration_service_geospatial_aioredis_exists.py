import itertools

import asynctest
import pytest
from aioredis import RedisError
from jsondaora import dataclasses


@pytest.mark.asyncio
async def test_should_exists(fake_service, serialized_fake_entity):
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    assert fake_service.exists('fake')


@pytest.mark.asyncio
async def test_should_exists_without_cache(
    fake_service, serialized_fake_entity
):
    fake_service.exists_cache = None
    await fake_service.repository.memory_data_source.hmset(
        'fake:fake', *itertools.chain(*serialized_fake_entity.items())
    )
    assert await fake_service.exists('fake')


@pytest.mark.asyncio
async def test_should_exists_from_cache(fake_service, serialized_fake_entity):
    fake_service.repository.memory_data_source.exists = (
        asynctest.CoroutineMock()
    )
    fake_service.exists_cache['fake'] = True

    assert await fake_service.exists('fake')
    assert not fake_service.repository.memory_data_source.exists.called


@pytest.mark.asyncio
async def test_should_exists_from_fallback_after_open_circuit_breaker(
    fake_service, fake_entity, mocker
):
    fake_service.repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake'
    ] = dataclasses.asdict(fake_entity)

    assert await fake_service.exists('fake')
    assert fake_service.logger.warning.call_count == 1


@pytest.mark.asyncio
async def test_should_exists_from_fallback_after_open_circuit_breaker_without_cache(
    fake_service, fake_entity, mocker
):
    fake_service.exists_cache = None
    fake_service.repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=RedisError
    )
    fake_service.repository.fallback_data_source.db[
        'fake:fake'
    ] = dataclasses.asdict(fake_entity)

    assert await fake_service.exists('fake')
    assert fake_service.logger.warning.call_count == 1
