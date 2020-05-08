import pytest
from aioredis import RedisError


@pytest.mark.asyncio
async def test_should_add(fake_service, serialized_fake_entity, fake_entity):
    await fake_service.repository.add(fake_entity)

    entity = await fake_service.get_one('fake')

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_add_to_fallback_after_open_circuit_breaker(
    fake_service, serialized_fake_entity, fake_entity, mocker
):
    fake_multi_exec = mocker.MagicMock()
    fake_multi_exec.return_value.execute.side_effect = RedisError
    fake_service.repository.memory_data_source.multi_exec = fake_multi_exec
    await fake_service.add(fake_entity)

    entity = await fake_service.get_one('fake')

    assert entity == fake_entity
    assert fake_service.logger.warning.call_count == 2
