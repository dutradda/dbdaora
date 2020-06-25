import asynctest
import pytest

from dbdaora import EntityNotFoundError, HashService
from dbdaora.service import CACHE_ALREADY_NOT_FOUND


@pytest.fixture
def service():
    s = HashService(
        repository=asynctest.MagicMock(),
        circuit_breaker=asynctest.MagicMock(),
        get_entity_timeout=1,
        cache={},
    )
    s.entity_circuit = asynctest.CoroutineMock()
    s.entities_circuit = asynctest.CoroutineMock()
    return s


@pytest.mark.asyncio
async def test_should_set_cache_entity_not_found_when_getting_one(service):
    error = EntityNotFoundError()
    service.entity_circuit.side_effect = error

    with pytest.raises(EntityNotFoundError) as exc_info:
        await service.get_one('fake')

    assert exc_info.value == error
    assert service.cache['fake'] == CACHE_ALREADY_NOT_FOUND


@pytest.mark.asyncio
async def test_should_get_already_not_found_when_getting_one(service):
    service.cache['fake'] = CACHE_ALREADY_NOT_FOUND

    with pytest.raises(EntityNotFoundError) as exc_info:
        await service.get_one('fake')

    assert not service.entity_circuit.called
    assert exc_info.value.args == ('fake',)


@pytest.mark.asyncio
async def test_should_set_cache_entities_not_found_when_getting_many(service):
    fake_entity = {'id': 'fake'}
    service.entity_circuit.side_effect = [
        fake_entity,
        EntityNotFoundError,
        EntityNotFoundError,
    ]
    service.repository.id_name = 'id'

    entities = [e async for e in service.get_many('fake', 'fake2', 'fake3')]

    assert entities == [fake_entity]
    assert service.cache['fake2'] == CACHE_ALREADY_NOT_FOUND
    assert service.cache['fake3'] == CACHE_ALREADY_NOT_FOUND


@pytest.mark.asyncio
async def test_should_get_already_not_found_when_getting_many(service):
    fake_entity = {'id': 'fake'}
    service.cache['fake'] = fake_entity
    service.cache['fake2'] = CACHE_ALREADY_NOT_FOUND
    service.cache['fake3'] = CACHE_ALREADY_NOT_FOUND

    entities = [e async for e in service.get_many('fake', 'fake2', 'fake3')]

    assert not service.entities_circuit.called
    assert entities == [fake_entity]
    assert service.cache['fake2'] == CACHE_ALREADY_NOT_FOUND
    assert service.cache['fake3'] == CACHE_ALREADY_NOT_FOUND
