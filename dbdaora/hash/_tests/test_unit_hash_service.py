import asynctest
import pytest

from dbdaora import EntityNotFoundError, HashService
from dbdaora.hash.service import CACHE_ALREADY_NOT_FOUND


@pytest.fixture
def service():
    s = HashService(
        repository=asynctest.MagicMock(),
        circuit_breaker=asynctest.MagicMock(),
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
    assert service.cache['fake'] == error


@pytest.mark.asyncio
async def test_should_get_already_not_found_when_getting_one(service):
    error = EntityNotFoundError()
    service.cache['fake'] = error

    with pytest.raises(EntityNotFoundError) as exc_info:
        await service.get_one('fake')

    assert not service.entity_circuit.called
    assert exc_info.value == error


@pytest.mark.asyncio
async def test_should_set_cache_entities_not_found_when_getting_many(service):
    fake_entity = {'id': 'fake'}
    service.entities_circuit.return_value = [fake_entity]
    service.repository.id_name = 'id'

    entities = await service.get_many('fake', 'fake2', 'fake3')

    assert entities == [fake_entity]
    assert service.cache['fake2'] == CACHE_ALREADY_NOT_FOUND
    assert service.cache['fake3'] == CACHE_ALREADY_NOT_FOUND


@pytest.mark.asyncio
async def test_should_get_already_not_found_when_getting_many(service):
    fake_entity = {'id': 'fake'}
    service.cache['fake'] = fake_entity
    service.cache['fake2'] = CACHE_ALREADY_NOT_FOUND
    service.cache['fake3'] = CACHE_ALREADY_NOT_FOUND

    entities = await service.get_many('fake', 'fake2', 'fake3')

    assert not service.entities_circuit.called
    assert entities == [fake_entity]
    assert service.cache['fake2'] == CACHE_ALREADY_NOT_FOUND
    assert service.cache['fake3'] == CACHE_ALREADY_NOT_FOUND
