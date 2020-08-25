import pytest

from dbdaora import EntityNotFoundError, HashService
from dbdaora.service import CACHE_ALREADY_NOT_FOUND


@pytest.fixture
def service(mocker):
    s = HashService(
        repository=mocker.MagicMock(id_name='id'),
        circuit_breaker=mocker.MagicMock(),
        fallback_circuit_breaker=mocker.MagicMock(),
        cache={},
    )
    s.entity_circuit = mocker.AsyncMock()
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
async def test_should_set_cache_entities_not_found_when_getting_many(
    service, async_iterator
):
    fake_entity = {'id': 'fake'}
    service.repository.query.return_value.entities = async_iterator(
        [fake_entity]
    )
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

    assert not service.repository.query.called
    assert entities == [fake_entity]
    assert service.cache['fake2'] == CACHE_ALREADY_NOT_FOUND
    assert service.cache['fake3'] == CACHE_ALREADY_NOT_FOUND
