import pytest
import dataclasses
from typing import Optional, Dict, Any, TypeVar

import asynctest

from dbdaora.repositories.memory.dict import DictMemoryRepository
from dbdaora.repositories.memory.query.dict import DictQuery
from dbdaora.entity import EntityData, Entity
from dbdaora.exceptions import EntityNotFoundError
from dbdaora.data_sources.memory import MemoryDataSource
from dbdaora.data_sources.fallback import FallbackDataSource



@pytest.fixture
def repository():
    return DictMemoryRepository()


@pytest.mark.asyncio
async def test_should_get_from_memory(repository) -> None:
    expected_entity = 'fake'
    repository.memory_data_source.db[expected_entity] = expected_entity
    assert await repository.query(expected_entity).get() == expected_entity


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository) -> None:
    fake_entity = 'fake'
    expected_query = DictQuery(repository, entity_id=fake_entity)

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity).get()

    assert exc_info.value.args == (expected_query,)


@pytest.mark.asyncio
async def test_should_raise_not_found_error_when_already_raised_before(repository, mocker) -> None:
    fake_entity = 'fake'
    expected_query = DictQuery(repository, entity_id=fake_entity)
    repository.memory_data_source.get = asynctest.CoroutineMock()
    repository.memory_data_source.get.side_effect = [None, '1']
    repository.memory_data_source.set = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(fake_entity).get()

    assert exc_info.value.args == (expected_query,)
    assert repository.memory_data_source.get.call_args_list == [
        mocker.call(fake_entity),
        mocker.call('fake:not-found')
    ]
    assert not repository.memory_data_source.set.called


@pytest.mark.asyncio
async def test_should_get_from_fallback(repository) -> None:
    expected_entity = 'fake'
    repository.memory_data_source.get = asynctest.CoroutineMock(return_value=None)
    repository.fallback_data_source.db[expected_entity] = expected_entity
    entity = await repository.query(expected_entity).get()

    assert repository.memory_data_source.get.called
    assert entity == expected_entity



@pytest.mark.asyncio
async def test_should_set_memory_after_got_fallback(repository, mocker) -> None:
    expected_entity = 'fake'
    repository.memory_data_source.get = asynctest.CoroutineMock(return_value=None)
    repository.memory_data_source.set = asynctest.CoroutineMock()
    repository.fallback_data_source.db[expected_entity] = expected_entity
    entity = await repository.query(expected_entity).get()

    assert repository.memory_data_source.get.called
    assert repository.memory_data_source.set.call_args_list == [
        mocker.call(expected_entity, expected_entity)
    ]
    assert entity == expected_entity

