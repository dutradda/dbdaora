import itertools

import pytest


@pytest.fixture(autouse=True)
async def set_values(fake_service, fake_entity_withscores):
    values = list(itertools.chain(*fake_entity_withscores.values))
    values.reverse()
    await fake_service.repository.memory_data_source.zadd('fake:fake', *values)


@pytest.mark.asyncio
async def test_should_get_one(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(fake_entity_withscores.id)

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_entity_withscores.id, reverse=True,
    )

    fake_entity.values.reverse()
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_withscores(fake_service, fake_entity_withscores):
    entity = await fake_service.get_one(
        fake_entity_withscores.id, withscores=True
    )

    assert entity == fake_entity_withscores


@pytest.mark.asyncio
async def test_should_get_one_withmaxsize(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_entity_withscores.id, withmaxsize=True
    )

    fake_entity.max_size = 2
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_withmaxsize_and_withscore(
    fake_service, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_entity_withscores.id, withmaxsize=True, withscores=True
    )

    fake_entity_withscores.max_size = 2
    assert entity == fake_entity_withscores
