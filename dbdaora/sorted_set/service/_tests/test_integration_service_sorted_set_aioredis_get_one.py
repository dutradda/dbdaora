import itertools

import pytest


@pytest.fixture(autouse=True)
async def set_data(fake_service, fake_entity_withscores):
    data = list(itertools.chain(*fake_entity_withscores.data))
    data.reverse()
    await fake_service.repository.memory_data_source.zadd('fake:fake', *data)


@pytest.mark.asyncio
async def test_should_get_one(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(fake_id=fake_entity_withscores.fake_id)

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, reverse=True,
    )

    fake_entity.data.reverse()
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_withscores(fake_service, fake_entity_withscores):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, withscores=True
    )

    assert entity == fake_entity_withscores


@pytest.mark.asyncio
async def test_should_get_one_withmaxsize(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, withmaxsize=True
    )

    fake_entity.max_size = 2
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_withmaxsize_and_withscore(
    fake_service, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        withmaxsize=True,
        withscores=True,
    )

    fake_entity_withscores.max_size = 2
    assert entity == fake_entity_withscores
