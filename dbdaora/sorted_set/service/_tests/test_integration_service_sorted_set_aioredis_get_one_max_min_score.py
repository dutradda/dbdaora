import itertools

import pytest

from dbdaora import EntityNotFoundError


@pytest.fixture(autouse=True)
async def set_values(fake_service, fake_entity_withscores):
    values = list(itertools.chain(*fake_entity_withscores.values))
    values.reverse()
    await fake_service.repository.memory_data_source.zadd('fake:fake', *values)


@pytest.mark.asyncio
async def test_should_get_one_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, max_score=0,
    )

    fake_entity.values = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=1,
    )

    fake_entity.values = [b'2']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score_and_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=0, max_score=0,
    )

    fake_entity.values = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score_and_max_score_all_values(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=0, max_score=1,
    )

    fake_entity.values = [b'1', b'2']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score_and_max_score_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id, min_score=2, max_score=3,
        )


@pytest.mark.asyncio
async def test_should_get_one_min_score_max_score_and_withscores_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            withscores=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_min_score_max_score_and_withmaxsize_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            withmaxsize=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_min_score_max_score_withmaxsize_and_withscores_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            withmaxsize=True,
            withscores=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_reverse_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, max_score=0, reverse=True,
    )

    fake_entity.values = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=1, reverse=True,
    )

    fake_entity.values = [b'2']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_and_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=0,
        reverse=True,
    )

    fake_entity.values = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_and_max_score_all_values(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=1,
        reverse=True,
    )

    fake_entity.values.reverse()
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_max_score_and_withscores_all_values(
    fake_service, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=1,
        reverse=True,
        withscores=True,
    )

    fake_entity_withscores.values.reverse()
    assert entity == fake_entity_withscores


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_and_withmaxsize(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=1,
        reverse=True,
        withmaxsize=True,
    )

    fake_entity.values = [fake_entity.values[-1]]
    fake_entity.max_size = 2
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_withmaxsize_and_withscores(
    fake_service, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=1,
        reverse=True,
        withscores=True,
        withmaxsize=True,
    )

    fake_entity_withscores.values = [fake_entity_withscores.values[-1]]
    fake_entity_withscores.max_size = 2
    assert entity == fake_entity_withscores


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_and_max_score_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_max_score_and_withscores_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
            withscores=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_max_score_and_withmaxsize_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
            withmaxsize=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_max_score_withmaxsize_and_withscores_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
            withmaxsize=True,
            withscores=True,
        )
