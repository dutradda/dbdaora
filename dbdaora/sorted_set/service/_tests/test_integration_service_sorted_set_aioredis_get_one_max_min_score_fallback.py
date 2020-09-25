import itertools

import pytest

from dbdaora import EntityNotFoundError


@pytest.fixture(autouse=True)
async def set_fallback_data(fake_service, fake_entity_withscores):
    data = list(itertools.chain(*fake_entity_withscores.data))
    fake_service.repository.fallback_data_source.db['fake:fake'] = {
        'id': 'fake',
        'data': data,
    }


@pytest.mark.asyncio
async def test_should_get_one_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, max_score=0, memory=False,
    )

    fake_entity.data = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=1, memory=False,
    )

    fake_entity.data = [b'2']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score_and_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=0,
        memory=False,
    )

    fake_entity.data = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score_and_max_score_all_data(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=1,
        memory=False,
    )

    fake_entity.data = [b'1', b'2']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score_and_max_score_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            memory=False,
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
            memory=False,
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
            memory=False,
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
            memory=False,
        )


@pytest.mark.asyncio
async def test_should_get_one_reverse_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        max_score=0,
        reverse=True,
        memory=False,
    )

    fake_entity.data = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=1,
        reverse=True,
        memory=False,
    )

    fake_entity.data = [b'2']
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
        memory=False,
    )

    fake_entity.data = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_and_max_score_all_data(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=1,
        reverse=True,
        memory=False,
    )

    fake_entity.data.reverse()
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_max_score_and_withscores_all_data(
    fake_service, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=1,
        reverse=True,
        withscores=True,
        memory=False,
    )

    fake_entity_withscores.data.reverse()
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
        memory=False,
    )

    fake_entity.data = [fake_entity.data[-1]]
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
        memory=False,
    )

    fake_entity_withscores.data = [fake_entity_withscores.data[-1]]
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
            memory=False,
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
            memory=False,
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
            memory=False,
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
            memory=False,
        )
