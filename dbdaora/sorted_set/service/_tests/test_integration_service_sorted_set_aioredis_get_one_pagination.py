import itertools

import pytest

from dbdaora import EntityNotFoundError


@pytest.fixture(autouse=True)
async def set_values(fake_service, fake_entity_withscores):
    values = list(itertools.chain(*fake_entity_withscores.values))
    values.reverse()
    await fake_service.repository.memory_data_source.zadd('fake:fake', *values)


@pytest.mark.asyncio
async def test_should_get_one_page_size(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, page_size=1,
    )

    fake_entity.values = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_page_size_and_page(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, page_size=1, page=2,
    )

    fake_entity.values = [b'2']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_page_size_and_page_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id, page_size=1, page=3,
        )


@pytest.mark.asyncio
async def test_should_get_one_page_size_page_and_withscores_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            page_size=1,
            page=3,
            withscores=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_page_size_page_and_withmaxsize_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            page_size=1,
            page=3,
            withmaxsize=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_page_size_page_withmaxsize_and_withscores_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            page_size=1,
            page=3,
            withmaxsize=True,
            withscores=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_reverse_and_page_size(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, reverse=True, page_size=1,
    )

    fake_entity.values = [b'2']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_page_size_and_page(
    fake_service, fake_entity, fake_entity_withscores
):
    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        reverse=True,
        page_size=1,
        page=2,
    )

    fake_entity.values = [b'1']
    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_page_size_and_page_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            reverse=True,
            page_size=1,
            page=3,
        )


@pytest.mark.asyncio
async def test_should_get_one_reverse_page_size_page_and_withscores_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            reverse=True,
            page_size=1,
            page=3,
            withscores=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_reverse_page_size_page_and_withmaxsize_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            reverse=True,
            page_size=1,
            page=3,
            withmaxsize=True,
        )


@pytest.mark.asyncio
async def test_should_get_one_reverse_page_size_page_withmaxsize_and_withscores_not_found(
    fake_service, fake_entity_withscores
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            reverse=True,
            page_size=1,
            page=3,
            withmaxsize=True,
            withscores=True,
        )
