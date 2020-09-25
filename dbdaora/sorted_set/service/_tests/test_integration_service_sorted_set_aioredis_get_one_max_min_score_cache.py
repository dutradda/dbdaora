import itertools

import pytest

from dbdaora import CacheType, EntityNotFoundError
from dbdaora.service import CACHE_ALREADY_NOT_FOUND


@pytest.fixture
def cache_config():
    return {
        'cache_type': CacheType.TTL,
        'cache_ttl': 60,
        'cache_max_size': 10,
    }


@pytest.fixture(autouse=True)
async def set_data(fake_service, fake_entity_withscores):
    data = list(itertools.chain(*fake_entity_withscores.data))
    data.reverse()
    await fake_service.repository.memory_data_source.zadd('fake:fake', *data)


@pytest.mark.asyncio
async def test_should_get_one_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, max_score=0,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, max_score=0,
    )

    fake_entity.data = [b'1']
    cached_entity = fake_service.cache.get('fakemax_score0')
    assert entity == fake_entity == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score(
    fake_service, fake_entity, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=1,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=1,
    )

    fake_entity.data = [b'2']
    cached_entity = fake_service.cache.get('fakemin_score1')
    assert entity == fake_entity == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score_and_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=0, max_score=0,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=0, max_score=0,
    )

    fake_entity.data = [b'1']
    cached_entity = fake_service.cache.get('fakemin_score0max_score0')
    assert entity == fake_entity == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score_and_max_score_all_data(
    fake_service, fake_entity, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=0, max_score=1,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=0, max_score=1,
    )

    fake_entity.data = [b'1', b'2']
    cached_entity = fake_service.cache.get('fakemin_score0max_score1')
    assert entity == fake_entity == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_min_score_and_max_score_not_found(
    fake_service, fake_entity_withscores, mocker
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id, min_score=2, max_score=3,
        )

    assert (
        fake_service.cache.get('fakemin_score2max_score3')
        == CACHE_ALREADY_NOT_FOUND
    )

    fake_service.cache.get = mocker.MagicMock(
        return_value=CACHE_ALREADY_NOT_FOUND
    )

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id, min_score=2, max_score=3,
        )

    assert fake_service.cache.get.call_args_list == [
        mocker.call('fakemin_score2max_score3')
    ]


@pytest.mark.asyncio
async def test_should_get_one_min_score_max_score_and_withscores_not_found(
    fake_service, fake_entity_withscores, mocker
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            withscores=True,
        )

    assert (
        fake_service.cache.get('fakemin_score2max_score3withscoresTrue')
        == CACHE_ALREADY_NOT_FOUND
    )

    fake_service.cache.get = mocker.MagicMock(
        return_value=CACHE_ALREADY_NOT_FOUND
    )

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            withscores=True,
        )

    assert fake_service.cache.get.call_args_list == [
        mocker.call('fakemin_score2max_score3withscoresTrue')
    ]


@pytest.mark.asyncio
async def test_should_get_one_min_score_max_score_and_withmaxsize_not_found(
    fake_service, fake_entity_withscores, mocker
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            withmaxsize=True,
        )

    assert (
        fake_service.cache.get('fakemin_score2max_score3withmaxsizeTrue')
        == CACHE_ALREADY_NOT_FOUND
    )

    fake_service.cache.get = mocker.MagicMock(
        return_value=CACHE_ALREADY_NOT_FOUND
    )

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            withmaxsize=True,
        )

    assert fake_service.cache.get.call_args_list == [
        mocker.call('fakemin_score2max_score3withmaxsizeTrue')
    ]


@pytest.mark.asyncio
async def test_should_get_one_min_score_max_score_withmaxsize_and_withscores_not_found(
    fake_service, fake_entity_withscores, mocker
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            withmaxsize=True,
            withscores=True,
        )

    assert (
        fake_service.cache.get(
            'fakemin_score2max_score3withmaxsizeTruewithscoresTrue'
        )
        == CACHE_ALREADY_NOT_FOUND
    )

    fake_service.cache.get = mocker.MagicMock(
        return_value=CACHE_ALREADY_NOT_FOUND
    )

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            withmaxsize=True,
            withscores=True,
        )

    assert fake_service.cache.get.call_args_list == [
        mocker.call('fakemin_score2max_score3withmaxsizeTruewithscoresTrue')
    ]


@pytest.mark.asyncio
async def test_should_get_one_reverse_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, max_score=0, reverse=True,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, max_score=0, reverse=True,
    )

    fake_entity.data = [b'1']
    cached_entity = fake_service.cache.get('fakemax_score0reverseTrue')
    assert entity == fake_entity == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score(
    fake_service, fake_entity, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=1, reverse=True,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id, min_score=1, reverse=True,
    )

    fake_entity.data = [b'2']
    cached_entity = fake_service.cache.get('fakemin_score1reverseTrue')
    assert entity == fake_entity == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_and_max_score(
    fake_service, fake_entity, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=0,
        reverse=True,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=0,
        reverse=True,
    )

    fake_entity.data = [b'1']
    cached_entity = fake_service.cache.get(
        'fakemin_score0max_score0reverseTrue'
    )
    assert entity == fake_entity == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_and_max_score_all_data(
    fake_service, fake_entity, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=1,
        reverse=True,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=1,
        reverse=True,
    )

    fake_entity.data.reverse()
    cached_entity = fake_service.cache.get(
        'fakemin_score0max_score1reverseTrue'
    )
    assert entity == fake_entity == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_max_score_and_withscores_all_data(
    fake_service, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=1,
        reverse=True,
        withscores=True,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=0,
        max_score=1,
        reverse=True,
        withscores=True,
    )

    fake_entity_withscores.data.reverse()
    cached_entity = fake_service.cache.get(
        'fakemin_score0max_score1reverseTruewithscoresTrue'
    )
    assert entity == fake_entity_withscores == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_and_withmaxsize(
    fake_service, fake_entity, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=1,
        reverse=True,
        withmaxsize=True,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=1,
        reverse=True,
        withmaxsize=True,
    )

    fake_entity.data = [fake_entity.data[-1]]
    fake_entity.max_size = 2
    cached_entity = fake_service.cache.get(
        'fakemin_score1reverseTruewithmaxsizeTrue'
    )
    assert entity == fake_entity == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_withmaxsize_and_withscores(
    fake_service, fake_entity_withscores
):
    await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=1,
        reverse=True,
        withscores=True,
        withmaxsize=True,
    )
    await fake_service.repository.memory_data_source.delete('fake:fake')

    entity = await fake_service.get_one(
        fake_id=fake_entity_withscores.fake_id,
        min_score=1,
        reverse=True,
        withscores=True,
        withmaxsize=True,
    )

    fake_entity_withscores.data = [fake_entity_withscores.data[-1]]
    fake_entity_withscores.max_size = 2
    cached_entity = fake_service.cache.get(
        'fakemin_score1reverseTruewithscoresTruewithmaxsizeTrue'
    )
    assert entity == fake_entity_withscores == cached_entity


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_and_max_score_not_found(
    fake_service, fake_entity_withscores, mocker
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
        )

    assert (
        fake_service.cache.get('fakemin_score2max_score3reverseTrue')
        == CACHE_ALREADY_NOT_FOUND
    )

    fake_service.cache.get = mocker.MagicMock(
        return_value=CACHE_ALREADY_NOT_FOUND
    )

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
        )

    assert fake_service.cache.get.call_args_list == [
        mocker.call('fakemin_score2max_score3reverseTrue')
    ]


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_max_score_and_withscores_not_found(
    fake_service, fake_entity_withscores, mocker
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
            withscores=True,
        )

    assert (
        fake_service.cache.get(
            'fakemin_score2max_score3reverseTruewithscoresTrue'
        )
        == CACHE_ALREADY_NOT_FOUND
    )

    fake_service.cache.get = mocker.MagicMock(
        return_value=CACHE_ALREADY_NOT_FOUND
    )

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
            withscores=True,
        )

    assert fake_service.cache.get.call_args_list == [
        mocker.call('fakemin_score2max_score3reverseTruewithscoresTrue')
    ]


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_max_score_and_withmaxsize_not_found(
    fake_service, fake_entity_withscores, mocker
):
    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
            withmaxsize=True,
        )

    assert (
        fake_service.cache.get(
            'fakemin_score2max_score3reverseTruewithmaxsizeTrue'
        )
        == CACHE_ALREADY_NOT_FOUND
    )

    fake_service.cache.get = mocker.MagicMock(
        return_value=CACHE_ALREADY_NOT_FOUND
    )

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
            withmaxsize=True,
        )

    assert fake_service.cache.get.call_args_list == [
        mocker.call('fakemin_score2max_score3reverseTruewithmaxsizeTrue')
    ]


@pytest.mark.asyncio
async def test_should_get_one_reverse_min_score_max_score_withmaxsize_and_withscores_not_found(
    fake_service, fake_entity_withscores, mocker
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

    assert (
        fake_service.cache.get(
            'fakemin_score2max_score3reverseTruewithmaxsizeTruewithscoresTrue'
        )
        == CACHE_ALREADY_NOT_FOUND
    )

    fake_service.cache.get = mocker.MagicMock(
        return_value=CACHE_ALREADY_NOT_FOUND
    )

    with pytest.raises(EntityNotFoundError):
        await fake_service.get_one(
            fake_id=fake_entity_withscores.fake_id,
            min_score=2,
            max_score=3,
            reverse=True,
            withmaxsize=True,
            withscores=True,
        )

    assert fake_service.cache.get.call_args_list == [
        mocker.call(
            'fakemin_score2max_score3reverseTruewithmaxsizeTruewithscoresTrue'
        )
    ]
