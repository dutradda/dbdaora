import itertools

import asynctest
import pytest

from dbdaora import GeoSpatialQuery
from dbdaora.exceptions import EntityNotFoundError


@pytest.mark.asyncio
async def test_should_get_from_memory(
    repository, serialized_fake_entity, fake_entity
):
    await repository.memory_data_source.geoadd(
        'fake:fake2:fake', *itertools.chain(*serialized_fake_entity)
    )
    entity = await repository.query(
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=5,
        longitude=6,
        max_distance=1,
    ).entity

    assert entity == fake_entity


@pytest.mark.asyncio
async def test_should_raise_not_found_error(repository, fake_entity, mocker):
    fake_query = GeoSpatialQuery(
        repository,
        memory=True,
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=1,
        longitude=1,
        max_distance=1,
    )

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(
            fake_id=fake_entity.fake_id,
            fake2_id=fake_entity.fake2_id,
            latitude=1,
            longitude=1,
            max_distance=1,
        ).entity

    assert exc_info.value.args == (fake_query,)


@pytest.mark.asyncio
async def test_should_raise_not_found_error_when_already_raised_before(
    repository, mocker, fake_entity
):
    expected_query = GeoSpatialQuery(
        repository,
        memory=True,
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=1,
        longitude=1,
        max_distance=1,
    )
    repository.memory_data_source.georadius = asynctest.CoroutineMock(
        side_effect=[[]]
    )
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[True]
    )
    repository.memory_data_source.geoadd = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(
            fake_id=fake_entity.fake_id,
            fake2_id=fake_entity.fake2_id,
            latitude=1,
            longitude=1,
            max_distance=1,
        ).entity

    assert exc_info.value.args == (expected_query,)
    assert repository.memory_data_source.georadius.call_args_list == [
        mocker.call(
            key='fake:fake2:fake',
            longitude=1,
            latitude=1,
            radius=1,
            unit='km',
            with_dist=True,
            with_coord=True,
            count=None,
        ),
    ]
    assert repository.memory_data_source.exists.call_args_list == [
        mocker.call('fake:fake2:fake')
    ]
    assert not repository.memory_data_source.geoadd.called


@pytest.mark.asyncio
async def test_should_set_already_not_found_error(
    repository, mocker, fake_entity
):
    expected_query = GeoSpatialQuery(
        repository,
        memory=True,
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=1,
        longitude=1,
        max_distance=1,
    )
    repository.memory_data_source.georadius = asynctest.CoroutineMock(
        side_effect=[[]]
    )
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[False]
    )
    repository.fallback_data_source.query = asynctest.CoroutineMock(
        return_value=[]
    )
    repository.memory_data_source.geoadd = asynctest.CoroutineMock()

    with pytest.raises(EntityNotFoundError) as exc_info:
        await repository.query(
            fake_id=fake_entity.fake_id,
            fake2_id=fake_entity.fake2_id,
            latitude=1,
            longitude=1,
            max_distance=1,
        ).entity

    assert exc_info.value.args == (expected_query,)
    assert repository.memory_data_source.georadius.call_args_list == [
        mocker.call(
            key='fake:fake2:fake',
            longitude=1,
            latitude=1,
            radius=1,
            unit='km',
            with_dist=True,
            with_coord=True,
            count=None,
        ),
    ]
    assert repository.memory_data_source.exists.call_args_list == [
        mocker.call('fake:fake2:fake')
    ]
    assert repository.fallback_data_source.query.call_args_list == [
        mocker.call('fake:fake2:fake')
    ]
    assert not repository.memory_data_source.geoadd.called


@pytest.mark.asyncio
async def test_should_get_from_fallback(
    repository,
    fake_entity,
    fake_fallback_data_entity,
    fake_fallback_data_entity2,
):
    await repository.memory_data_source.delete('fake:fake2:fake')
    repository.fallback_data_source.db[
        'fake:fake2:m1'
    ] = fake_fallback_data_entity
    repository.fallback_data_source.db[
        'fake:fake2:m2'
    ] = fake_fallback_data_entity2
    entity = await repository.query(
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=5,
        longitude=6,
        max_distance=1,
    ).entity

    assert entity == fake_entity
    assert repository.memory_data_source.exists('fake:fake2:fake')


@pytest.mark.asyncio
async def test_should_set_memory_after_got_fallback(
    repository,
    fake_entity,
    mocker,
    fake_fallback_data_entity,
    fake_fallback_data_entity2,
):
    repository.memory_data_source.georadius = asynctest.CoroutineMock(
        side_effect=[[], fake_entity.data]
    )
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        side_effect=[False]
    )
    repository.fallback_data_source.db[
        'fake:fake2:m1'
    ] = fake_fallback_data_entity
    repository.fallback_data_source.db[
        'fake:fake2:m2'
    ] = fake_fallback_data_entity2
    repository.memory_data_source.geoadd = asynctest.CoroutineMock()
    entity = await repository.query(
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=5,
        longitude=6,
        max_distance=1,
    ).entity

    assert repository.memory_data_source.georadius.called
    assert repository.memory_data_source.exists.called
    assert repository.memory_data_source.geoadd.call_args_list == [
        mocker.call(
            'fake:fake2:fake',
            longitude=6.000002324581146,
            latitude=4.999999830436074,
            member=b'm1',
        ),
        mocker.call(
            'fake:fake2:fake',
            longitude=6.000002324581146,
            latitude=4.999999830436074,
            member=b'm2',
        ),
    ]
    assert entity == fake_entity
