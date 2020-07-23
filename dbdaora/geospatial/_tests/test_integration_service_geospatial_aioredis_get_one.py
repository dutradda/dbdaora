import itertools

import pytest


@pytest.mark.asyncio
async def test_should_get_one(
    fake_service, serialized_fake_entity, fake_entity, repository
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
