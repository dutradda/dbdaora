import pytest


@pytest.mark.asyncio
async def test_should_get_one(
    fake_service, fake_entity, repository, fake_fallback_data_entity
):
    await repository.memory_data_source.delete('fake:fake2:fake')
    key = fake_service.repository.fallback_data_source.make_key(
        'fake', 'fake2', 'fake'
    )
    await fake_service.repository.fallback_data_source.put(
        key, fake_fallback_data_entity
    )
    entity = await repository.query(
        fake_id=fake_entity.fake_id,
        fake2_id=fake_entity.fake2_id,
        latitude=5,
        longitude=6,
        max_distance=1,
    ).entity

    assert entity == fake_entity
