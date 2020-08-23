import itertools

import pytest


@pytest.mark.asyncio
async def test_should_get_one(
    fake_service, fake_entity, fake_entity_withscores
):
    values = list(itertools.chain(*fake_entity_withscores.values))
    values.reverse()
    await fake_service.repository.memory_data_source.zadd('fake:fake', *values)
    entity = await fake_service.get_one(fake_entity_withscores.id)

    assert entity == fake_entity
