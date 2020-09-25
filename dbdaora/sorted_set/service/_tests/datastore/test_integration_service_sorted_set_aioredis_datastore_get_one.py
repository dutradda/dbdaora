import itertools

import pytest


@pytest.mark.asyncio
async def test_should_get_one(
    fake_service, fake_entity, fake_entity_withscores
):
    data = list(itertools.chain(*fake_entity_withscores.data))
    data.reverse()
    await fake_service.repository.memory_data_source.zadd('fake:fake', *data)
    entity = await fake_service.get_one(fake_id=fake_entity_withscores.fake_id)

    assert entity == fake_entity
