import asynctest
import pytest


@pytest.mark.asyncio
async def test_should_add_memory(
    repository, fake_entity, fake_entity_withscores, mocker
):
    repository.memory_data_source.exists = asynctest.CoroutineMock(
        return_value=True
    )
    repository.memory_data_source.zadd = asynctest.CoroutineMock()
    await repository.add(fake_entity_withscores)

    assert repository.memory_data_source.zadd.call_args_list == [
        mocker.call('fake:fake', 1, b'2', 0, b'1')
    ]
