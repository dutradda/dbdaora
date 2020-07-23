import dataclasses
from functools import partial
from typing import List

import pytest
from aioredis import GeoMember, GeoPoint, RedisError

from dbdaora import (
    DictFallbackDataSource,
    GeoSpatialRepository,
    GeoSpatialService,
    build_service,
    make_aioredis_data_source,
)


@pytest.mark.asyncio
@pytest.fixture
async def fake_service(mocker, fallback_data_source, fake_repository_cls):
    memory_data_source_factory = partial(
        make_aioredis_data_source,
        'redis://',
        'redis://localhost/1',
        'redis://localhost/2',
    )

    async def fallback_data_source_factory():
        return fallback_data_source

    service = await build_service(
        GeoSpatialService,
        fake_repository_cls,
        memory_data_source_factory,
        fallback_data_source_factory,
        repository_expire_time=1,
        cb_failure_threshold=0,
        cb_recovery_timeout=10,
        cb_expected_exception=RedisError,
        logger=mocker.MagicMock(),
    )

    yield service

    service.repository.memory_data_source.close()
    await service.repository.memory_data_source.wait_closed()


@pytest.fixture
def fallback_data_source():
    return DictFallbackDataSource()


@dataclasses.dataclass
class FakeEntity:
    fake_id: str
    fake2_id: str
    data: List[GeoMember]


class FakeGeoSpatialRepository(GeoSpatialRepository[str]):
    name = 'fake'
    key_attrs = ('fake2_id', 'fake_id')
    entity_cls = FakeEntity


@pytest.fixture
def fake_entity_cls():
    return FakeEntity


@pytest.fixture
def fake_repository_cls():
    return FakeGeoSpatialRepository


@pytest.fixture
def fake_entity(fake_entity_cls):
    return fake_entity_cls(
        fake_id='fake',
        fake2_id='fake2',
        data=[
            GeoMember(
                member=b'm1',
                dist=0.0003,
                hash=None,
                coord=GeoPoint(6.000002324581146, 4.999999830436074),
            ),
            GeoMember(
                member=b'm2',
                dist=0.0003,
                hash=None,
                coord=GeoPoint(6.000002324581146, 4.999999830436074),
            ),
        ],
    )


@pytest.fixture
def fake_entity_add(fake_entity_cls):
    return fake_entity_cls(
        fake_id='fake',
        fake2_id='fake2',
        data=GeoMember(
            member=b'm1',
            dist=0.0003,
            hash=None,
            coord=GeoPoint(6.000002324581146, 4.999999830436074),
        ),
    )


@pytest.fixture
def fake_entity_add2(fake_entity_cls):
    return fake_entity_cls(
        fake_id='fake',
        fake2_id='fake2',
        data=GeoMember(
            member=b'm2',
            dist=0.0003,
            hash=None,
            coord=GeoPoint(6.000002324581146, 4.999999830436074),
        ),
    )


@pytest.fixture
def serialized_fake_entity():
    return [
        (6.000002324581146, 4.999999830436074, 'm1'),
        (6.000002324581146, 4.999999830436074, 'm2'),
    ]


@pytest.fixture
def fake_fallback_data_entity(fake_entity):
    return {
        'latitude': fake_entity.data[0].coord.latitude,
        'longitude': fake_entity.data[0].coord.longitude,
        'member': fake_entity.data[0].member,
    }


@pytest.fixture
def fake_fallback_data_entity2(fake_entity):
    return {
        'latitude': fake_entity.data[1].coord.latitude,
        'longitude': fake_entity.data[1].coord.longitude,
        'member': fake_entity.data[1].member,
    }


@pytest.mark.asyncio
@pytest.fixture
async def memory_data_source():
    return await make_aioredis_data_source(
        'redis://', 'redis://localhost/1', 'redis://localhost/2'
    )


@pytest.mark.asyncio
@pytest.fixture
async def repository(
    mocker, memory_data_source, fallback_data_source, fake_repository_cls
):
    yield fake_repository_cls(
        memory_data_source=memory_data_source,
        fallback_data_source=fallback_data_source,
        expire_time=1,
    )
    memory_data_source.close()
    await memory_data_source.wait_closed()
