from typing import Any, ClassVar, Dict, Optional, Sequence, Type, Union

from dbdaora.data_sources.memory import GeoMember, GeoRadiusOutput
from dbdaora.exceptions import (
    EntityNotFoundError,
    InvalidGeoSpatialDataError,
    InvalidQueryError,
)
from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query
from dbdaora.repository import MemoryRepository

from ..entity import GeoSpatialEntity


GeoSpatialData = GeoRadiusOutput


class GeoSpatialRepository(MemoryRepository[Any, GeoSpatialData, FallbackKey]):
    __skip_cls_validation__ = ('GeoSpatialRepository',)
    entity_cls: ClassVar[Type[Any]] = GeoSpatialEntity
    key_attrs: ClassVar[Sequence[str]] = ('id',)

    async def get_memory_data(  # type: ignore
        self, key: str, query: 'GeoSpatialQuery[FallbackKey]',
    ) -> Optional[GeoSpatialData]:
        self._validate_query(query)

        data = await self.memory_data_source.georadius(
            key=key,
            longitude=query.longitude,  # type: ignore
            latitude=query.latitude,  # type: ignore
            radius=query.max_distance,  # type: ignore
            unit=query.distance_unit,
            with_dist=query.with_dist,
            with_coord=query.with_coord,
        )

        if not data:
            return None

        return data

    async def get_fallback_data(  # type: ignore
        self,
        query: 'GeoSpatialQuery[FallbackKey]',
        *,
        for_memory: bool = False,
    ) -> Optional[GeoSpatialData]:
        key = self.fallback_key(query)
        data = await self.fallback_data_source.get(key)

        if data is None:
            return None

        return self.make_fallback_data_for_memory(key, query, data)

    def _validate_query(self, query: 'GeoSpatialQuery[FallbackKey]') -> None:
        if query.type == GeoSpatialQueryType.RADIUS:
            if (
                query.latitude is None
                or query.longitude is None
                or query.max_distance is None
            ):
                raise InvalidQueryError(query)

            return

        raise InvalidQueryError(query)

    def make_fallback_data_for_memory(
        self,
        key: FallbackKey,
        query: 'GeoSpatialQuery[FallbackKey]',
        data: Dict[str, Any],
    ) -> GeoSpatialData:
        return [
            self.memory_data_source.geomember_cls(
                member=member['member'],
                hash=None,
                dist=None,
                coord=self.memory_data_source.geopoint_cls(
                    latitude=member['latitude'], longitude=member['longitude'],
                ),
            )
            for member in data['data']
        ]

    def make_entity(  # type: ignore
        self,
        data: GeoSpatialData,
        query: 'Query[GeoSpatialEntity, GeoSpatialData, FallbackKey]',
    ) -> Any:
        return self.entity_cls(
            data=data,
            **{
                id_name: id_value
                for id_name, id_value in zip(self.key_attrs, query.key_parts)
            },
        )

    def make_entity_from_fallback(  # type: ignore
        self,
        data: GeoSpatialData,
        query: 'Query[GeoSpatialEntity, GeoSpatialData, FallbackKey]',
    ) -> Any:
        return self.make_entity(data, query)

    async def add_memory_data(
        self, key: str, data: GeoSpatialData, from_fallback: bool = False
    ) -> None:
        for i, geomember in enumerate(data):
            if (
                isinstance(geomember, self.memory_data_source.geomember_cls)
                and geomember.coord is not None
            ):
                await self.memory_data_source.geoadd(
                    key,
                    longitude=geomember.coord.longitude,
                    latitude=geomember.coord.latitude,
                    member=geomember.member,
                )
            else:
                raise InvalidGeoSpatialDataError(i, geomember)

    async def add_memory_data_from_fallback(
        self,
        key: str,
        query: Union[
            BaseQuery[GeoSpatialEntity, GeoSpatialData, FallbackKey],
            GeoSpatialEntity,
        ],
        data: GeoSpatialData,
    ) -> GeoSpatialData:
        geomembers = self.make_memory_data_from_fallback(query, data)

        for i, geomember in enumerate(geomembers):
            if (
                isinstance(geomember, self.memory_data_source.geomember_cls)
                and geomember.coord is not None
            ):
                await self.memory_data_source.geoadd(
                    key,
                    longitude=geomember.coord.longitude,
                    latitude=geomember.coord.latitude,
                    member=geomember.member,
                )
            else:
                raise InvalidGeoSpatialDataError(i, geomember)

        if isinstance(query, GeoSpatialQuery):
            memory_data = await self.get_memory_data(key, query)
        else:
            memory_data = data

        if memory_data is None:
            raise EntityNotFoundError(query)

        return memory_data

    def make_memory_data_from_fallback(
        self,
        query: Union[
            BaseQuery[GeoSpatialEntity, GeoSpatialData, FallbackKey],
            GeoSpatialEntity,
        ],
        data: GeoSpatialData,
    ) -> Sequence[GeoMember]:
        return data  # type: ignore

    def make_memory_data_from_entity(
        self, entity: GeoSpatialEntity
    ) -> GeoSpatialData:
        return entity.data

    async def add_fallback(
        self,
        entity: GeoSpatialEntity,
        *entities: GeoSpatialEntity,
        **kwargs: Any,
    ) -> None:
        data = {
            'data': [
                {
                    'latitude': m.coord.latitude,
                    'longitude': m.coord.longitude,
                    'member': m.member,
                }
                for m in entity.data
                if (
                    isinstance(m, self.memory_data_source.geomember_cls)
                    and m.coord is not None
                )
            ],
        }
        await self.fallback_data_source.put(
            self.fallback_key(entity), data, **kwargs
        )

    def make_query(
        self, *args: Any, **kwargs: Any
    ) -> 'BaseQuery[GeoSpatialEntity, GeoSpatialData, FallbackKey]':
        return query_factory(self, *args, **kwargs)

    async def already_got_not_found(
        self,
        query: Union[
            'Query[GeoSpatialEntity, GeoSpatialData, FallbackKey]',
            GeoSpatialEntity,
        ],
    ) -> bool:
        return bool(
            await self.memory_data_source.exists(self.memory_key(query))
        )

    async def delete_fallback_not_found(
        self,
        query: Union[
            'Query[GeoSpatialEntity, GeoSpatialData, FallbackKey]',
            GeoSpatialEntity,
        ],
    ) -> None:
        ...

    async def set_fallback_not_found(
        self,
        query: Union[
            'Query[GeoSpatialEntity, GeoSpatialData, FallbackKey]',
            GeoSpatialEntity,
        ],
    ) -> None:
        ...


from ..query import (  # noqa isort:skip
    GeoSpatialQuery,
    GeoSpatialQueryMany,
    GeoSpatialQueryType,
)
from ..query import make as query_factory  # noqa isort:skip
