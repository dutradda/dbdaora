from typing import (  # type: ignore
    Any,
    ClassVar,
    Dict,
    Optional,
    Sequence,
    Type,
    Union,
    _TypedDictMeta,
)

from dbdaora.data_sources.memory import GeoMember
from dbdaora.exceptions import (
    EntityNotFoundError,
    InvalidGeoSpatialDataError,
    InvalidQueryError,
)
from dbdaora.keys import FallbackKey
from dbdaora.query import BaseQuery, Query
from dbdaora.repository import MemoryRepository

from ..entity import GeoSpatialData, GeoSpatialEntity


class GeoSpatialRepository(MemoryRepository[Any, GeoSpatialData, FallbackKey]):
    __skip_cls_validation__ = ('GeoSpatialRepository',)
    entity_cls: ClassVar[Type[Any]] = GeoSpatialEntity

    async def get_memory_data(  # type: ignore
        self, key: str, query: 'GeoSpatialQuery[FallbackKey]',
    ) -> Optional[GeoSpatialData]:
        if query.type == GeoSpatialQueryType.RADIUS:
            if (
                query.latitude is None
                or query.longitude is None
                or query.max_distance is None
            ):
                raise InvalidQueryError(query)

        else:
            raise InvalidQueryError(query)

        data = await self.memory_data_source.georadius(
            key=key,
            longitude=query.longitude,
            latitude=query.latitude,
            radius=query.max_distance,
            unit=query.distance_unit,
            with_dist=query.with_dist,
            with_coord=query.with_coord,
            count=query.count,
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
        data = tuple(await self.fallback_data_source.query(key))

        if not data:
            return None

        return self.make_fallback_data_for_memory(key, query, data)

    def make_fallback_data_for_memory(
        self,
        key: FallbackKey,
        query: 'GeoSpatialQuery[FallbackKey]',
        data: Sequence[Dict[str, Any]],
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
            for member in data
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
        if (
            isinstance(data, self.memory_data_source.geomember_cls)
            and data.coord is not None
        ):
            await self.memory_data_source.geoadd(
                key,
                longitude=data.coord.longitude,
                latitude=data.coord.latitude,
                member=data.member,
            )
        else:
            raise InvalidGeoSpatialDataError(data)

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
        if (
            isinstance(entity.data, self.memory_data_source.geomember_cls)
            and entity.data.coord is not None
        ):
            data = {
                'latitude': entity.data.coord.latitude,
                'longitude': entity.data.coord.longitude,
                'member': entity.data.member,
            }
            await self.fallback_data_source.put(
                self.fallback_key(entity), data, **kwargs
            )
            return

        raise InvalidGeoSpatialDataError(entity)

    def fallback_key(
        self,
        query: Union[
            'Query[GeoSpatialEntity, GeoSpatialData, FallbackKey]',
            GeoSpatialEntity,
            Dict[str, Any],
        ],
    ) -> FallbackKey:
        if isinstance(query, Query):
            return self.fallback_data_source.make_key(
                self.name, *query.key_parts, *('',)
            )

        elif isinstance(self.get_entity_type(query), _TypedDictMeta):
            return self.fallback_data_source.make_key(
                self.name,
                *self.key_parts(query),
                *(
                    query['data'].member.decode()  # type: ignore
                    if isinstance(query['data'].member, bytes)  # type: ignore
                    else query['data'].member,  # type: ignore
                ),
            )

        elif isinstance(query, self.get_entity_type(query)):
            return self.fallback_data_source.make_key(
                self.name,
                *self.key_parts(query),
                *(
                    query.data.member.decode()
                    if isinstance(query.data.member, bytes)
                    else query.data.member,
                ),
            )

        raise InvalidQueryError(query)

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

    async def delete(
        self, query: 'Query[GeoSpatialEntity, GeoSpatialData, FallbackKey]',
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def exists(
        self, query: 'Query[GeoSpatialEntity, GeoSpatialData, FallbackKey]',
    ) -> bool:
        raise NotImplementedError()  # pragma: no cover


from ..query import (  # noqa isort:skip
    GeoSpatialQuery,
    GeoSpatialQueryType,
)
from ..query import make as query_factory  # noqa isort:skip
