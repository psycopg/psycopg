"""
Adapters for PostGIS geometries
"""

from typing import Optional

from .. import postgres
from ..abc import AdaptContext, Buffer
from ..adapt import Dumper, Loader
from ..pq import Format
from .._typeinfo import TypeInfo


try:
    from shapely.wkb import loads, dumps
    from shapely.geometry.base import BaseGeometry

except ImportError:
    raise ImportError(
        "The module psycopg.types.shapely requires the package 'Shapely'"
        " to be installed"
    )


class GeometryBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> "BaseGeometry":
        if not isinstance(data, bytes):
            data = bytes(data)
        return loads(data)


class GeometryLoader(Loader):
    def load(self, data: Buffer) -> "BaseGeometry":
        # it's a hex string in binary
        if isinstance(data, memoryview):
            data = bytes(data)
        return loads(data.decode(), hex=True)


class BaseGeometryBinaryDumper(Dumper):
    format = Format.BINARY

    def dump(self, obj: "BaseGeometry") -> bytes:
        return dumps(obj)  # type: ignore


class BaseGeometryDumper(Dumper):
    def dump(self, obj: "BaseGeometry") -> bytes:
        return dumps(obj, hex=True).encode()  # type: ignore


def register_shapely(
    info: TypeInfo, context: Optional[AdaptContext] = None
) -> None:
    """Register Shapely dumper and loaders.

    After invoking this function on an adapter, the queries retrieving
    PostGIS geometry objects will return Shapely's shape object instances
    both in text and binary mode.

    Similarly, shape objects can be sent to the database.

    This requires the Shapely library to be installed.

    :param info: The object with the information about the geometry type.
    :param context: The context where to register the adapters. If `!None`,
        register it globally.

    .. note::

        Registering the adapters doesn't affect objects already created, even
        if they are children of the registered context. For instance,
        registering the adapter globally doesn't affect already existing
        connections.
    """

    # A friendly error warning instead of an AttributeError in case fetch()
    # failed and it wasn't noticed.
    if not info:
        raise TypeError("no info passed. Is the 'postgis' extension loaded?")

    info.register(context)
    adapters = context.adapters if context else postgres.adapters

    class GeometryDumper(BaseGeometryDumper):
        oid = info.oid

    class GeometryBinaryDumper(BaseGeometryBinaryDumper):
        oid = info.oid

    adapters.register_loader(info.oid, GeometryBinaryLoader)
    adapters.register_loader(info.oid, GeometryLoader)
    # Default binary dump
    adapters.register_dumper(BaseGeometry, GeometryDumper)
    adapters.register_dumper(BaseGeometry, GeometryBinaryDumper)
