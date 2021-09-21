"""
Adapters for PostGIS geometries
"""

from typing import Optional, Type

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
    format = Format.TEXT

    def load(self, data: Buffer) -> "BaseGeometry":
        # it's a hex string in binary
        if isinstance(data, memoryview):
            data = bytes(data)
        return loads(data.decode(), hex=True)


class GeometryBinaryDumper(Dumper):
    format = Format.BINARY

    def dump(self, obj: "BaseGeometry") -> bytes:
        return dumps(obj)  # type: ignore


class GeometryDumper(Dumper):
    format = Format.TEXT

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

    """

    # A friendly error warning instead of an AttributeError in case fetch()
    # failed and it wasn't noticed.
    if not info:
        raise TypeError("no info passed. Is the 'postgis' extension loaded?")

    info.register(context)
    adapters = context.adapters if context else postgres.adapters
    # Generate and register the text and binary dumper
    binary_dumper: Type[GeometryBinaryDumper] = type(
        "GeometryBinaryDumper", (GeometryBinaryDumper,), {"oid": info.oid}
    )
    dumper: Type[GeometryDumper] = type(
        "GeometryDumper", (GeometryDumper,), {"oid": info.oid}
    )

    adapters.register_loader(info.oid, GeometryBinaryLoader)
    adapters.register_loader(info.oid, GeometryLoader)
    adapters.register_dumper(BaseGeometry, dumper)
    adapters.register_dumper(BaseGeometry, binary_dumper)
