"""
Adapters for PostGIS geometries
"""

from typing import Optional, Type

from .. import postgres
from ..abc import AdaptContext
from ..adapt import Dumper, Loader
from ..pq import Format
from .._typeinfo import TypeInfo


try:
    import shapely.wkb as wkb
    from shapely.geometry.base import BaseGeometry

except ImportError:
    raise ImportError(
        "The module psycopg.types.geometry requires the package 'Shapely'"
        " to be installed"
    )


class GeometryBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: bytes) -> "BaseGeometry":
        return wkb.loads(data)


class GeometryLoader(Loader):
    format = Format.TEXT

    def load(self, data: bytes) -> "BaseGeometry":
        # it's a hex string in binary
        return wkb.loads(data.decode(), hex=True)


class GeometryBinaryDumper(Dumper):
    format = Format.BINARY

    def dump(self, obj: "BaseGeometry") -> bytes:
        return wkb.dumps(obj).encode()  # type: ignore


class GeometryDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: "BaseGeometry") -> bytes:
        return wkb.dumps(obj, hex=True).encode()  # type: ignore


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
    adapters.register_dumper(BaseGeometry, binary_dumper)
    adapters.register_dumper(BaseGeometry, dumper)
