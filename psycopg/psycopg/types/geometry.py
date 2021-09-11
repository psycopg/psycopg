"""
Adapters for PostGIS geometries
"""

from typing import TYPE_CHECKING

from psycopg.adapt import Dumper, Loader
from psycopg.pq import Format
from psycopg.types import TypeInfo

if TYPE_CHECKING:
    from psycopg.connection import Connection
    from psycopg.rows import Row


def register_shapely_adapters(conn: Connection[Row]) -> None:
    """Register Shapely dumper and loaders.

    After invoking this function on a connection, the queries retrieving
    PostGIS geometry objects will return Shapely's shape object instances
    in text and binary mode.

    Similarly, shape onjects can be sent to the database.

    :param conn: the connection on which to set up the loader and dumper

    """
    import shapely.wkb
    from shapely.geometry.base import BaseGeometry

    class GeometryBinaryLoader(Loader):
        format = Format.BINARY

        def load(self, data: bytes) -> BaseGeometry:
            return shapely.wkb.loads(data)

    class GeometryTextLoader(Loader):
        format = Format.TEXT

        def load(self, data: bytes) -> BaseGeometry:
            # it's a hex string in binary
            return shapely.wkb.loads(data.decode(), hex=True)

    class GeometryBinaryDumper(Dumper):
        format = Format.BINARY

        def dump(self, obj: BaseGeometry) -> bytes:
            return shapely.wkb.dumps(obj).encode()  # type: ignore

    class GeometryTextDumper(Dumper):
        format = Format.TEXT

        def dump(self, obj: BaseGeometry) -> bytes:
            return shapely.wkb.dumps(obj, hex=True).encode()  # type: ignore

    t = TypeInfo.fetch(conn, "geometry")
    if t is None:
        raise ValueError("The database has no geometry type. Is it PostGIS?")
    t.register(conn)
    conn.adapters.register_loader("geometry", GeometryBinaryLoader)
    conn.adapters.register_loader("geometry", GeometryTextLoader)
    conn.adapters.register_dumper(BaseGeometry, GeometryBinaryDumper)
    conn.adapters.register_dumper(BaseGeometry, GeometryTextDumper)
