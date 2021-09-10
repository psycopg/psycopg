"""
Adapters for PostGIS geometries
"""

import psycopg
from psycopg.adapt import Dumper, Loader
from psycopg.pq import Format
from psycopg.types import TypeInfo


def register_shapely_adapters(conn: psycopg.Connection):
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
        def load(self, data):
            return shapely.wkb.loads(data)

    class GeometryTextLoader(Loader):
        format = Format.TEXT
        def load(self, data):
            # it's a hex string in binary
            return shapely.wkb.loads(data.decode(), hex=True)

    class GeometryBinaryDumper(Dumper):
        format = Format.BINARY
        def dump(self, obj):
            return shapely.wkb.dumps(obj).encode()

    class GeometryTextDumper(Dumper):
        format = Format.TEXT
        def dump(self, obj):
            return shapely.wkb.dumps(obj, hex=True).encode()

    t = TypeInfo.fetch(conn, "geometry")
    t.register(conn)
    conn.adapters.register_loader("geometry", GeometryBinaryLoader)
    conn.adapters.register_loader("geometry", GeometryTextLoader)
    conn.adapters.register_dumper(BaseGeometry, GeometryBinaryDumper)
    conn.adapters.register_dumper(BaseGeometry, GeometryTextDumper)
