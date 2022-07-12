import pytest

import psycopg
from psycopg.pq import Format
from psycopg.types import TypeInfo
from psycopg.adapt import PyFormat

pytest.importorskip("shapely")

from shapely.geometry import Point, Polygon, MultiPolygon  # noqa: E402
from psycopg.types.shapely import register_shapely  # noqa: E402

pytestmark = [
    pytest.mark.postgis,
    pytest.mark.crdb("skip"),
]

# real example, with CRS and "holes"
MULTIPOLYGON_GEOJSON = """
{
   "type":"MultiPolygon",
   "crs":{
      "type":"name",
      "properties":{
         "name":"EPSG:3857"
      }
   },
   "coordinates":[
      [
         [
            [89574.61111389, 6894228.638802719],
            [89576.815239808, 6894208.60747024],
            [89576.904295401, 6894207.820852726],
            [89577.99522641, 6894208.022080451],
            [89577.961830563, 6894209.229446936],
            [89589.227363031, 6894210.601454523],
            [89594.615226386, 6894161.849595264],
            [89600.314784314, 6894111.37846976],
            [89651.187791607, 6894116.774968589],
            [89648.49385993, 6894140.226914071],
            [89642.92788539, 6894193.423936413],
            [89639.721884055, 6894224.08372821],
            [89589.283022777, 6894218.431048969],
            [89588.192091767, 6894230.248628867],
            [89574.61111389, 6894228.638802719]
         ],
         [
            [89610.344670435, 6894182.466199101],
            [89625.985058891, 6894184.258949757],
            [89629.547282597, 6894153.270030369],
            [89613.918026089, 6894151.458993318],
            [89610.344670435, 6894182.466199101]
         ]
      ]
   ]
}"""

SAMPLE_POINT_GEOJSON = '{"type":"Point","coordinates":[1.2, 3.4]}'


@pytest.fixture
def shapely_conn(conn, svcconn):
    try:
        with svcconn.transaction():
            svcconn.execute("create extension if not exists postgis")
    except psycopg.Error as e:
        pytest.skip(f"can't create extension postgis: {e}")

    info = TypeInfo.fetch(conn, "geometry")
    assert info
    register_shapely(info, conn)
    return conn


def test_no_adapter(conn):
    point = Point(1.2, 3.4)
    with pytest.raises(psycopg.ProgrammingError, match="cannot adapt type 'Point'"):
        conn.execute("SELECT pg_typeof(%s)", [point]).fetchone()[0]


def test_no_info_error(conn):
    from psycopg.types.shapely import register_shapely

    with pytest.raises(TypeError, match="postgis.*extension"):
        register_shapely(None, conn)  # type: ignore[arg-type]


def test_with_adapter(shapely_conn):
    SAMPLE_POINT = Point(1.2, 3.4)
    SAMPLE_POLYGON = Polygon([(0, 0), (1, 1), (1, 0)])

    assert (
        shapely_conn.execute("SELECT pg_typeof(%s)", [SAMPLE_POINT]).fetchone()[0]
        == "geometry"
    )

    assert (
        shapely_conn.execute("SELECT pg_typeof(%s)", [SAMPLE_POLYGON]).fetchone()[0]
        == "geometry"
    )


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", Format)
def test_write_read_shape(shapely_conn, fmt_in, fmt_out):
    SAMPLE_POINT = Point(1.2, 3.4)
    SAMPLE_POLYGON = Polygon([(0, 0), (1, 1), (1, 0)])

    with shapely_conn.cursor(binary=fmt_out) as cur:
        cur.execute(
            """
        create table sample_geoms(
            id     INTEGER PRIMARY KEY,
            geom   geometry
        )
        """
        )
        cur.execute(
            f"insert into sample_geoms(id, geom) VALUES(1, %{fmt_in})",
            (SAMPLE_POINT,),
        )
        cur.execute(
            f"insert into sample_geoms(id, geom) VALUES(2, %{fmt_in})",
            (SAMPLE_POLYGON,),
        )

        cur.execute("select geom from sample_geoms where id=1")
        result = cur.fetchone()[0]
        assert result == SAMPLE_POINT

        cur.execute("select geom from sample_geoms where id=2")
        result = cur.fetchone()[0]
        assert result == SAMPLE_POLYGON


@pytest.mark.parametrize("fmt_out", Format)
def test_match_geojson(shapely_conn, fmt_out):
    SAMPLE_POINT = Point(1.2, 3.4)
    with shapely_conn.cursor(binary=fmt_out) as cur:
        cur.execute(
            """
            select ST_GeomFromGeoJSON(%s)
            """,
            (SAMPLE_POINT_GEOJSON,),
        )
        result = cur.fetchone()[0]
        # clone the coordinates to have a list instead of a shapely wrapper
        assert result.coords[:] == SAMPLE_POINT.coords[:]
        #
        cur.execute("select ST_GeomFromGeoJSON(%s)", (MULTIPOLYGON_GEOJSON,))
        result = cur.fetchone()[0]
        assert isinstance(result, MultiPolygon)
