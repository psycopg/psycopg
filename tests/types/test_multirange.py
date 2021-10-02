import pytest

from psycopg.adapt import PyFormat
from psycopg.types.multirange import Multirange

pytestmark = pytest.mark.pg(">= 14")

mr_names = """int4multirange int8multirange nummultirange
    datemultirange tsmultirange tstzmultirange""".split()

mr_classes = """Int4Multirange Int8Multirange NumericMultirange
    DateMultirange TimestampMultirange TimestamptzMultirange""".split()


@pytest.mark.parametrize("pgtype", mr_names)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_empty(conn, pgtype, fmt_in):
    mr = Multirange()
    cur = conn.execute(f"select '{{}}'::{pgtype} = %{fmt_in}", (mr,))
    assert cur.fetchone()[0] is True
