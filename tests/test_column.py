import pickle

import pytest

from psycopg.postgres import types as builtins

from .fix_crdb import crdb_encoding, is_crdb, skip_crdb


def test_description_attribs(conn):
    curs = conn.cursor()
    curs.execute(
        """select
        3.14::decimal(10,2) as pi,
        'hello'::text as hi,
        '2010-02-18'::date as now
        """
    )
    assert len(curs.description) == 3
    for c in curs.description:
        len(c) == 7  # DBAPI happy
        for i, a in enumerate(
            """
            name type_code display_size internal_size precision scale null_ok
            """.split()
        ):
            assert c[i] == getattr(c, a)

        # Won't fill them up
        assert c.null_ok is None

    c = curs.description[0]
    assert c.name == "pi"
    assert c.type_code == builtins["numeric"].oid
    assert c.display_size is None
    assert c.internal_size is None
    assert c.precision == 10
    assert c.scale == 2

    c = curs.description[1]
    assert c.name == "hi"
    assert c.type_code == builtins["text"].oid
    assert c.display_size is None
    assert c.internal_size is None
    assert c.precision is None
    assert c.scale is None

    c = curs.description[2]
    assert c.name == "now"
    assert c.type_code == builtins["date"].oid
    assert c.display_size is None
    if is_crdb(conn) and conn.info.server_version < 230000:
        assert c.internal_size == 16
    else:
        assert c.internal_size == 4
    assert c.precision is None
    assert c.scale is None


def test_description_slice(conn):
    curs = conn.cursor()
    curs.execute("select 1::int as a")
    curs.description[0][0:2] == ("a", 23)


def skip_neg_scale(*args):
    return pytest.param(
        *args,
        marks=[
            pytest.mark.crdb("skip", reason="negative precision numeric"),
            pytest.mark.pg(">=15"),
        ],
    )


@pytest.mark.parametrize(
    "type, precision, scale, dsize, isize",
    [
        ("text", None, None, None, None),
        ("text[]", None, None, None, None),
        ("varchar", None, None, None, None),
        ("varchar(1)", None, None, 1, None),
        ("varchar(1)[]", None, None, 1, None),
        ("varchar(42)", None, None, 42, None),
        skip_crdb("bpchar(42)", None, None, 42, None, reason="bpchar"),
        ("varchar(10485760)", None, None, 10485760, None),
        ("int4", None, None, None, 4),
        ("numeric", None, None, None, None),
        ("numeric(10,0)", 10, 0, None, None),
        ("numeric(10,3)[]", 10, 3, None, None),
        skip_neg_scale("numeric(2,-3)", 2, -3, None, None),
        skip_neg_scale("numeric(3,5)", 3, 5, None, None),
        skip_neg_scale("numeric(1,-1000)", 1, -1000, None, None),
        skip_neg_scale("numeric(1,1000)", 1, 1000, None, None),
        ("numeric(1000,1000)", 1000, 1000, None, None),
        ("time", None, None, None, 8),
        ("time[]", None, None, None, None),
        ("timetz", None, None, None, 12),
        ("timestamp", None, None, None, 8),
        ("timestamptz", None, None, None, 8),
        ("interval", None, None, None, 16),
        ("bit(1)", None, None, 1, None),
        ("bit(42)", None, None, 42, None),
        ("bit(83886080)", None, None, 83886080, None),
        ("varbit", None, None, None, None),
        ("varbit(1)", None, None, 1, None),
        ("varbit(42)", None, None, 42, None),
        ("varbit(83886080)", None, None, 83886080, None),
    ],
)
def test_details(conn, type, precision, scale, dsize, isize):
    cur = conn.cursor()
    cur.execute(f"select null::{type}")
    col = cur.description[0]
    assert type == col.type_display
    assert f" {type} " in (repr(col))
    assert col.precision == precision
    assert col.scale == scale
    assert col.display_size == dsize
    if not is_crdb(conn):
        assert col.internal_size == isize


@pytest.mark.crdb("skip", reason="time precision")
@pytest.mark.parametrize("type", "time timetz timestamp timestamptz interval".split())
@pytest.mark.parametrize("precision", [0, 2, 6])
def test_details_time(conn, type, precision):
    type = f"{type}({precision})"
    cur = conn.cursor()
    cur.execute(f"select null::{type}")
    col = cur.description[0]
    assert type in (repr(col))
    assert col.precision == precision


def test_pickle(conn):
    curs = conn.cursor()
    curs.execute(
        """select
        3.14::decimal(10,2) as pi,
        'hello'::text as hi,
        '2010-02-18'::date as now
        """
    )
    description = curs.description
    pickled = pickle.dumps(description, pickle.HIGHEST_PROTOCOL)
    unpickled = pickle.loads(pickled)
    assert [tuple(d) for d in description] == [tuple(d) for d in unpickled]


@pytest.mark.crdb_skip("no col query")
def test_no_col_query(conn):
    cur = conn.execute("select")
    assert cur.description == []
    assert cur.fetchall() == [()]


def test_description_closed_connection(conn):
    # If we have reasons to break this test we will (e.g. we really need
    # the connection). In #172 it fails just by accident.
    cur = conn.execute("select 1::int4 as foo")
    conn.close()
    assert len(cur.description) == 1
    col = cur.description[0]
    assert col.name == "foo"
    assert col.type_code == 23


def test_name_not_a_name(conn):
    cur = conn.cursor()
    (res,) = cur.execute("""select 'x' as "foo-bar" """).fetchone()
    assert res == "x"
    assert cur.description[0].name == "foo-bar"


@pytest.mark.parametrize("encoding", ["utf8", crdb_encoding("latin9")])
def test_name_encode(conn, encoding):
    conn.execute(f"set client_encoding to {encoding}")
    cur = conn.cursor()
    (res,) = cur.execute("""select 'x' as "\u20ac" """).fetchone()
    assert res == "x"
    assert cur.description[0].name == "\u20ac"
