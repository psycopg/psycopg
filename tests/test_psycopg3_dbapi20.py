import pytest
import datetime as dt

import psycopg3
from psycopg3.conninfo import conninfo_to_dict

from . import dbapi20


@pytest.fixture(scope="class")
def with_dsn(request, dsn):
    request.cls.connect_args = (dsn,)


@pytest.mark.usefixtures("with_dsn")
class Psycopg3Tests(dbapi20.DatabaseAPI20Test):
    driver = psycopg3
    # connect_args = () # set by the fixture
    connect_kw_args = {}

    def test_nextset(self):
        # tested elsewhere
        pass

    def test_setoutputsize(self):
        # no-op
        pass


# Shut up warnings
Psycopg3Tests.failUnless = Psycopg3Tests.assertTrue


@pytest.mark.parametrize(
    "typename, singleton",
    [
        ("bytea", "BINARY"),
        ("date", "DATETIME"),
        ("timestamp without time zone", "DATETIME"),
        ("timestamp with time zone", "DATETIME"),
        ("time without time zone", "DATETIME"),
        ("time with time zone", "DATETIME"),
        ("interval", "DATETIME"),
        ("integer", "NUMBER"),
        ("smallint", "NUMBER"),
        ("bigint", "NUMBER"),
        ("real", "NUMBER"),
        ("double precision", "NUMBER"),
        ("numeric", "NUMBER"),
        ("decimal", "NUMBER"),
        ("oid", "ROWID"),
        ("varchar", "STRING"),
        ("char", "STRING"),
        ("text", "STRING"),
    ],
)
def test_singletons(conn, typename, singleton):
    singleton = getattr(psycopg3, singleton)
    cur = conn.cursor()
    cur.execute(f"select null::{typename}")
    oid = cur.description[0].type_code
    assert singleton == oid
    assert oid == singleton
    assert singleton != oid + 10000
    assert oid + 10000 != singleton


@pytest.mark.parametrize(
    "ticks, want",
    [
        (0, "1970-01-01T00:00:00.000000+0000"),
        (1273173119.99992, "2010-05-06T14:11:59.999920-0500"),
    ],
)
def test_timestamp_from_ticks(ticks, want):
    s = psycopg3.TimestampFromTicks(ticks)
    want = dt.datetime.strptime(want, "%Y-%m-%dT%H:%M:%S.%f%z")
    assert s == want


@pytest.mark.parametrize(
    "ticks, want",
    [
        (0, "1970-01-01"),
        # Returned date is local
        (1273173119.99992, ["2010-05-06", "2010-05-07"]),
    ],
)
def test_date_from_ticks(ticks, want):
    s = psycopg3.DateFromTicks(ticks)
    if isinstance(want, str):
        want = [want]
    want = [dt.datetime.strptime(w, "%Y-%m-%d").date() for w in want]
    assert s in want


@pytest.mark.parametrize(
    "ticks, want",
    [(0, "00:00:00.000000"), (1273173119.99992, "00:11:59.999920")],
)
def test_time_from_ticks(ticks, want):
    s = psycopg3.TimeFromTicks(ticks)
    want = dt.datetime.strptime(want, "%H:%M:%S.%f").time()
    assert s.replace(hour=0) == want


@pytest.mark.parametrize(
    "args, kwargs, want",
    [
        ((), {}, ""),
        (("",), {}, ""),
        (("host=foo user=bar",), {}, "host=foo user=bar"),
        (("host=foo",), {"user": "baz"}, "host=foo user=baz"),
        (
            ("host=foo port=5432",),
            {"host": "qux", "user": "joe"},
            "host=qux user=joe port=5432",
        ),
        (("host=foo",), {"user": None}, "host=foo"),
    ],
)
def test_connect_args(monkeypatch, pgconn, args, kwargs, want):
    the_conninfo = None

    def fake_connect(conninfo):
        nonlocal the_conninfo
        the_conninfo = conninfo
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    psycopg3.connect(*args, **kwargs)
    assert conninfo_to_dict(the_conninfo) == conninfo_to_dict(want)


@pytest.mark.parametrize(
    "args, kwargs",
    [
        (("host=foo", "host=bar"), {}),
        (("", ""), {}),
        ((), {"nosuchparam": 42}),
    ],
)
def test_connect_badargs(monkeypatch, pgconn, args, kwargs):
    def fake_connect(conninfo):
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    with pytest.raises((TypeError, psycopg3.ProgrammingError)):
        psycopg3.connect(*args, **kwargs)
