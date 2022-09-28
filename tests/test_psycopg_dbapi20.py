import pytest
import datetime as dt
from typing import Any, Dict

import psycopg
from psycopg.conninfo import conninfo_to_dict

from . import dbapi20
from . import dbapi20_tpc


@pytest.fixture(scope="class")
def with_dsn(request, session_dsn):
    request.cls.connect_args = (session_dsn,)


@pytest.mark.usefixtures("with_dsn")
class PsycopgTests(dbapi20.DatabaseAPI20Test):
    driver = psycopg
    # connect_args = () # set by the fixture
    connect_kw_args: Dict[str, Any] = {}

    def test_nextset(self):
        # tested elsewhere
        pass

    def test_setoutputsize(self):
        # no-op
        pass


@pytest.mark.usefixtures("tpc")
@pytest.mark.usefixtures("with_dsn")
class PsycopgTPCTests(dbapi20_tpc.TwoPhaseCommitTests):
    driver = psycopg
    connect_args = ()  # set by the fixture

    def connect(self):
        return psycopg.connect(*self.connect_args)


# Shut up warnings
PsycopgTests.failUnless = PsycopgTests.assertTrue
PsycopgTPCTests.assertEquals = PsycopgTPCTests.assertEqual


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
    singleton = getattr(psycopg, singleton)
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
    s = psycopg.TimestampFromTicks(ticks)
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
    s = psycopg.DateFromTicks(ticks)
    if isinstance(want, str):
        want = [want]
    want = [dt.datetime.strptime(w, "%Y-%m-%d").date() for w in want]
    assert s in want


@pytest.mark.parametrize(
    "ticks, want",
    [(0, "00:00:00.000000"), (1273173119.99992, "00:11:59.999920")],
)
def test_time_from_ticks(ticks, want):
    s = psycopg.TimeFromTicks(ticks)
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
    the_conninfo: str

    def fake_connect(conninfo):
        nonlocal the_conninfo
        the_conninfo = conninfo
        return pgconn
        yield

    monkeypatch.setattr(psycopg.connection, "connect", fake_connect)
    conn = psycopg.connect(*args, **kwargs)
    assert conninfo_to_dict(the_conninfo) == conninfo_to_dict(want)
    conn.close()


@pytest.mark.parametrize(
    "args, kwargs, exctype",
    [
        (("host=foo", "host=bar"), {}, TypeError),
        (("", ""), {}, TypeError),
        ((), {"nosuchparam": 42}, psycopg.ProgrammingError),
    ],
)
def test_connect_badargs(monkeypatch, pgconn, args, kwargs, exctype):
    def fake_connect(conninfo):
        return pgconn
        yield

    with pytest.raises(exctype):
        psycopg.connect(*args, **kwargs)
