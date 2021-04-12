import pytest

import psycopg3
from psycopg3 import ProgrammingError
from psycopg3.conninfo import make_conninfo, conninfo_to_dict, ConnectionInfo

snowman = "\u2603"


class MyString(str):
    pass


@pytest.mark.parametrize(
    "conninfo, kwargs, exp",
    [
        ("", {}, ""),
        ("dbname=foo", {}, "dbname=foo"),
        ("dbname=foo", {"user": "bar"}, "dbname=foo user=bar"),
        ("dbname=sony", {"password": ""}, "dbname=sony password="),
        ("dbname=foo", {"dbname": "bar"}, "dbname=bar"),
        ("user=bar", {"dbname": "foo bar"}, "dbname='foo bar' user=bar"),
        ("", {"dbname": "foo"}, "dbname=foo"),
        ("", {"dbname": "foo", "user": None}, "dbname=foo"),
        ("", {"dbname": "foo", "port": 15432}, "dbname=foo port=15432"),
        ("", {"dbname": "a'b"}, r"dbname='a\'b'"),
        (f"dbname={snowman}", {}, f"dbname={snowman}"),
        ("", {"dbname": snowman}, f"dbname={snowman}"),
        (
            "postgresql://host1/test",
            {"host": "host2"},
            "dbname=test host=host2",
        ),
        (MyString(""), {}, ""),
    ],
)
def test_make_conninfo(conninfo, kwargs, exp):
    out = make_conninfo(conninfo, **kwargs)
    assert conninfo_to_dict(out) == conninfo_to_dict(exp)


@pytest.mark.parametrize(
    "conninfo, kwargs",
    [
        ("hello", {}),
        ("dbname=foo bar", {}),
        ("foo=bar", {}),
        ("dbname=foo", {"bar": "baz"}),
        ("postgresql://tester:secret@/test?port=5433=x", {}),
        (f"{snowman}={snowman}", {}),
    ],
)
def test_make_conninfo_bad(conninfo, kwargs):
    with pytest.raises(ProgrammingError):
        make_conninfo(conninfo, **kwargs)


@pytest.mark.parametrize(
    "conninfo, exp",
    [
        ("", {}),
        ("dbname=foo user=bar", {"dbname": "foo", "user": "bar"}),
        ("dbname=sony password=", {"dbname": "sony", "password": ""}),
        ("dbname='foo bar'", {"dbname": "foo bar"}),
        ("dbname='a\"b'", {"dbname": 'a"b'}),
        (r"dbname='a\'b'", {"dbname": "a'b"}),
        (r"dbname='a\\b'", {"dbname": r"a\b"}),
        (f"dbname={snowman}", {"dbname": snowman}),
        (
            "postgresql://tester:secret@/test?port=5433",
            {
                "user": "tester",
                "password": "secret",
                "dbname": "test",
                "port": "5433",
            },
        ),
    ],
)
def test_conninfo_to_dict(conninfo, exp):
    assert conninfo_to_dict(conninfo) == exp


def test_no_munging():
    dsnin = "dbname=a host=b user=c password=d"
    dsnout = make_conninfo(dsnin)
    assert dsnin == dsnout


class TestConnectionInfo:
    @pytest.mark.parametrize(
        "attr", [("dbname", "db"), "host", "user", "password", "options"]
    )
    def test_attrs(self, conn, attr):
        if isinstance(attr, tuple):
            info_attr, pgconn_attr = attr
        else:
            info_attr = pgconn_attr = attr

        info_val = getattr(conn.info, info_attr)
        pgconn_val = getattr(conn.pgconn, pgconn_attr).decode("utf-8")
        assert info_val == pgconn_val

        conn.close()
        with pytest.raises(psycopg3.OperationalError):
            getattr(conn.info, info_attr)

    def test_port(self, conn):
        assert conn.info.port == int(conn.pgconn.port.decode("utf-8"))
        conn.close()
        with pytest.raises(psycopg3.OperationalError):
            conn.info.port

    def test_get_params(self, conn, dsn):
        info = conn.info.get_parameters()
        for k, v in conninfo_to_dict(dsn).items():
            assert info.get(k) == v

    def test_get_params_env(self, dsn, monkeypatch):
        dsn = conninfo_to_dict(dsn)
        dsn.pop("application_name", None)

        monkeypatch.delenv("PGAPPNAME", raising=False)
        with psycopg3.connect(**dsn) as conn:
            assert "application_name" not in conn.info.get_parameters()

        monkeypatch.setenv("PGAPPNAME", "hello test")
        with psycopg3.connect(**dsn) as conn:
            assert (
                conn.info.get_parameters()["application_name"] == "hello test"
            )

    def test_status(self, conn):
        assert conn.info.status.name == "OK"
        conn.close()
        assert conn.info.status.name == "BAD"

    def test_transaction_status(self, conn):
        assert conn.info.transaction_status.name == "IDLE"
        conn.close()
        assert conn.info.transaction_status.name == "UNKNOWN"

    def test_no_password(self, dsn):
        dsn2 = make_conninfo(dsn, password="the-pass-word")
        pgconn = psycopg3.pq.PGconn.connect_start(dsn2.encode("utf8"))
        info = ConnectionInfo(pgconn)
        assert info.password == "the-pass-word"
        assert "password" not in info.get_parameters()
        assert info.get_parameters()["dbname"] == info.dbname

    def test_parameter_status(self, conn):
        assert conn.info.parameter_status("nosuchparam") is None
        tz = conn.info.parameter_status("TimeZone")
        assert tz and isinstance(tz, str)
        assert tz == conn.execute("show timezone").fetchone()[0]
