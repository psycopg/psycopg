import sys
import datetime as dt

import pytest

import psycopg
from psycopg import ProgrammingError
from psycopg.conninfo import (
    _conninfo_connect_timeout,
    make_conninfo,
    conninfo_to_dict,
    ConnectionInfo,
)

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


@pytest.mark.parametrize(
    "dsn, kwargs, exp",
    [
        (
            "",
            {"host": "localhost", "connect_timeout": 1},
            ({"host": "localhost", "connect_timeout": "1"}, 1),
        ),
        (
            "dbname=postgres",
            {},
            ({"dbname": "postgres"}, None),
        ),
        (
            "dbname=postgres connect_timeout=2",
            {},
            ({"dbname": "postgres", "connect_timeout": "2"}, 2),
        ),
        (
            "postgresql:///postgres?connect_timeout=2",
            {"connect_timeout": 10},
            ({"dbname": "postgres", "connect_timeout": "10"}, 10),
        ),
    ],
)
def test__conninfo_connect_timeout(dsn, kwargs, exp):
    conninfo, connect_timeout = _conninfo_connect_timeout(dsn, **kwargs)
    assert conninfo_to_dict(conninfo) == exp[0]
    assert connect_timeout == exp[1]


class TestConnectionInfo:
    @pytest.mark.parametrize(
        "attr",
        [("dbname", "db"), "host", "hostaddr", "user", "password", "options"],
    )
    def test_attrs(self, conn, attr):
        if isinstance(attr, tuple):
            info_attr, pgconn_attr = attr
        else:
            info_attr = pgconn_attr = attr

        if info_attr == "hostaddr" and psycopg.pq.version() < 120000:
            pytest.skip("hostaddr not supported on libpq < 12")

        info_val = getattr(conn.info, info_attr)
        pgconn_val = getattr(conn.pgconn, pgconn_attr).decode("utf-8")
        assert info_val == pgconn_val

        conn.close()
        with pytest.raises(psycopg.OperationalError):
            getattr(conn.info, info_attr)

    @pytest.mark.libpq("< 12")
    def test_hostaddr_not_supported(self, conn):
        with pytest.raises(psycopg.NotSupportedError):
            conn.info.hostaddr

    def test_port(self, conn):
        assert conn.info.port == int(conn.pgconn.port.decode("utf-8"))
        conn.close()
        with pytest.raises(psycopg.OperationalError):
            conn.info.port

    def test_get_params(self, conn, dsn):
        info = conn.info.get_parameters()
        for k, v in conninfo_to_dict(dsn).items():
            if k != "password":
                assert info.get(k) == v
            else:
                assert k not in info

    def test_dsn(self, conn, dsn):
        dsn = conn.info.dsn
        assert "password" not in dsn
        for k, v in conninfo_to_dict(dsn).items():
            if k != "password":
                assert f"{k}=" in dsn

    def test_get_params_env(self, dsn, monkeypatch):
        dsn = conninfo_to_dict(dsn)
        dsn.pop("application_name", None)

        monkeypatch.delenv("PGAPPNAME", raising=False)
        with psycopg.connect(**dsn) as conn:
            assert "application_name" not in conn.info.get_parameters()

        monkeypatch.setenv("PGAPPNAME", "hello test")
        with psycopg.connect(**dsn) as conn:
            assert (
                conn.info.get_parameters()["application_name"] == "hello test"
            )

    def test_dsn_env(self, dsn, monkeypatch):
        dsn = conninfo_to_dict(dsn)
        dsn.pop("application_name", None)

        monkeypatch.delenv("PGAPPNAME", raising=False)
        with psycopg.connect(**dsn) as conn:
            assert "application_name=" not in conn.info.dsn

        monkeypatch.setenv("PGAPPNAME", "hello test")
        with psycopg.connect(**dsn) as conn:
            assert "application_name='hello test'" in conn.info.dsn

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
        pgconn = psycopg.pq.PGconn.connect_start(dsn2.encode("utf8"))
        info = ConnectionInfo(pgconn)
        assert info.password == "the-pass-word"
        assert "password" not in info.get_parameters()
        assert info.get_parameters()["dbname"] == info.dbname

    def test_dsn_no_password(self, dsn):
        dsn2 = make_conninfo(dsn, password="the-pass-word")
        pgconn = psycopg.pq.PGconn.connect_start(dsn2.encode("utf8"))
        info = ConnectionInfo(pgconn)
        assert info.password == "the-pass-word"
        assert "password" not in info.dsn
        assert f"dbname={info.dbname}" in info.dsn

    def test_parameter_status(self, conn):
        assert conn.info.parameter_status("nosuchparam") is None
        tz = conn.info.parameter_status("TimeZone")
        assert tz and isinstance(tz, str)
        assert tz == conn.execute("show timezone").fetchone()[0]

    def test_server_version(self, conn):
        assert conn.info.server_version == conn.pgconn.server_version

    def test_protocol_version(self, conn):
        assert conn.info.protocol_version >= 3

    def test_error_message(self, conn):
        assert conn.info.error_message == ""
        with pytest.raises(psycopg.ProgrammingError) as ex:
            conn.execute("wat")

        assert conn.info.error_message
        assert str(ex.value) in conn.info.error_message
        assert ex.value.diag.severity in conn.info.error_message

        conn.close()
        with pytest.raises(psycopg.OperationalError):
            conn.info.error_message

    def test_backend_pid(self, conn):
        assert conn.info.backend_pid
        assert conn.info.backend_pid == conn.pgconn.backend_pid
        conn.close()
        with pytest.raises(psycopg.OperationalError):
            conn.info.backend_pid

    @pytest.mark.skipif(
        sys.platform == "win32", reason="no IANA db on Windows"
    )
    def test_timezone(self, conn):
        conn.execute("set timezone to 'Europe/Rome'")
        tz = conn.info.timezone
        assert isinstance(tz, dt.tzinfo)
        assert tz.utcoffset(dt.datetime(2000, 1, 1)).total_seconds() == 3600
        assert tz.utcoffset(dt.datetime(2000, 7, 1)).total_seconds() == 7200

    def test_timezone_warn(self, conn, caplog):
        conn.execute("set timezone to 'FOOBAR0'")
        assert len(caplog.records) == 0
        tz = conn.info.timezone
        assert tz == dt.timezone.utc
        assert len(caplog.records) == 1
        assert "FOOBAR0" in caplog.records[0].message

        conn.info.timezone
        assert len(caplog.records) == 1

        conn.execute("set timezone to 'FOOBAAR0'")
        assert len(caplog.records) == 1
        conn.info.timezone
        assert len(caplog.records) == 2
        assert "FOOBAAR0" in caplog.records[1].message
