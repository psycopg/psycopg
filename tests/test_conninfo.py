import socket
import asyncio
import datetime as dt

import pytest

import psycopg
from psycopg import ProgrammingError
from psycopg.conninfo import make_conninfo, conninfo_to_dict, ConnectionInfo
from psycopg.conninfo import resolve_hostaddr_async
from psycopg._encodings import pg2pyenc

from .fix_crdb import crdb_encoding

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
        pgconn_val = getattr(conn.pgconn, pgconn_attr).decode()
        assert info_val == pgconn_val

        conn.close()
        with pytest.raises(psycopg.OperationalError):
            getattr(conn.info, info_attr)

    @pytest.mark.libpq("< 12")
    def test_hostaddr_not_supported(self, conn):
        with pytest.raises(psycopg.NotSupportedError):
            conn.info.hostaddr

    def test_port(self, conn):
        assert conn.info.port == int(conn.pgconn.port.decode())
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

    def test_get_params_env(self, conn_cls, dsn, monkeypatch):
        dsn = conninfo_to_dict(dsn)
        dsn.pop("application_name", None)

        monkeypatch.delenv("PGAPPNAME", raising=False)
        with conn_cls.connect(**dsn) as conn:
            assert "application_name" not in conn.info.get_parameters()

        monkeypatch.setenv("PGAPPNAME", "hello test")
        with conn_cls.connect(**dsn) as conn:
            assert conn.info.get_parameters()["application_name"] == "hello test"

    def test_dsn_env(self, conn_cls, dsn, monkeypatch):
        dsn = conninfo_to_dict(dsn)
        dsn.pop("application_name", None)

        monkeypatch.delenv("PGAPPNAME", raising=False)
        with conn_cls.connect(**dsn) as conn:
            assert "application_name=" not in conn.info.dsn

        monkeypatch.setenv("PGAPPNAME", "hello test")
        with conn_cls.connect(**dsn) as conn:
            assert "application_name='hello test'" in conn.info.dsn

    def test_status(self, conn):
        assert conn.info.status.name == "OK"
        conn.close()
        assert conn.info.status.name == "BAD"

    def test_transaction_status(self, conn):
        assert conn.info.transaction_status.name == "IDLE"
        conn.close()
        assert conn.info.transaction_status.name == "UNKNOWN"

    @pytest.mark.pipeline
    def test_pipeline_status(self, conn):
        assert not conn.info.pipeline_status
        assert conn.info.pipeline_status.name == "OFF"
        with conn.pipeline():
            assert conn.info.pipeline_status
            assert conn.info.pipeline_status.name == "ON"

    @pytest.mark.libpq("< 14")
    def test_pipeline_status_no_pipeline(self, conn):
        assert not conn.info.pipeline_status
        assert conn.info.pipeline_status.name == "OFF"

    def test_no_password(self, dsn):
        dsn2 = make_conninfo(dsn, password="the-pass-word")
        pgconn = psycopg.pq.PGconn.connect_start(dsn2.encode())
        info = ConnectionInfo(pgconn)
        assert info.password == "the-pass-word"
        assert "password" not in info.get_parameters()
        assert info.get_parameters()["dbname"] == info.dbname

    def test_dsn_no_password(self, dsn):
        dsn2 = make_conninfo(dsn, password="the-pass-word")
        pgconn = psycopg.pq.PGconn.connect_start(dsn2.encode())
        info = ConnectionInfo(pgconn)
        assert info.password == "the-pass-word"
        assert "password" not in info.dsn
        assert f"dbname={info.dbname}" in info.dsn

    def test_parameter_status(self, conn):
        assert conn.info.parameter_status("nosuchparam") is None
        tz = conn.info.parameter_status("TimeZone")
        assert tz and isinstance(tz, str)
        assert tz == conn.execute("show timezone").fetchone()[0]

    @pytest.mark.crdb("skip")
    def test_server_version(self, conn):
        assert conn.info.server_version == conn.pgconn.server_version

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

    @pytest.mark.crdb_skip("backend pid")
    def test_backend_pid(self, conn):
        assert conn.info.backend_pid
        assert conn.info.backend_pid == conn.pgconn.backend_pid
        conn.close()
        with pytest.raises(psycopg.OperationalError):
            conn.info.backend_pid

    def test_timezone(self, conn):
        conn.execute("set timezone to 'Europe/Rome'")
        tz = conn.info.timezone
        assert isinstance(tz, dt.tzinfo)
        offset = tz.utcoffset(dt.datetime(2000, 1, 1))
        assert offset and offset.total_seconds() == 3600
        offset = tz.utcoffset(dt.datetime(2000, 7, 1))
        assert offset and offset.total_seconds() == 7200

    @pytest.mark.crdb("skip", reason="crdb doesn't allow invalid timezones")
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

    def test_encoding(self, conn):
        enc = conn.execute("show client_encoding").fetchone()[0]
        assert conn.info.encoding == pg2pyenc(enc.encode())

    @pytest.mark.crdb("skip", reason="encoding not normalized")
    @pytest.mark.parametrize(
        "enc, out, codec",
        [
            ("utf8", "UTF8", "utf-8"),
            ("utf-8", "UTF8", "utf-8"),
            ("utf_8", "UTF8", "utf-8"),
            ("eucjp", "EUC_JP", "euc_jp"),
            ("euc-jp", "EUC_JP", "euc_jp"),
            ("latin9", "LATIN9", "iso8859-15"),
        ],
    )
    def test_normalize_encoding(self, conn, enc, out, codec):
        conn.execute("select set_config('client_encoding', %s, false)", [enc])
        assert conn.info.parameter_status("client_encoding") == out
        assert conn.info.encoding == codec

    @pytest.mark.parametrize(
        "enc, out, codec",
        [
            ("utf8", "UTF8", "utf-8"),
            ("utf-8", "UTF8", "utf-8"),
            ("utf_8", "UTF8", "utf-8"),
            crdb_encoding("eucjp", "EUC_JP", "euc_jp"),
            crdb_encoding("euc-jp", "EUC_JP", "euc_jp"),
        ],
    )
    def test_encoding_env_var(self, conn_cls, dsn, monkeypatch, enc, out, codec):
        monkeypatch.setenv("PGCLIENTENCODING", enc)
        with conn_cls.connect(dsn) as conn:
            clienc = conn.info.parameter_status("client_encoding")
            assert clienc
            if conn.info.vendor == "PostgreSQL":
                assert clienc == out
            else:
                assert clienc.replace("-", "").replace("_", "").upper() == out
            assert conn.info.encoding == codec

    @pytest.mark.crdb_skip("encoding")
    def test_set_encoding_unsupported(self, conn):
        cur = conn.cursor()
        cur.execute("set client_encoding to EUC_TW")
        with pytest.raises(psycopg.NotSupportedError):
            cur.execute("select 'x'")

    def test_vendor(self, conn):
        assert conn.info.vendor


@pytest.mark.parametrize(
    "conninfo, want, env",
    [
        ("", "", None),
        ("host='' user=bar", "host='' user=bar", None),
        (
            "host=127.0.0.1 user=bar",
            "host=127.0.0.1 user=bar hostaddr=127.0.0.1",
            None,
        ),
        (
            "host=1.1.1.1,2.2.2.2 user=bar",
            "host=1.1.1.1,2.2.2.2 user=bar hostaddr=1.1.1.1,2.2.2.2",
            None,
        ),
        (
            "host=1.1.1.1,2.2.2.2 port=5432",
            "host=1.1.1.1,2.2.2.2 port=5432 hostaddr=1.1.1.1,2.2.2.2",
            None,
        ),
        (
            "port=5432",
            "host=1.1.1.1,2.2.2.2 port=5432 hostaddr=1.1.1.1,2.2.2.2",
            {"PGHOST": "1.1.1.1,2.2.2.2"},
        ),
        (
            "host=foo.com port=5432",
            "host=foo.com port=5432",
            {"PGHOSTADDR": "1.2.3.4"},
        ),
    ],
)
@pytest.mark.asyncio
async def test_resolve_hostaddr_async_no_resolve(
    setpgenv, conninfo, want, env, fail_resolve
):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    params = await resolve_hostaddr_async(params)
    assert conninfo_to_dict(want) == params


@pytest.mark.parametrize(
    "conninfo, want, env",
    [
        (
            "host=foo.com,qux.com",
            "host=foo.com,qux.com hostaddr=1.1.1.1,2.2.2.2",
            None,
        ),
        (
            "host=foo.com,qux.com port=5433",
            "host=foo.com,qux.com hostaddr=1.1.1.1,2.2.2.2 port=5433",
            None,
        ),
        (
            "host=foo.com,qux.com port=5432,5433",
            "host=foo.com,qux.com hostaddr=1.1.1.1,2.2.2.2 port=5432,5433",
            None,
        ),
        (
            "host=foo.com,nosuchhost.com",
            "host=foo.com hostaddr=1.1.1.1",
            None,
        ),
        (
            "host=foo.com, port=5432,5433",
            "host=foo.com, hostaddr=1.1.1.1, port=5432,5433",
            None,
        ),
        (
            "host=nosuchhost.com,foo.com",
            "host=foo.com hostaddr=1.1.1.1",
            None,
        ),
        (
            "host=foo.com,qux.com",
            "host=foo.com,qux.com hostaddr=1.1.1.1,2.2.2.2",
            {},
        ),
    ],
)
@pytest.mark.asyncio
async def test_resolve_hostaddr_async(conninfo, want, env, fake_resolve):
    params = conninfo_to_dict(conninfo)
    params = await resolve_hostaddr_async(params)
    assert conninfo_to_dict(want) == params


@pytest.mark.parametrize(
    "conninfo, env",
    [
        ("host=bad1.com,bad2.com", None),
        ("host=foo.com port=1,2", None),
        ("host=1.1.1.1,2.2.2.2 port=5432,5433,5434", None),
        ("host=1.1.1.1,2.2.2.2", {"PGPORT": "1,2,3"}),
    ],
)
@pytest.mark.asyncio
async def test_resolve_hostaddr_async_bad(setpgenv, conninfo, env, fake_resolve):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    with pytest.raises(psycopg.Error):
        await resolve_hostaddr_async(params)


@pytest.fixture
async def fake_resolve(monkeypatch):
    fake_hosts = {
        "localhost": "127.0.0.1",
        "foo.com": "1.1.1.1",
        "qux.com": "2.2.2.2",
    }

    async def fake_getaddrinfo(host, port, **kwargs):
        assert isinstance(port, int) or (isinstance(port, str) and port.isdigit())
        try:
            addr = fake_hosts[host]
        except KeyError:
            raise OSError(f"unknown test host: {host}")
        else:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (addr, 432))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", fake_getaddrinfo)


@pytest.fixture
async def fail_resolve(monkeypatch):
    async def fail_getaddrinfo(host, port, **kwargs):
        pytest.fail(f"shouldn't try to resolve {host}")

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", fail_getaddrinfo)
