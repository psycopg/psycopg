import datetime as dt

import pytest

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg._encodings import pg2pyenc

from .fix_crdb import crdb_encoding


@pytest.mark.parametrize(
    "attr",
    [("dbname", "db"), "host", "hostaddr", "user", "password", "options"],
)
def test_attrs(conn, attr):
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
def test_hostaddr_not_supported(conn):
    with pytest.raises(psycopg.NotSupportedError):
        conn.info.hostaddr


def test_port(conn):
    assert conn.info.port == int(conn.pgconn.port.decode())
    conn.close()
    with pytest.raises(psycopg.OperationalError):
        conn.info.port


@pytest.mark.skipif(psycopg.pq.__impl__ != "python", reason="can't monkeypatch C")
def test_blank_port(conn, monkeypatch):

    import psycopg.pq._pq_ctypes

    monkeypatch.setenv("PGPORT", "9999")
    monkeypatch.setattr(psycopg.pq._pq_ctypes, "PQport", lambda self: b"")
    assert conn.pgconn.port == b""
    # assume 5432 is the compiled value
    assert conn.info.port == 5432

    assert "port=" not in repr(conn)


def test_get_params(conn, dsn):
    info = conn.info.get_parameters()
    for k, v in conninfo_to_dict(dsn).items():
        if k != "password":
            assert info.get(k) == v
        else:
            assert k not in info


def test_dsn(conn, dsn):
    dsn = conn.info.dsn
    assert "password" not in dsn
    for k, v in conninfo_to_dict(dsn).items():
        if k != "password":
            assert f"{k}=" in dsn


def test_get_params_env(conn_cls, dsn, monkeypatch):
    dsn = conninfo_to_dict(dsn)
    dsn.pop("application_name", None)

    monkeypatch.delenv("PGAPPNAME", raising=False)
    with conn_cls.connect(**dsn) as conn:
        assert "application_name" not in conn.info.get_parameters()

    monkeypatch.setenv("PGAPPNAME", "hello test")
    with conn_cls.connect(**dsn) as conn:
        assert conn.info.get_parameters()["application_name"] == "hello test"


def test_dsn_env(conn_cls, dsn, monkeypatch):
    dsn = conninfo_to_dict(dsn)
    dsn.pop("application_name", None)

    monkeypatch.delenv("PGAPPNAME", raising=False)
    with conn_cls.connect(**dsn) as conn:
        assert "application_name=" not in conn.info.dsn

    monkeypatch.setenv("PGAPPNAME", "hello test")
    with conn_cls.connect(**dsn) as conn:
        assert "application_name='hello test'" in conn.info.dsn


def test_status(conn):
    assert conn.info.status.name == "OK"
    conn.close()
    assert conn.info.status.name == "BAD"


def test_transaction_status(conn):
    assert conn.info.transaction_status.name == "IDLE"
    conn.close()
    assert conn.info.transaction_status.name == "UNKNOWN"


@pytest.mark.pipeline
def test_pipeline_status(conn):
    assert not conn.info.pipeline_status
    assert conn.info.pipeline_status.name == "OFF"
    with conn.pipeline():
        assert conn.info.pipeline_status
        assert conn.info.pipeline_status.name == "ON"


@pytest.mark.libpq("< 14")
def test_pipeline_status_no_pipeline(conn):
    assert not conn.info.pipeline_status
    assert conn.info.pipeline_status.name == "OFF"


def test_no_password(dsn):
    dsn2 = make_conninfo(dsn, password="the-pass-word")
    pgconn = psycopg.pq.PGconn.connect_start(dsn2.encode())
    info = psycopg.ConnectionInfo(pgconn)
    assert info.password == "the-pass-word"
    assert "password" not in info.get_parameters()
    assert info.get_parameters()["dbname"] == info.dbname


def test_dsn_no_password(dsn):
    dsn2 = make_conninfo(dsn, password="the-pass-word")
    pgconn = psycopg.pq.PGconn.connect_start(dsn2.encode())
    info = psycopg.ConnectionInfo(pgconn)
    assert info.password == "the-pass-word"
    assert "password" not in info.dsn
    assert f"dbname={info.dbname}" in info.dsn


def test_parameter_status(conn):
    assert conn.info.parameter_status("nosuchparam") is None
    tz = conn.info.parameter_status("TimeZone")
    assert tz and isinstance(tz, str)
    assert tz == conn.execute("show timezone").fetchone()[0]


@pytest.mark.crdb("skip")
def test_server_version(conn):
    assert conn.info.server_version == conn.pgconn.server_version


def test_error_message(conn):
    assert conn.info.error_message == ""
    with pytest.raises(psycopg.ProgrammingError) as ex:
        conn.execute("wat")

    assert conn.info.error_message
    assert str(ex.value) in conn.info.error_message
    assert ex.value.diag.severity in conn.info.error_message

    conn.close()
    assert "NULL" in conn.info.error_message


@pytest.mark.crdb_skip("backend pid")
def test_backend_pid(conn):
    assert conn.info.backend_pid
    assert conn.info.backend_pid == conn.pgconn.backend_pid
    conn.close()
    with pytest.raises(psycopg.OperationalError):
        conn.info.backend_pid


def test_timezone(conn):
    conn.execute("set timezone to 'Europe/Rome'")
    tz = conn.info.timezone
    assert isinstance(tz, dt.tzinfo)
    offset = tz.utcoffset(dt.datetime(2000, 1, 1))
    assert offset and offset.total_seconds() == 3600
    offset = tz.utcoffset(dt.datetime(2000, 7, 1))
    assert offset and offset.total_seconds() == 7200


@pytest.mark.crdb("skip", reason="crdb doesn't allow invalid timezones")
def test_timezone_warn(conn, caplog):
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


def test_encoding(conn):
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
def test_normalize_encoding(conn, enc, out, codec):
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
def test_encoding_env_var(conn_cls, dsn, monkeypatch, enc, out, codec):
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
def test_set_encoding_unsupported(conn):
    cur = conn.cursor()
    cur.execute("set client_encoding to EUC_TW")
    with pytest.raises(psycopg.NotSupportedError):
        cur.execute("select 'x'")


def test_vendor(conn):
    assert conn.info.vendor
