import sys
import time
import pytest
import logging
import weakref
from typing import Any, List

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

import psycopg
from psycopg import Connection, Notify
from psycopg.rows import tuple_row
from psycopg.errors import UndefinedTable
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from .utils import gc_collect
from .test_cursor import my_row_factory
from .test_adapt import make_bin_dumper, make_dumper


def test_connect(dsn):
    conn = Connection.connect(dsn)
    assert not conn.closed
    assert conn.pgconn.status == conn.ConnStatus.OK
    conn.close()


def test_connect_str_subclass(dsn):
    class MyString(str):
        pass

    conn = Connection.connect(MyString(dsn))
    assert not conn.closed
    assert conn.pgconn.status == conn.ConnStatus.OK
    conn.close()


def test_connect_bad():
    with pytest.raises(psycopg.OperationalError):
        Connection.connect("dbname=nosuchdb")


@pytest.mark.slow
@pytest.mark.timing
def test_connect_timeout(deaf_port):
    t0 = time.time()
    with pytest.raises(psycopg.OperationalError, match="timeout expired"):
        Connection.connect(host="localhost", port=deaf_port, connect_timeout=1)
    elapsed = time.time() - t0
    assert elapsed == pytest.approx(1.0, abs=0.05)


def test_close(conn):
    assert not conn.closed
    assert not conn.broken

    cur = conn.cursor()

    conn.close()
    assert conn.closed
    assert not conn.broken
    assert conn.pgconn.status == conn.ConnStatus.BAD

    conn.close()
    assert conn.closed
    assert conn.pgconn.status == conn.ConnStatus.BAD

    with pytest.raises(psycopg.OperationalError):
        cur.execute("select 1")


def test_broken(conn):
    with pytest.raises(psycopg.OperationalError):
        conn.execute("select pg_terminate_backend(%s)", [conn.pgconn.backend_pid])
    assert conn.closed
    assert conn.broken
    conn.close()
    assert conn.closed
    assert conn.broken


def test_cursor_closed(conn):
    conn.close()
    with pytest.raises(psycopg.OperationalError):
        with conn.cursor("foo"):
            pass
    with pytest.raises(psycopg.OperationalError):
        conn.cursor()


def test_connection_warn_close(dsn, recwarn):
    conn = Connection.connect(dsn)
    conn.close()
    del conn
    assert not recwarn, [str(w.message) for w in recwarn.list]

    conn = Connection.connect(dsn)
    del conn
    assert "IDLE" in str(recwarn.pop(ResourceWarning).message)

    conn = Connection.connect(dsn)
    conn.execute("select 1")
    del conn
    assert "INTRANS" in str(recwarn.pop(ResourceWarning).message)

    conn = Connection.connect(dsn)
    try:
        conn.execute("select wat")
    except Exception:
        pass
    del conn
    assert "INERROR" in str(recwarn.pop(ResourceWarning).message)

    with Connection.connect(dsn) as conn:
        pass
    del conn
    assert not recwarn, [str(w.message) for w in recwarn.list]


def test_context_commit(conn, dsn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists textctx")
            cur.execute("create table textctx ()")

    assert conn.closed
    assert not conn.broken

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select * from textctx")
            assert cur.fetchall() == []


def test_context_rollback(conn, dsn):
    with conn.cursor() as cur:
        cur.execute("drop table if exists textctx")
    conn.commit()

    with pytest.raises(ZeroDivisionError):
        with conn:
            with conn.cursor() as cur:
                cur.execute("create table textctx ()")
                1 / 0

    assert conn.closed
    assert not conn.broken

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            with pytest.raises(UndefinedTable):
                cur.execute("select * from textctx")


def test_context_close(conn):
    with conn:
        conn.execute("select 1")
        conn.close()


def test_context_inerror_rollback_no_clobber(conn, dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")

    with pytest.raises(ZeroDivisionError):
        with psycopg.connect(dsn) as conn2:
            conn2.execute("select 1")
            conn.execute(
                "select pg_terminate_backend(%s::int)",
                [conn2.pgconn.backend_pid],
            )
            1 / 0

    assert len(caplog.records) == 1
    rec = caplog.records[0]
    assert rec.levelno == logging.WARNING
    assert "in rollback" in rec.message


def test_context_active_rollback_no_clobber(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")

    with pytest.raises(ZeroDivisionError):
        with psycopg.connect(dsn) as conn:
            conn.pgconn.exec_(b"copy (select generate_series(1, 10)) to stdout")
            status = conn.info.transaction_status
            assert status == conn.TransactionStatus.ACTIVE
            1 / 0

    assert len(caplog.records) == 1
    rec = caplog.records[0]
    assert rec.levelno == logging.WARNING
    assert "in rollback" in rec.message


@pytest.mark.slow
def test_weakref(dsn):
    conn = psycopg.connect(dsn)
    w = weakref.ref(conn)
    conn.close()
    del conn
    gc_collect()
    assert w() is None


def test_commit(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")
    conn.pgconn.exec_(b"begin")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    conn.pgconn.exec_(b"insert into foo values (1)")
    conn.commit()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    res = conn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) == b"1"

    conn.close()
    with pytest.raises(psycopg.OperationalError):
        conn.commit()


def test_rollback(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")
    conn.pgconn.exec_(b"begin")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    conn.pgconn.exec_(b"insert into foo values (1)")
    conn.rollback()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    res = conn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.ntuples == 0

    conn.close()
    with pytest.raises(psycopg.OperationalError):
        conn.rollback()


def test_auto_transaction(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = conn.cursor()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE

    cur.execute("insert into foo values (1)")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS

    conn.commit()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    assert cur.execute("select * from foo").fetchone() == (1,)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


def test_auto_transaction_fail(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = conn.cursor()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE

    cur.execute("insert into foo values (1)")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS

    with pytest.raises(psycopg.DatabaseError):
        cur.execute("meh")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR

    with pytest.raises(psycopg.errors.InFailedSqlTransaction):
        cur.execute("select 1")

    conn.commit()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    assert cur.execute("select * from foo").fetchone() is None
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


def test_autocommit(conn):
    assert conn.autocommit is False
    conn.autocommit = True
    assert conn.autocommit
    cur = conn.cursor()
    assert cur.execute("select 1").fetchone() == (1,)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE

    conn.autocommit = ""
    assert conn.autocommit is False  # type: ignore[comparison-overlap]
    conn.autocommit = "yeah"
    assert conn.autocommit is True


def test_autocommit_connect(dsn):
    conn = Connection.connect(dsn, autocommit=True)
    assert conn.autocommit
    conn.close()


def test_autocommit_intrans(conn):
    cur = conn.cursor()
    assert cur.execute("select 1").fetchone() == (1,)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    with pytest.raises(psycopg.ProgrammingError):
        conn.autocommit = True
    assert not conn.autocommit


def test_autocommit_inerror(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg.DatabaseError):
        cur.execute("meh")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR
    with pytest.raises(psycopg.ProgrammingError):
        conn.autocommit = True
    assert not conn.autocommit


def test_autocommit_unknown(conn):
    conn.close()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.UNKNOWN
    with pytest.raises(psycopg.ProgrammingError):
        conn.autocommit = True
    assert not conn.autocommit


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
    conn = psycopg.Connection.connect(*args, **kwargs)
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

    monkeypatch.setattr(psycopg.connection, "connect", fake_connect)
    with pytest.raises(exctype):
        psycopg.Connection.connect(*args, **kwargs)


def test_broken_connection(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg.DatabaseError):
        cur.execute("select pg_terminate_backend(pg_backend_pid())")
    assert conn.closed


def test_notice_handlers(conn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    messages = []
    severities = []

    def cb1(diag):
        messages.append(diag.message_primary)

    def cb2(res):
        raise Exception("hello from cb2")

    conn.add_notice_handler(cb1)
    conn.add_notice_handler(cb2)
    conn.add_notice_handler("the wrong thing")
    conn.add_notice_handler(lambda diag: severities.append(diag.severity_nonlocalized))

    conn.pgconn.exec_(b"set client_min_messages to notice")
    cur = conn.cursor()
    cur.execute("do $$begin raise notice 'hello notice'; end$$ language plpgsql")
    assert messages == ["hello notice"]
    assert severities == ["NOTICE"]

    assert len(caplog.records) == 2
    rec = caplog.records[0]
    assert rec.levelno == logging.ERROR
    assert "hello from cb2" in rec.message
    rec = caplog.records[1]
    assert rec.levelno == logging.ERROR
    assert "the wrong thing" in rec.message

    conn.remove_notice_handler(cb1)
    conn.remove_notice_handler("the wrong thing")
    cur.execute("do $$begin raise warning 'hello warning'; end$$ language plpgsql")
    assert len(caplog.records) == 3
    assert messages == ["hello notice"]
    assert severities == ["NOTICE", "WARNING"]

    with pytest.raises(ValueError):
        conn.remove_notice_handler(cb1)


def test_notify_handlers(conn):
    nots1 = []
    nots2 = []

    def cb1(n):
        nots1.append(n)

    conn.add_notify_handler(cb1)
    conn.add_notify_handler(lambda n: nots2.append(n))

    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("listen foo")
    cur.execute("notify foo, 'n1'")

    assert len(nots1) == 1
    n = nots1[0]
    assert n.channel == "foo"
    assert n.payload == "n1"
    assert n.pid == conn.pgconn.backend_pid

    assert len(nots2) == 1
    assert nots2[0] == nots1[0]

    conn.remove_notify_handler(cb1)
    cur.execute("notify foo, 'n2'")

    assert len(nots1) == 1
    assert len(nots2) == 2
    n = nots2[1]
    assert isinstance(n, Notify)
    assert n.channel == "foo"
    assert n.payload == "n2"
    assert n.pid == conn.pgconn.backend_pid
    assert hash(n)

    with pytest.raises(ValueError):
        conn.remove_notify_handler(cb1)


def test_execute(conn):
    cur = conn.execute("select %s, %s", [10, 20])
    assert cur.fetchone() == (10, 20)
    assert cur.format == 0
    assert cur.pgresult.fformat(0) == 0

    cur = conn.execute("select %(a)s, %(b)s", {"a": 11, "b": 21})
    assert cur.fetchone() == (11, 21)

    cur = conn.execute("select 12, 22")
    assert cur.fetchone() == (12, 22)


def test_execute_binary(conn):
    cur = conn.execute("select %s, %s", [10, 20], binary=True)
    assert cur.fetchone() == (10, 20)
    assert cur.format == 1
    assert cur.pgresult.fformat(0) == 1


def test_row_factory(dsn):
    defaultconn = Connection.connect(dsn)
    assert defaultconn.row_factory is tuple_row  # type: ignore[comparison-overlap]
    defaultconn.close()

    conn = Connection.connect(dsn, row_factory=my_row_factory)
    assert conn.row_factory is my_row_factory  # type: ignore[comparison-overlap]

    cur = conn.execute("select 'a' as ve")
    assert cur.fetchone() == ["Ave"]

    with conn.cursor(row_factory=lambda c: set) as cur1:
        cur1.execute("select 1, 1, 2")
        assert cur1.fetchall() == [{1, 2}]

    with conn.cursor(row_factory=tuple_row) as cur2:
        cur2.execute("select 1, 1, 2")
        assert cur2.fetchall() == [(1, 1, 2)]

    # TODO: maybe fix something to get rid of 'type: ignore' below.
    conn.row_factory = tuple_row  # type: ignore[assignment]
    cur3 = conn.execute("select 'vale'")
    r = cur3.fetchone()
    assert r and r == ("vale",)  # type: ignore[comparison-overlap]
    conn.close()


def test_str(conn):
    assert "[IDLE]" in str(conn)
    conn.close()
    assert "[BAD]" in str(conn)


def test_fileno(conn):
    assert conn.fileno() == conn.pgconn.socket
    conn.close()
    with pytest.raises(psycopg.OperationalError):
        conn.fileno()


def test_cursor_factory(conn):
    assert conn.cursor_factory is psycopg.Cursor

    class MyCursor(psycopg.Cursor[psycopg.rows.Row]):
        pass

    conn.cursor_factory = MyCursor
    with conn.cursor() as cur:
        assert isinstance(cur, MyCursor)

    with conn.execute("select 1") as cur:
        assert isinstance(cur, MyCursor)


def test_server_cursor_factory(conn):
    assert conn.server_cursor_factory is psycopg.ServerCursor

    class MyServerCursor(psycopg.ServerCursor[psycopg.rows.Row]):
        pass

    conn.server_cursor_factory = MyServerCursor
    with conn.cursor(name="n") as cur:
        assert isinstance(cur, MyServerCursor)


class ParamDef(TypedDict):
    guc: str
    values: List[Any]
    set_method: str


tx_params = {
    "isolation_level": ParamDef(
        {
            "guc": "isolation",
            "values": list(psycopg.IsolationLevel),
            "set_method": "set_isolation_level",
        }
    ),
    "read_only": ParamDef(
        {
            "guc": "read_only",
            "values": [True, False],
            "set_method": "set_read_only",
        }
    ),
    "deferrable": ParamDef(
        {
            "guc": "deferrable",
            "values": [True, False],
            "set_method": "set_deferrable",
        }
    ),
}

# Map Python values to Postgres values for the tx_params possible values
tx_values_map = {
    v.name.lower().replace("_", " "): v.value for v in psycopg.IsolationLevel
}
tx_values_map["on"] = True
tx_values_map["off"] = False


@pytest.mark.parametrize("attr", list(tx_params))
def test_transaction_param_default(conn, attr):
    assert getattr(conn, attr) is None
    guc = tx_params[attr]["guc"]
    current, default = conn.execute(
        "select current_setting(%s), current_setting(%s)",
        [f"transaction_{guc}", f"default_transaction_{guc}"],
    ).fetchone()
    assert current == default


@pytest.mark.parametrize("autocommit", [True, False])
@pytest.mark.parametrize("attr", list(tx_params))
def test_set_transaction_param_implicit(conn, attr, autocommit):
    guc = tx_params[attr]["guc"]
    conn.autocommit = autocommit
    for value in tx_params[attr]["values"]:
        setattr(conn, attr, value)
        pgval, default = conn.execute(
            "select current_setting(%s), current_setting(%s)",
            [f"transaction_{guc}", f"default_transaction_{guc}"],
        ).fetchone()
        if autocommit:
            assert pgval == default
        else:
            assert tx_values_map[pgval] == value
        conn.rollback()


@pytest.mark.parametrize("autocommit", [True, False])
@pytest.mark.parametrize("attr", list(tx_params))
def test_set_transaction_param_block(conn, attr, autocommit):
    guc = tx_params[attr]["guc"]
    conn.autocommit = autocommit
    for value in tx_params[attr]["values"]:
        setattr(conn, attr, value)
        with conn.transaction():
            pgval = conn.execute(
                "select current_setting(%s)", [f"transaction_{guc}"]
            ).fetchone()[0]
        assert tx_values_map[pgval] == value


@pytest.mark.parametrize("attr", list(tx_params))
def test_set_transaction_param_not_intrans_implicit(conn, attr):
    conn.execute("select 1")
    with pytest.raises(psycopg.ProgrammingError):
        setattr(conn, attr, tx_params[attr]["values"][0])


@pytest.mark.parametrize("attr", list(tx_params))
def test_set_transaction_param_not_intrans_block(conn, attr):
    with conn.transaction():
        with pytest.raises(psycopg.ProgrammingError):
            setattr(conn, attr, tx_params[attr]["values"][0])


@pytest.mark.parametrize("attr", list(tx_params))
def test_set_transaction_param_not_intrans_external(conn, attr):
    conn.autocommit = True
    conn.execute("begin")
    with pytest.raises(psycopg.ProgrammingError):
        setattr(conn, attr, tx_params[attr]["values"][0])


def test_set_transaction_param_all(conn):
    for attr in tx_params:
        value = tx_params[attr]["values"][0]
        setattr(conn, attr, value)

    for attr in tx_params:
        guc = tx_params[attr]["guc"]
        pgval = conn.execute(
            "select current_setting(%s)", [f"transaction_{guc}"]
        ).fetchone()[0]
        assert tx_values_map[pgval] == value


def test_set_transaction_param_strange(conn):
    for val in ("asdf", 0, 5):
        with pytest.raises(ValueError):
            conn.isolation_level = val

    conn.isolation_level = psycopg.IsolationLevel.SERIALIZABLE.value
    assert conn.isolation_level is psycopg.IsolationLevel.SERIALIZABLE

    conn.read_only = 1
    assert conn.read_only is True

    conn.deferrable = 0
    assert conn.deferrable is False


conninfo_params_timeout = [
    (
        "",
        {"host": "localhost", "connect_timeout": None},
        ({"host": "localhost"}, None),
    ),
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
]


@pytest.mark.parametrize("dsn, kwargs, exp", conninfo_params_timeout)
def test_get_connection_params(dsn, kwargs, exp):
    params = Connection._get_connection_params(dsn, **kwargs)
    conninfo = make_conninfo(**params)
    assert conninfo_to_dict(conninfo) == exp[0]
    assert params.get("connect_timeout") == exp[1]


def test_connect_context(dsn):
    ctx = psycopg.adapt.AdaptersMap(psycopg.adapters)
    ctx.register_dumper(str, make_bin_dumper("b"))
    ctx.register_dumper(str, make_dumper("t"))

    conn = psycopg.connect(dsn, context=ctx)

    cur = conn.execute("select %s", ["hello"])
    assert cur.fetchone()[0] == "hellot"  # type: ignore[index]
    cur = conn.execute("select %b", ["hello"])
    assert cur.fetchone()[0] == "hellob"  # type: ignore[index]
    conn.close()


def test_connect_context_copy(dsn, conn):
    conn.adapters.register_dumper(str, make_bin_dumper("b"))
    conn.adapters.register_dumper(str, make_dumper("t"))

    conn2 = psycopg.connect(dsn, context=conn)

    cur = conn2.execute("select %s", ["hello"])
    assert cur.fetchone()[0] == "hellot"  # type: ignore[index]
    cur = conn2.execute("select %b", ["hello"])
    assert cur.fetchone()[0] == "hellob"  # type: ignore[index]
    conn2.close()
