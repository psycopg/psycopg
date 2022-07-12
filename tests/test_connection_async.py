import time
import pytest
import logging
import weakref
from typing import List, Any

import psycopg
from psycopg import Notify, errors as e
from psycopg.rows import tuple_row
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from .utils import gc_collect
from .test_cursor import my_row_factory
from .test_connection import tx_params, tx_params_isolation, tx_values_map
from .test_connection import conninfo_params_timeout
from .test_connection import testctx  # noqa: F401  # fixture
from .test_adapt import make_bin_dumper, make_dumper
from .test_conninfo import fake_resolve  # noqa: F401

pytestmark = pytest.mark.asyncio


async def test_connect(aconn_cls, dsn):
    conn = await aconn_cls.connect(dsn)
    assert not conn.closed
    assert conn.pgconn.status == conn.ConnStatus.OK
    await conn.close()


async def test_connect_bad(aconn_cls):
    with pytest.raises(psycopg.OperationalError):
        await aconn_cls.connect("dbname=nosuchdb")


async def test_connect_str_subclass(aconn_cls, dsn):
    class MyString(str):
        pass

    conn = await aconn_cls.connect(MyString(dsn))
    assert not conn.closed
    assert conn.pgconn.status == conn.ConnStatus.OK
    await conn.close()


@pytest.mark.slow
@pytest.mark.timing
async def test_connect_timeout(aconn_cls, deaf_port):
    t0 = time.time()
    with pytest.raises(psycopg.OperationalError, match="timeout expired"):
        await aconn_cls.connect(host="localhost", port=deaf_port, connect_timeout=1)
    elapsed = time.time() - t0
    assert elapsed == pytest.approx(1.0, abs=0.05)


async def test_close(aconn):
    assert not aconn.closed
    assert not aconn.broken

    cur = aconn.cursor()

    await aconn.close()
    assert aconn.closed
    assert not aconn.broken
    assert aconn.pgconn.status == aconn.ConnStatus.BAD

    await aconn.close()
    assert aconn.closed
    assert aconn.pgconn.status == aconn.ConnStatus.BAD

    with pytest.raises(psycopg.OperationalError):
        await cur.execute("select 1")


@pytest.mark.crdb_skip("pg_terminate_backend")
async def test_broken(aconn):
    with pytest.raises(psycopg.OperationalError):
        await aconn.execute(
            "select pg_terminate_backend(%s)", [aconn.pgconn.backend_pid]
        )
    assert aconn.closed
    assert aconn.broken
    await aconn.close()
    assert aconn.closed
    assert aconn.broken


async def test_cursor_closed(aconn):
    await aconn.close()
    with pytest.raises(psycopg.OperationalError):
        async with aconn.cursor("foo"):
            pass
        aconn.cursor("foo")
    with pytest.raises(psycopg.OperationalError):
        aconn.cursor()


async def test_connection_warn_close(aconn_cls, dsn, recwarn):
    conn = await aconn_cls.connect(dsn)
    await conn.close()
    del conn
    assert not recwarn, [str(w.message) for w in recwarn.list]

    conn = await aconn_cls.connect(dsn)
    del conn
    assert "IDLE" in str(recwarn.pop(ResourceWarning).message)

    conn = await aconn_cls.connect(dsn)
    await conn.execute("select 1")
    del conn
    assert "INTRANS" in str(recwarn.pop(ResourceWarning).message)

    conn = await aconn_cls.connect(dsn)
    try:
        await conn.execute("select wat")
    except Exception:
        pass
    del conn
    assert "INERROR" in str(recwarn.pop(ResourceWarning).message)

    async with await aconn_cls.connect(dsn) as conn:
        pass
    del conn
    assert not recwarn, [str(w.message) for w in recwarn.list]


@pytest.mark.usefixtures("testctx")
async def test_context_commit(aconn_cls, aconn, dsn):
    async with aconn:
        async with aconn.cursor() as cur:
            await cur.execute("insert into testctx values (42)")

    assert aconn.closed
    assert not aconn.broken

    async with await aconn_cls.connect(dsn) as aconn:
        async with aconn.cursor() as cur:
            await cur.execute("select * from testctx")
            assert await cur.fetchall() == [(42,)]


@pytest.mark.usefixtures("testctx")
async def test_context_rollback(aconn_cls, aconn, dsn):
    with pytest.raises(ZeroDivisionError):
        async with aconn:
            async with aconn.cursor() as cur:
                await cur.execute("insert into testctx values (42)")
                1 / 0

    assert aconn.closed
    assert not aconn.broken

    async with await aconn_cls.connect(dsn) as aconn:
        async with aconn.cursor() as cur:
            await cur.execute("select * from testctx")
            assert await cur.fetchall() == []


async def test_context_close(aconn):
    async with aconn:
        await aconn.execute("select 1")
        await aconn.close()


@pytest.mark.crdb_skip("pg_terminate_backend")
async def test_context_inerror_rollback_no_clobber(aconn_cls, conn, dsn, caplog):
    with pytest.raises(ZeroDivisionError):
        async with await aconn_cls.connect(dsn) as conn2:
            await conn2.execute("select 1")
            conn.execute(
                "select pg_terminate_backend(%s::int)",
                [conn2.pgconn.backend_pid],
            )
            1 / 0

    assert len(caplog.records) == 1
    rec = caplog.records[0]
    assert rec.levelno == logging.WARNING
    assert "in rollback" in rec.message


@pytest.mark.crdb_skip("copy")
async def test_context_active_rollback_no_clobber(aconn_cls, dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")

    with pytest.raises(ZeroDivisionError):
        async with await aconn_cls.connect(dsn) as conn:
            conn.pgconn.exec_(b"copy (select generate_series(1, 10)) to stdout")
            assert not conn.pgconn.error_message
            status = conn.info.transaction_status
            assert status == conn.TransactionStatus.ACTIVE
            1 / 0

    assert len(caplog.records) == 1
    rec = caplog.records[0]
    assert rec.levelno == logging.WARNING
    assert "in rollback" in rec.message


@pytest.mark.slow
async def test_weakref(aconn_cls, dsn):
    conn = await aconn_cls.connect(dsn)
    w = weakref.ref(conn)
    await conn.close()
    del conn
    gc_collect()
    assert w() is None


async def test_commit(aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    aconn.pgconn.exec_(b"insert into foo values (1)")
    await aconn.commit()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) == b"1"

    await aconn.close()
    with pytest.raises(psycopg.OperationalError):
        await aconn.commit()


@pytest.mark.crdb_skip("deferrable")
async def test_commit_error(aconn):
    await aconn.execute(
        """
        drop table if exists selfref;
        create table selfref (
            x serial primary key,
            y int references selfref (x) deferrable initially deferred)
        """
    )
    await aconn.commit()

    await aconn.execute("insert into selfref (y) values (-1)")
    with pytest.raises(e.ForeignKeyViolation):
        await aconn.commit()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    cur = await aconn.execute("select 1")
    assert await cur.fetchone() == (1,)


async def test_rollback(aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    aconn.pgconn.exec_(b"insert into foo values (1)")
    await aconn.rollback()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.ntuples == 0

    await aconn.close()
    with pytest.raises(psycopg.OperationalError):
        await aconn.rollback()


async def test_auto_transaction(aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = aconn.cursor()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE

    await cur.execute("insert into foo values (1)")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS

    await aconn.commit()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    await cur.execute("select * from foo")
    assert await cur.fetchone() == (1,)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


async def test_auto_transaction_fail(aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = aconn.cursor()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE

    await cur.execute("insert into foo values (1)")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS

    with pytest.raises(psycopg.DatabaseError):
        await cur.execute("meh")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR

    await aconn.commit()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    await cur.execute("select * from foo")
    assert await cur.fetchone() is None
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


async def test_autocommit(aconn):
    assert aconn.autocommit is False
    with pytest.raises(AttributeError):
        aconn.autocommit = True
    assert not aconn.autocommit

    await aconn.set_autocommit(True)
    assert aconn.autocommit
    cur = aconn.cursor()
    await cur.execute("select 1")
    assert await cur.fetchone() == (1,)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE

    await aconn.set_autocommit("")
    assert aconn.autocommit is False
    await aconn.set_autocommit("yeah")
    assert aconn.autocommit is True


async def test_autocommit_connect(aconn_cls, dsn):
    aconn = await aconn_cls.connect(dsn, autocommit=True)
    assert aconn.autocommit
    await aconn.close()


async def test_autocommit_intrans(aconn):
    cur = aconn.cursor()
    await cur.execute("select 1")
    assert await cur.fetchone() == (1,)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    with pytest.raises(psycopg.ProgrammingError):
        await aconn.set_autocommit(True)
    assert not aconn.autocommit


async def test_autocommit_inerror(aconn):
    cur = aconn.cursor()
    with pytest.raises(psycopg.DatabaseError):
        await cur.execute("meh")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR
    with pytest.raises(psycopg.ProgrammingError):
        await aconn.set_autocommit(True)
    assert not aconn.autocommit


async def test_autocommit_unknown(aconn):
    await aconn.close()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.UNKNOWN
    with pytest.raises(psycopg.OperationalError):
        await aconn.set_autocommit(True)
    assert not aconn.autocommit


@pytest.mark.parametrize(
    "args, kwargs, want",
    [
        ((), {}, ""),
        (("",), {}, ""),
        (("dbname=foo user=bar",), {}, "dbname=foo user=bar"),
        (("dbname=foo",), {"user": "baz"}, "dbname=foo user=baz"),
        (
            ("dbname=foo port=5432",),
            {"dbname": "qux", "user": "joe"},
            "dbname=qux user=joe port=5432",
        ),
        (("dbname=foo",), {"user": None}, "dbname=foo"),
    ],
)
async def test_connect_args(
    aconn_cls, monkeypatch, setpgenv, pgconn, args, kwargs, want
):
    the_conninfo: str

    def fake_connect(conninfo):
        nonlocal the_conninfo
        the_conninfo = conninfo
        return pgconn
        yield

    setpgenv({})
    monkeypatch.setattr(psycopg.connection, "connect", fake_connect)
    conn = await aconn_cls.connect(*args, **kwargs)
    assert conninfo_to_dict(the_conninfo) == conninfo_to_dict(want)
    await conn.close()


@pytest.mark.parametrize(
    "args, kwargs, exctype",
    [
        (("host=foo", "host=bar"), {}, TypeError),
        (("", ""), {}, TypeError),
        ((), {"nosuchparam": 42}, psycopg.ProgrammingError),
    ],
)
async def test_connect_badargs(aconn_cls, monkeypatch, pgconn, args, kwargs, exctype):
    def fake_connect(conninfo):
        return pgconn
        yield

    monkeypatch.setattr(psycopg.connection, "connect", fake_connect)
    with pytest.raises(exctype):
        await aconn_cls.connect(*args, **kwargs)


@pytest.mark.crdb_skip("pg_terminate_backend")
async def test_broken_connection(aconn):
    cur = aconn.cursor()
    with pytest.raises(psycopg.DatabaseError):
        await cur.execute("select pg_terminate_backend(pg_backend_pid())")
    assert aconn.closed


@pytest.mark.crdb_skip("do")
async def test_notice_handlers(aconn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    messages = []
    severities = []

    def cb1(diag):
        messages.append(diag.message_primary)

    def cb2(res):
        raise Exception("hello from cb2")

    aconn.add_notice_handler(cb1)
    aconn.add_notice_handler(cb2)
    aconn.add_notice_handler("the wrong thing")
    aconn.add_notice_handler(lambda diag: severities.append(diag.severity_nonlocalized))

    aconn.pgconn.exec_(b"set client_min_messages to notice")
    cur = aconn.cursor()
    await cur.execute("do $$begin raise notice 'hello notice'; end$$ language plpgsql")
    assert messages == ["hello notice"]
    assert severities == ["NOTICE"]

    assert len(caplog.records) == 2
    rec = caplog.records[0]
    assert rec.levelno == logging.ERROR
    assert "hello from cb2" in rec.message
    rec = caplog.records[1]
    assert rec.levelno == logging.ERROR
    assert "the wrong thing" in rec.message

    aconn.remove_notice_handler(cb1)
    aconn.remove_notice_handler("the wrong thing")
    await cur.execute(
        "do $$begin raise warning 'hello warning'; end$$ language plpgsql"
    )
    assert len(caplog.records) == 3
    assert messages == ["hello notice"]
    assert severities == ["NOTICE", "WARNING"]

    with pytest.raises(ValueError):
        aconn.remove_notice_handler(cb1)


@pytest.mark.crdb_skip("notify")
async def test_notify_handlers(aconn):
    nots1 = []
    nots2 = []

    def cb1(n):
        nots1.append(n)

    aconn.add_notify_handler(cb1)
    aconn.add_notify_handler(lambda n: nots2.append(n))

    await aconn.set_autocommit(True)
    cur = aconn.cursor()
    await cur.execute("listen foo")
    await cur.execute("notify foo, 'n1'")

    assert len(nots1) == 1
    n = nots1[0]
    assert n.channel == "foo"
    assert n.payload == "n1"
    assert n.pid == aconn.pgconn.backend_pid

    assert len(nots2) == 1
    assert nots2[0] == nots1[0]

    aconn.remove_notify_handler(cb1)
    await cur.execute("notify foo, 'n2'")

    assert len(nots1) == 1
    assert len(nots2) == 2
    n = nots2[1]
    assert isinstance(n, Notify)
    assert n.channel == "foo"
    assert n.payload == "n2"
    assert n.pid == aconn.pgconn.backend_pid

    with pytest.raises(ValueError):
        aconn.remove_notify_handler(cb1)


async def test_execute(aconn):
    cur = await aconn.execute("select %s, %s", [10, 20])
    assert await cur.fetchone() == (10, 20)
    assert cur.format == 0
    assert cur.pgresult.fformat(0) == 0

    cur = await aconn.execute("select %(a)s, %(b)s", {"a": 11, "b": 21})
    assert await cur.fetchone() == (11, 21)

    cur = await aconn.execute("select 12, 22")
    assert await cur.fetchone() == (12, 22)


async def test_execute_binary(aconn):
    cur = await aconn.execute("select %s, %s", [10, 20], binary=True)
    assert await cur.fetchone() == (10, 20)
    assert cur.format == 1
    assert cur.pgresult.fformat(0) == 1


async def test_row_factory(aconn_cls, dsn):
    defaultconn = await aconn_cls.connect(dsn)
    assert defaultconn.row_factory is tuple_row
    await defaultconn.close()

    conn = await aconn_cls.connect(dsn, row_factory=my_row_factory)
    assert conn.row_factory is my_row_factory

    cur = await conn.execute("select 'a' as ve")
    assert await cur.fetchone() == ["Ave"]

    async with conn.cursor(row_factory=lambda c: lambda t: set(t)) as cur1:
        await cur1.execute("select 1, 1, 2")
        assert await cur1.fetchall() == [{1, 2}]

    async with conn.cursor(row_factory=tuple_row) as cur2:
        await cur2.execute("select 1, 1, 2")
        assert await cur2.fetchall() == [(1, 1, 2)]

    # TODO: maybe fix something to get rid of 'type: ignore' below.
    conn.row_factory = tuple_row
    cur3 = await conn.execute("select 'vale'")
    r = await cur3.fetchone()
    assert r and r == ("vale",)
    await conn.close()


async def test_str(aconn):
    assert "[IDLE]" in str(aconn)
    await aconn.close()
    assert "[BAD]" in str(aconn)


async def test_fileno(aconn):
    assert aconn.fileno() == aconn.pgconn.socket
    await aconn.close()
    with pytest.raises(psycopg.OperationalError):
        aconn.fileno()


async def test_cursor_factory(aconn):
    assert aconn.cursor_factory is psycopg.AsyncCursor

    class MyCursor(psycopg.AsyncCursor[psycopg.rows.Row]):
        pass

    aconn.cursor_factory = MyCursor
    async with aconn.cursor() as cur:
        assert isinstance(cur, MyCursor)

    async with (await aconn.execute("select 1")) as cur:
        assert isinstance(cur, MyCursor)


async def test_cursor_factory_connect(aconn_cls, dsn):
    class MyCursor(psycopg.AsyncCursor[psycopg.rows.Row]):
        pass

    async with await aconn_cls.connect(dsn, cursor_factory=MyCursor) as conn:
        assert conn.cursor_factory is MyCursor
        cur = conn.cursor()
        assert type(cur) is MyCursor


async def test_server_cursor_factory(aconn):
    assert aconn.server_cursor_factory is psycopg.AsyncServerCursor

    class MyServerCursor(psycopg.AsyncServerCursor[psycopg.rows.Row]):
        pass

    aconn.server_cursor_factory = MyServerCursor
    async with aconn.cursor(name="n") as cur:
        assert isinstance(cur, MyServerCursor)


@pytest.mark.parametrize("param", tx_params)
async def test_transaction_param_default(aconn, param):
    assert getattr(aconn, param.name) is None
    cur = await aconn.execute(
        "select current_setting(%s), current_setting(%s)",
        [f"transaction_{param.guc}", f"default_transaction_{param.guc}"],
    )
    current, default = await cur.fetchone()
    assert current == default


@pytest.mark.parametrize("param", tx_params)
async def test_transaction_param_readonly_property(aconn, param):
    with pytest.raises(AttributeError):
        setattr(aconn, param.name, None)


@pytest.mark.parametrize("autocommit", [True, False])
@pytest.mark.parametrize("param", tx_params_isolation)
async def test_set_transaction_param_implicit(aconn, param, autocommit):
    await aconn.set_autocommit(autocommit)
    for value in param.values:
        await getattr(aconn, f"set_{param.name}")(value)
        cur = await aconn.execute(
            "select current_setting(%s), current_setting(%s)",
            [f"transaction_{param.guc}", f"default_transaction_{param.guc}"],
        )
        pgval, default = await cur.fetchone()
        if autocommit:
            assert pgval == default
        else:
            assert tx_values_map[pgval] == value
        await aconn.rollback()


@pytest.mark.parametrize("autocommit", [True, False])
@pytest.mark.parametrize("param", tx_params_isolation)
async def test_set_transaction_param_block(aconn, param, autocommit):
    await aconn.set_autocommit(autocommit)
    for value in param.values:
        await getattr(aconn, f"set_{param.name}")(value)
        async with aconn.transaction():
            cur = await aconn.execute(
                "select current_setting(%s)", [f"transaction_{param.guc}"]
            )
            pgval = (await cur.fetchone())[0]
        assert tx_values_map[pgval] == value


@pytest.mark.parametrize("param", tx_params)
async def test_set_transaction_param_not_intrans_implicit(aconn, param):
    await aconn.execute("select 1")
    value = param.values[0]
    with pytest.raises(psycopg.ProgrammingError):
        await getattr(aconn, f"set_{param.name}")(value)


@pytest.mark.parametrize("param", tx_params)
async def test_set_transaction_param_not_intrans_block(aconn, param):
    value = param.values[0]
    async with aconn.transaction():
        with pytest.raises(psycopg.ProgrammingError):
            await getattr(aconn, f"set_{param.name}")(value)


@pytest.mark.parametrize("param", tx_params)
async def test_set_transaction_param_not_intrans_external(aconn, param):
    value = param.values[0]
    await aconn.set_autocommit(True)
    await aconn.execute("begin")
    with pytest.raises(psycopg.ProgrammingError):
        await getattr(aconn, f"set_{param.name}")(value)


@pytest.mark.crdb("skip", reason="transaction isolation")
async def test_set_transaction_param_all(aconn):
    params: List[Any] = tx_params[:]
    params[2] = params[2].values[0]

    for param in params:
        value = param.values[0]
        await getattr(aconn, f"set_{param.name}")(value)

    for param in params:
        cur = await aconn.execute(
            "select current_setting(%s)", [f"transaction_{param.guc}"]
        )
        pgval = (await cur.fetchone())[0]
        assert tx_values_map[pgval] == value


async def test_set_transaction_param_strange(aconn):
    for val in ("asdf", 0, 5):
        with pytest.raises(ValueError):
            await aconn.set_isolation_level(val)

    await aconn.set_isolation_level(psycopg.IsolationLevel.SERIALIZABLE.value)
    assert aconn.isolation_level is psycopg.IsolationLevel.SERIALIZABLE

    await aconn.set_read_only(1)
    assert aconn.read_only is True

    await aconn.set_deferrable(0)
    assert aconn.deferrable is False


@pytest.mark.parametrize("dsn, kwargs, exp", conninfo_params_timeout)
async def test_get_connection_params(aconn_cls, dsn, kwargs, exp, setpgenv):
    setpgenv({})
    params = await aconn_cls._get_connection_params(dsn, **kwargs)
    conninfo = make_conninfo(**params)
    assert conninfo_to_dict(conninfo) == exp[0]
    assert params["connect_timeout"] == exp[1]


async def test_connect_context_adapters(aconn_cls, dsn):
    ctx = psycopg.adapt.AdaptersMap(psycopg.adapters)
    ctx.register_dumper(str, make_bin_dumper("b"))
    ctx.register_dumper(str, make_dumper("t"))

    conn = await aconn_cls.connect(dsn, context=ctx)

    cur = await conn.execute("select %s", ["hello"])
    assert (await cur.fetchone())[0] == "hellot"
    cur = await conn.execute("select %b", ["hello"])
    assert (await cur.fetchone())[0] == "hellob"
    await conn.close()


async def test_connect_context_copy(aconn_cls, dsn, aconn):
    aconn.adapters.register_dumper(str, make_bin_dumper("b"))
    aconn.adapters.register_dumper(str, make_dumper("t"))

    aconn2 = await aconn_cls.connect(dsn, context=aconn)

    cur = await aconn2.execute("select %s", ["hello"])
    assert (await cur.fetchone())[0] == "hellot"
    cur = await aconn2.execute("select %b", ["hello"])
    assert (await cur.fetchone())[0] == "hellob"
    await aconn2.close()


async def test_cancel_closed(aconn):
    await aconn.close()
    aconn.cancel()


async def test_resolve_hostaddr_conn(monkeypatch, fake_resolve):  # noqa: F811
    got = []

    def fake_connect_gen(conninfo, **kwargs):
        got.append(conninfo)
        1 / 0

    monkeypatch.setattr(psycopg.AsyncConnection, "_connect_gen", fake_connect_gen)

    with pytest.raises(ZeroDivisionError):
        await psycopg.AsyncConnection.connect("host=foo.com")

    assert len(got) == 1
    want = {"host": "foo.com", "hostaddr": "1.1.1.1"}
    assert conninfo_to_dict(got[0]) == want
