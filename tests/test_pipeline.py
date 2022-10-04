import logging
import concurrent.futures
from typing import Any
from operator import attrgetter
from itertools import groupby

import pytest

import psycopg
from psycopg import pq
from psycopg import errors as e

pytestmark = [
    pytest.mark.pipeline,
    pytest.mark.skipif("not psycopg.Pipeline.is_supported()"),
]

pipeline_aborted = pytest.mark.flakey("the server might get in pipeline aborted")


def test_repr(conn):
    with conn.pipeline() as p:
        assert "psycopg.Pipeline" in repr(p)
        assert "[IDLE, pipeline=ON]" in repr(p)

    conn.close()
    assert "[BAD]" in repr(p)


def test_connection_closed(conn):
    conn.close()
    with pytest.raises(e.OperationalError):
        with conn.pipeline():
            pass


def test_pipeline_status(conn: psycopg.Connection[Any]) -> None:
    assert conn._pipeline is None
    with conn.pipeline() as p:
        assert conn._pipeline is p
        assert p.status == pq.PipelineStatus.ON
    assert p.status == pq.PipelineStatus.OFF
    assert not conn._pipeline


def test_pipeline_reenter(conn: psycopg.Connection[Any]) -> None:
    with conn.pipeline() as p1:
        with conn.pipeline() as p2:
            assert p2 is p1
            assert p1.status == pq.PipelineStatus.ON
        assert p2 is p1
        assert p2.status == pq.PipelineStatus.ON
    assert conn._pipeline is None
    assert p1.status == pq.PipelineStatus.OFF


def test_pipeline_broken_conn_exit(conn: psycopg.Connection[Any]) -> None:
    with pytest.raises(e.OperationalError):
        with conn.pipeline():
            conn.execute("select 1")
            conn.close()
            closed = True

    assert closed


def test_pipeline_exit_error_noclobber(conn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    with pytest.raises(ZeroDivisionError):
        with conn.pipeline():
            conn.close()
            1 / 0

    assert len(caplog.records) == 1


def test_pipeline_exit_error_noclobber_nested(conn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    with pytest.raises(ZeroDivisionError):
        with conn.pipeline():
            with conn.pipeline():
                conn.close()
                1 / 0

    assert len(caplog.records) == 2


def test_pipeline_exit_sync_trace(conn, trace):
    t = trace.trace(conn)
    with conn.pipeline():
        pass
    conn.close()
    assert len([i for i in t if i.type == "Sync"]) == 1


def test_pipeline_nested_sync_trace(conn, trace):
    t = trace.trace(conn)
    with conn.pipeline():
        with conn.pipeline():
            pass
    conn.close()
    assert len([i for i in t if i.type == "Sync"]) == 2


def test_cursor_stream(conn):
    with conn.pipeline(), conn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            cur.stream("select 1").__next__()


def test_server_cursor(conn):
    with conn.cursor(name="pipeline") as cur, conn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            cur.execute("select 1")


def test_cannot_insert_multiple_commands(conn):
    with pytest.raises((e.SyntaxError, e.InvalidPreparedStatementDefinition)):
        with conn.pipeline():
            conn.execute("select 1; select 2")


def test_copy(conn):
    with conn.pipeline():
        cur = conn.cursor()
        with pytest.raises(e.NotSupportedError):
            with cur.copy("copy (select 1) to stdout"):
                pass


def test_pipeline_processed_at_exit(conn):
    with conn.cursor() as cur:
        with conn.pipeline() as p:
            cur.execute("select 1")

            assert len(p.result_queue) == 1

        assert cur.fetchone() == (1,)


def test_pipeline_errors_processed_at_exit(conn):
    conn.autocommit = True
    with pytest.raises(e.UndefinedTable):
        with conn.pipeline():
            conn.execute("select * from nosuchtable")
            conn.execute("create table voila ()")
    cur = conn.execute(
        "select count(*) from pg_tables where tablename = %s", ("voila",)
    )
    (count,) = cur.fetchone()
    assert count == 0


def test_pipeline(conn):
    with conn.pipeline() as p:
        c1 = conn.cursor()
        c2 = conn.cursor()
        c1.execute("select 1")
        c2.execute("select 2")

        assert len(p.result_queue) == 2

        (r1,) = c1.fetchone()
        assert r1 == 1

    (r2,) = c2.fetchone()
    assert r2 == 2


def test_autocommit(conn):
    conn.autocommit = True
    with conn.pipeline(), conn.cursor() as c:
        c.execute("select 1")

        (r,) = c.fetchone()
        assert r == 1


def test_pipeline_aborted(conn):
    conn.autocommit = True
    with conn.pipeline() as p:
        c1 = conn.execute("select 1")
        with pytest.raises(e.UndefinedTable):
            conn.execute("select * from doesnotexist").fetchone()
        with pytest.raises(e.PipelineAborted):
            conn.execute("select 'aborted'").fetchone()
        # Sync restore the connection in usable state.
        p.sync()
        c2 = conn.execute("select 2")

    (r,) = c1.fetchone()
    assert r == 1

    (r,) = c2.fetchone()
    assert r == 2


def test_pipeline_commit_aborted(conn):
    with pytest.raises((e.UndefinedColumn, e.OperationalError)):
        with conn.pipeline():
            conn.execute("select error")
            conn.execute("create table voila ()")
            conn.commit()


def test_sync_syncs_results(conn):
    with conn.pipeline() as p:
        cur = conn.execute("select 1")
        assert cur.statusmessage is None
        p.sync()
        assert cur.statusmessage == "SELECT 1"


def test_sync_syncs_errors(conn):
    conn.autocommit = True
    with conn.pipeline() as p:
        conn.execute("select 1 from nosuchtable")
        with pytest.raises(e.UndefinedTable):
            p.sync()


@pipeline_aborted
def test_errors_raised_on_commit(conn):
    with conn.pipeline():
        conn.execute("select 1 from nosuchtable")
        with pytest.raises(e.UndefinedTable):
            conn.commit()
        conn.rollback()
        cur1 = conn.execute("select 1")
    cur2 = conn.execute("select 2")

    assert cur1.fetchone() == (1,)
    assert cur2.fetchone() == (2,)


def test_errors_raised_on_transaction_exit(conn):
    here = False
    with conn.pipeline():
        with pytest.raises(e.UndefinedTable):
            with conn.transaction():
                conn.execute("select 1 from nosuchtable")
                here = True
        cur1 = conn.execute("select 1")
    assert here
    cur2 = conn.execute("select 2")

    assert cur1.fetchone() == (1,)
    assert cur2.fetchone() == (2,)


def test_errors_raised_on_nested_transaction_exit(conn):
    here = False
    with conn.pipeline():
        with conn.transaction():
            with pytest.raises(e.UndefinedTable):
                with conn.transaction():
                    conn.execute("select 1 from nosuchtable")
                    here = True
            cur1 = conn.execute("select 1")
    assert here
    cur2 = conn.execute("select 2")

    assert cur1.fetchone() == (1,)
    assert cur2.fetchone() == (2,)


def test_implicit_transaction(conn):
    conn.autocommit = True
    with conn.pipeline():
        assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE
        conn.execute("select 'before'")
        # Transaction is ACTIVE because previous command is not completed
        # since we have not fetched its results.
        assert conn.pgconn.transaction_status == pq.TransactionStatus.ACTIVE
        # Upon entering the nested pipeline through "with transaction():", a
        # sync() is emitted to restore the transaction state to IDLE, as
        # expected to emit a BEGIN.
        with conn.transaction():
            conn.execute("select 'tx'")
        cur = conn.execute("select 'after'")
    assert cur.fetchone() == ("after",)


@pytest.mark.crdb_skip("deferrable")
def test_error_on_commit(conn):
    conn.execute(
        """
        drop table if exists selfref;
        create table selfref (
            x serial primary key,
            y int references selfref (x) deferrable initially deferred)
        """
    )
    conn.commit()

    with conn.pipeline():
        conn.execute("insert into selfref (y) values (-1)")
        with pytest.raises(e.ForeignKeyViolation):
            conn.commit()
        cur1 = conn.execute("select 1")
    cur2 = conn.execute("select 2")

    assert cur1.fetchone() == (1,)
    assert cur2.fetchone() == (2,)


def test_fetch_no_result(conn):
    with conn.pipeline():
        cur = conn.cursor()
        with pytest.raises(e.ProgrammingError):
            cur.fetchone()


def test_executemany(conn):
    conn.autocommit = True
    conn.execute("drop table if exists execmanypipeline")
    conn.execute(
        "create unlogged table execmanypipeline ("
        " id serial primary key, num integer)"
    )
    with conn.pipeline(), conn.cursor() as cur:
        cur.executemany(
            "insert into execmanypipeline(num) values (%s) returning num",
            [(10,), (20,)],
            returning=True,
        )
        assert cur.rowcount == 2
        assert cur.fetchone() == (10,)
        assert cur.nextset()
        assert cur.fetchone() == (20,)
        assert cur.nextset() is None


def test_executemany_no_returning(conn):
    conn.autocommit = True
    conn.execute("drop table if exists execmanypipelinenoreturning")
    conn.execute(
        "create unlogged table execmanypipelinenoreturning ("
        " id serial primary key, num integer)"
    )
    with conn.pipeline(), conn.cursor() as cur:
        cur.executemany(
            "insert into execmanypipelinenoreturning(num) values (%s)",
            [(10,), (20,)],
            returning=False,
        )
        with pytest.raises(e.ProgrammingError, match="no result available"):
            cur.fetchone()
        assert cur.nextset() is None
        with pytest.raises(e.ProgrammingError, match="no result available"):
            cur.fetchone()
        assert cur.nextset() is None


@pytest.mark.crdb("skip", reason="temp tables")
def test_executemany_trace(conn, trace):
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("create temp table trace (id int)")
    t = trace.trace(conn)
    with conn.pipeline():
        cur.executemany("insert into trace (id) values (%s)", [(10,), (20,)])
        cur.executemany("insert into trace (id) values (%s)", [(10,), (20,)])
    conn.close()
    items = list(t)
    assert items[-1].type == "Terminate"
    del items[-1]
    roundtrips = [k for k, g in groupby(items, key=attrgetter("direction"))]
    assert roundtrips == ["F", "B"]
    assert len([i for i in items if i.type == "Sync"]) == 1


@pytest.mark.crdb("skip", reason="temp tables")
def test_executemany_trace_returning(conn, trace):
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("create temp table trace (id int)")
    t = trace.trace(conn)
    with conn.pipeline():
        cur.executemany(
            "insert into trace (id) values (%s)", [(10,), (20,)], returning=True
        )
        cur.executemany(
            "insert into trace (id) values (%s)", [(10,), (20,)], returning=True
        )
    conn.close()
    items = list(t)
    assert items[-1].type == "Terminate"
    del items[-1]
    roundtrips = [k for k, g in groupby(items, key=attrgetter("direction"))]
    assert roundtrips == ["F", "B"] * 3
    assert items[-2].direction == "F"  # last 2 items are F B
    assert len([i for i in items if i.type == "Sync"]) == 1


def test_prepared(conn):
    conn.autocommit = True
    with conn.pipeline():
        c1 = conn.execute("select %s::int", [10], prepare=True)
        c2 = conn.execute(
            "select count(*) from pg_prepared_statements where name != ''"
        )

        (r,) = c1.fetchone()
        assert r == 10

        (r,) = c2.fetchone()
        assert r == 1


def test_auto_prepare(conn):
    conn.autocommit = True
    conn.prepared_threshold = 5
    with conn.pipeline():
        cursors = [
            conn.execute("select count(*) from pg_prepared_statements where name != ''")
            for i in range(10)
        ]

        assert len(conn._prepared._names) == 1

    res = [c.fetchone()[0] for c in cursors]
    assert res == [0] * 5 + [1] * 5


def test_transaction(conn):
    notices = []
    conn.add_notice_handler(lambda diag: notices.append(diag.message_primary))

    with conn.pipeline():
        with conn.transaction():
            cur = conn.execute("select 'tx'")

        (r,) = cur.fetchone()
        assert r == "tx"

        with conn.transaction():
            cur = conn.execute("select 'rb'")
            raise psycopg.Rollback()

        (r,) = cur.fetchone()
        assert r == "rb"

    assert not notices


def test_transaction_nested(conn):
    with conn.pipeline():
        with conn.transaction():
            outer = conn.execute("select 'outer'")
            with pytest.raises(ZeroDivisionError):
                with conn.transaction():
                    inner = conn.execute("select 'inner'")
                    1 / 0

        (r,) = outer.fetchone()
        assert r == "outer"
        (r,) = inner.fetchone()
        assert r == "inner"


def test_transaction_nested_no_statement(conn):
    with conn.pipeline():
        with conn.transaction():
            with conn.transaction():
                cur = conn.execute("select 1")

        (r,) = cur.fetchone()
        assert r == 1


def test_outer_transaction(conn):
    with conn.transaction():
        conn.execute("drop table if exists outertx")
    with conn.transaction():
        with conn.pipeline():
            conn.execute("create table outertx as (select 1)")
            cur = conn.execute("select * from outertx")
    (r,) = cur.fetchone()
    assert r == 1
    cur = conn.execute("select count(*) from pg_tables where tablename = 'outertx'")
    assert cur.fetchone()[0] == 1


def test_outer_transaction_error(conn):
    with conn.transaction():
        with pytest.raises((e.UndefinedColumn, e.OperationalError)):
            with conn.pipeline():
                conn.execute("select error")
                conn.execute("create table voila ()")


def test_rollback_explicit(conn):
    conn.autocommit = True
    with conn.pipeline():
        with pytest.raises(e.DivisionByZero):
            cur = conn.execute("select 1 / %s", [0])
            cur.fetchone()
        conn.rollback()
        conn.execute("select 1")


def test_rollback_transaction(conn):
    conn.autocommit = True
    with pytest.raises(e.DivisionByZero):
        with conn.pipeline():
            with conn.transaction():
                cur = conn.execute("select 1 / %s", [0])
                cur.fetchone()
    conn.execute("select 1")


def test_message_0x33(conn):
    # https://github.com/psycopg/psycopg/issues/314
    notices = []
    conn.add_notice_handler(lambda diag: notices.append(diag.message_primary))

    conn.autocommit = True
    with conn.pipeline():
        cur = conn.execute("select 'test'")
        assert cur.fetchone() == ("test",)

    assert not notices


def test_transaction_state_implicit_begin(conn, trace):
    # Regression test to ensure that the transaction state is correct after
    # the implicit BEGIN statement (in non-autocommit mode).
    notices = []
    conn.add_notice_handler(lambda diag: notices.append(diag.message_primary))
    t = trace.trace(conn)
    with conn.pipeline():
        conn.execute("select 'x'").fetchone()
        conn.execute("select 'y'")
    assert not notices
    assert [
        e.content[0] for e in t if e.type == "Parse" and b"BEGIN" in e.content[0]
    ] == [b' "" "BEGIN" 0']


def test_concurrency(conn):
    with conn.transaction():
        conn.execute("drop table if exists pipeline_concurrency")
        conn.execute("drop table if exists accessed")
    with conn.transaction():
        conn.execute(
            "create unlogged table pipeline_concurrency ("
            " id serial primary key,"
            " value integer"
            ")"
        )
        conn.execute("create unlogged table accessed as (select now() as value)")

    def update(value):
        cur = conn.execute(
            "insert into pipeline_concurrency(value) values (%s) returning value",
            (value,),
        )
        conn.execute("update accessed set value = now()")
        return cur

    conn.autocommit = True

    (before,) = conn.execute("select value from accessed").fetchone()

    values = range(1, 10)
    with conn.pipeline():
        with concurrent.futures.ThreadPoolExecutor() as e:
            cursors = e.map(update, values, timeout=len(values))
            assert sum(cur.fetchone()[0] for cur in cursors) == sum(values)

    (s,) = conn.execute("select sum(value) from pipeline_concurrency").fetchone()
    assert s == sum(values)
    (after,) = conn.execute("select value from accessed").fetchone()
    assert after > before
