import logging
from typing import Any
import concurrent.futures

import pytest

import psycopg
from psycopg import pq
from psycopg import errors as e

pytestmark = pytest.mark.libpq(">= 14")


def test_repr(conn):
    with conn.pipeline() as p:
        assert "psycopg.Pipeline" in repr(p)
        assert "[IDLE]" in repr(p)

    conn.close()
    assert "[BAD]" in repr(p)


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


def test_cursor_stream(conn):
    with conn.pipeline(), conn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            cur.stream("select 1").__next__()


def test_server_cursor(conn):
    with conn.cursor(name="pipeline") as cur, conn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            cur.execute("select 1")


def test_cannot_insert_multiple_commands(conn):
    with pytest.raises(psycopg.errors.SyntaxError) as cm:
        with conn.pipeline():
            conn.execute("select 1; select 2")
    assert cm.value.sqlstate == "42601"


def test_pipeline_processed_at_exit(conn):
    with conn.cursor() as cur:
        with conn.pipeline() as p:
            cur.execute("select 1")

            # PQsendQuery[BEGIN], PQsendQuery
            assert len(p.result_queue) == 2

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

        # PQsendQuery[BEGIN], PQsendQuery(2)
        assert len(p.result_queue) == 3

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
        with pytest.raises(e.OperationalError, match="pipeline aborted"):
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


def test_executemany(conn):
    conn.autocommit = True
    conn.execute("drop table if exists execmanypipeline")
    conn.execute(
        "create unlogged table execmanypipeline ("
        " id serial primary key, num integer)"
    )
    with conn.pipeline(), conn.cursor() as cur:
        cur.executemany(
            "insert into execmanypipeline(num) values (%s) returning id",
            [(10,), (20,)],
        )
        assert cur.fetchone() == (1,)
        assert cur.nextset()
        assert cur.fetchone() == (2,)
        assert cur.nextset() is None


def test_prepared(conn):
    conn.autocommit = True
    with conn.pipeline():
        c1 = conn.execute("select %s::int", [10], prepare=True)
        c2 = conn.execute("select count(*) from pg_prepared_statements")

        (r,) = c1.fetchone()
        assert r == 10

        (r,) = c2.fetchone()
        assert r == 1


def test_auto_prepare(conn):
    conn.autocommit = True
    conn.prepared_threshold = 5
    with conn.pipeline():
        cursors = [
            conn.execute("select count(*) from pg_prepared_statements")
            for i in range(10)
        ]

        assert len(conn._prepared._names) == 1

    res = [c.fetchone()[0] for c in cursors]
    assert res == [0] * 5 + [1] * 5


def test_transaction(conn):
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


def test_outer_transaction(conn):
    with conn.transaction():
        with conn.pipeline():
            conn.execute("drop table if exists outertx")
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


def test_concurrency(conn):
    with conn.transaction():
        conn.execute("drop table if exists pipeline_concurrency")
        conn.execute(
            "create unlogged table pipeline_concurrency ("
            " id serial primary key,"
            " value integer"
            ")"
        )
        conn.execute("drop table if exists accessed")
        conn.execute("create unlogged table accessed as (select now() as value)")

    def update(value):
        cur = conn.execute(
            "insert into pipeline_concurrency(value) values (%s) returning id",
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
